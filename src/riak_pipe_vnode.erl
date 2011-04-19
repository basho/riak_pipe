%% -------------------------------------------------------------------
%%
%% Copyright (c) 2011 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(riak_pipe_vnode).
-behaviour(riak_core_vnode).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_exit/3]).
-export([queue_work/2,
         queue_work/3,
         eoi/2,
         next_input/2,
         reply_archive/3,
         status/1]).

-include_lib("riak_core/include/riak_core_vnode.hrl"). %% ?FOLD_REQ
-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").
-include("riak_pipe_debug.hrl").

-define(DEFAULT_WORKER_LIMIT, 50).
-define(DEFAULT_WORKER_Q_LIMIT, 64).

-record(state, {partition,
                worker_sup,
                workers,
                worker_limit,
                worker_q_limit,
                workers_archiving,
                handoff}).

-record(cmd_enqueue, {fitting, input}).
-record(cmd_eoi, {fitting}).
-record(cmd_next_input, {fitting}).
-record(cmd_archive, {fitting, archive}).
-record(cmd_status, {sender}).

-record(worker, {pid, fitting, details, state, inputs_done,
                 q, q_limit, blocking, handoff}).
-record(worker_handoff, {fitting,
                         queue,
                         blocking,
                         archive}).
-record(handoff, {fold, acc, sender}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

validate_or_exit(Thing, Validator, Msg) ->
    case Validator(Thing) of
        true -> Thing;
        false ->
            error_logger:error_msg(Msg++"~n   (found ~p)", [Thing]),
            exit({invalid_config, {Msg, Thing}})
    end.

init([Partition]) ->
    WL = validate_or_exit(app_helper:get_env(riak_pipe, worker_limit,
                                             ?DEFAULT_WORKER_LIMIT),
                          fun(X) -> is_integer(X) andalso (X > 0) end,
                          "riak_pipe.worker_limit must be"
                          " an integer greater than zero"),
    WQL = validate_or_exit(app_helper:get_env(riak_pipe, worker_queue_limit,
                                              ?DEFAULT_WORKER_Q_LIMIT),
                           fun(X) -> is_integer(X) andalso (X > 0) end,
                           "riak_pipe.worker_queue_limit must be"
                           " an integer greater than zero"),
    {ok, WorkerSup} = riak_pipe_vnode_worker_sup:start_link(Partition, self()),
    {ok, #state{
       partition=Partition,
       worker_sup=WorkerSup,
       workers=[],
       worker_limit=WL,
       worker_q_limit=WQL,
       workers_archiving=[]
      }}.

any_local_vnode() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    hd(riak_core_ring:my_indices(Ring)).

queue_work(#fitting{partfun=follow}=Fitting, Input) ->
    %% this should only happen if someone sets up a pipe with
    %% the first fitting as partfun=follow
    queue_work(Fitting, Input, any_local_vnode());
queue_work(#fitting{partfun=PartFun}=Fitting, Input) ->
    queue_work(Fitting, Input, PartFun(Input)).

queue_work(Fitting, Input, PartitionOverride) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Owner = riak_core_ring:index_owner(Ring, PartitionOverride),
    ReplyRef = make_ref(),
    riak_core_vnode_master:command(
      {PartitionOverride, Owner},
      #cmd_enqueue{fitting=Fitting, input=Input},
      {raw, ReplyRef, self()},
      riak_pipe_vnode_master),
    %% block until input confirmed queued, for backpressure
    receive {ReplyRef, Reply} -> Reply end.

eoi(Pid, Fitting) ->
    riak_core_vnode:send_command(Pid, #cmd_eoi{fitting=Fitting}).

next_input(Pid, Fitting) ->
    riak_core_vnode:send_command(Pid, #cmd_next_input{fitting=Fitting}).

reply_archive(Pid, Fitting, Archive) ->
    riak_core_vnode:send_command(Pid, #cmd_archive{fitting=Fitting,
                                                   archive=Archive}).

status(Pid) ->
    Ref = make_ref(),
    riak_core_vnode:send_command(Pid, #cmd_status{sender={raw, Ref, self()}}),
    receive
        {Ref, Reply} -> Reply
    end.

% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};
handle_command(#cmd_enqueue{}=Cmd, Sender, State) ->
    enqueue_internal(Cmd, Sender, State);
handle_command(#cmd_eoi{}=Cmd, _Sender, State) ->
    eoi_internal(Cmd, State);
handle_command(#cmd_next_input{}=Cmd, _Sender, State) ->
    next_input_internal(Cmd, State);
handle_command(#cmd_status{}=Cmd, _Sender, State) ->
    status_internal(Cmd, State);
handle_command(Message, _Sender, State) ->
    error_logger:info_msg("~p:~p Unhandled command:~n~p",
                          [?MODULE, ?LINE, Message]),
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{}=Cmd, Sender, State) ->
    handoff_cmd_internal(Cmd, Sender, State);
handle_handoff_command(#cmd_archive{}=Cmd, _Sender, State) ->
    archive_internal(Cmd, State);
handle_handoff_command(#cmd_enqueue{fitting=F}=Cmd, Sender, State) ->
    case worker_by_fitting(F, State) of
        {ok, _} ->
            %% not yet handed off: proceed
            handle_command(Cmd, Sender, State);
        none ->
            %% handed off, or never existed: forward
            {forward, State}
    end;
handle_handoff_command(#cmd_eoi{fitting=F}=Cmd, Sender, State) ->
    case worker_by_fitting(F, State) of
        {ok, _} ->
            %% not yet handed off: proceed
            handle_command(Cmd, Sender, State);
        none ->
            %% handed off, or never existed: reply done
            %% (let the other node deal with its own eoi)
            send_done(F)
    end;
handle_handoff_command(#cmd_next_input{fitting=F}, _Sender, State) ->
    %% force workers into waiting state so we can ask them to
    %% prepare for handoff
    {noreply, archive_fitting(F, State)};
handle_handoff_command(Cmd, Sender, State) ->
    %% handle the rest (#cmd_status, Unknown) as usual
    handle_command(Cmd, Sender, State).

handoff_starting(_TargetNode, State) ->
    {true, State#state{handoff=starting}}.

handoff_cancelled(#state{handoff=starting, workers_archiving=[]}=State) ->
    %%TODO: handoff is only cancelled before anything is handed off, right?
    {ok, State#state{handoff=cancelled}}.

handoff_finished(_TargetNode, #state{workers=[]}=State) ->
    %% #state.workers should be empty, because they were all handed off
    %% clear out list of handed off items
    {ok, State#state{handoff=finished}}.

handle_handoff_data(Data, State) ->
    #worker_handoff{fitting=Fitting,
                    queue=Queue,
                    blocking=Blocking,
                    archive=Archive} = binary_to_term(Data),
    case worker_for(Fitting, State) of
        {ok, Worker} ->
            NewWorker = handoff_worker(Worker, Queue, Blocking, Archive),
            {reply, ok, replace_worker(NewWorker, State)};
        Error ->
            {reply, {error, Error}, State}
    end.

encode_handoff_item(Fitting, {Queue, Blocking, Archive}) ->
    term_to_binary(#worker_handoff{fitting=Fitting,
                                   queue=Queue,
                                   blocking=Blocking,
                                   archive=Archive}).

is_empty(#state{workers=Workers}=State) ->
    {Workers==[], State}.

delete(#state{workers=[]}=State) ->
    %%TODO: delete is only called if is_empty/1==true, right?
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_exit(Pid, Reason, #state{partition=Partition}=State) ->
    NewState = case worker_by_pid(Pid, State) of
                   {ok, Worker} ->
                       case {Worker#worker.inputs_done, Reason} of
                           {true, normal} ->
                               ?T(Worker#worker.details, [done],
                                  {vnode, {done, Partition}}),
                               send_done(Worker#worker.fitting),
                               remove_worker(Worker, State);
                           _ ->
                               ?T(Worker#worker.details, [restart],
                                  {vnode, {restart, Partition}}),
                               %% TODO: what do to with failed input?
                               restart_worker(Worker, State)
                       end;
                   none ->
                       case worker_by_fitting_pid(Pid, State) of
                           {ok, Worker} ->
                               ?T(Worker#worker.details, [error],
                                  {vnode, {fitting_died, Partition}}),
                               %% if the fitting died, tear down its worker
                               erlang:unlink(Worker#worker.pid),
                               erlang:exit(Worker#worker.pid, fitting_died),
                               remove_worker(Worker, State);
                           none ->
                               %% TODO: log this somewhere?
                               %% don't know what this pid is
                               %% may be old worker DOWN passing in flight
                               State
                       end
               end,
    {noreply, NewState}.

%% internal

enqueue_internal(#cmd_enqueue{fitting=Fitting, input=Input},
                 Sender, #state{partition=Partition}=State) ->
    case worker_for(Fitting, State) of
        {ok, Worker} ->
            case add_input(Worker, Input, Sender) of
                {ok, NewWorker} ->
                    ?T(NewWorker#worker.details, [queue],
                       {vnode, {queued, Partition, Input}}),
                    {reply, ok, replace_worker(NewWorker, State)};
                {queue_full, NewWorker} ->
                    ?T(NewWorker#worker.details, [queue,queue_full],
                       {vnode, {queue_full, Partition, Input}}),
                    %% if the queue is full, hold up the producer
                    %% until we're ready for more
                    {noreply, replace_worker(NewWorker, State)}
            end;
        worker_limit_reached ->
            %% TODO: log/trace this event
            {reply, {error, worker_limit_reached}, State};
        worker_startup_failed ->
            %% TODO: log/trace this event
            {reply, {error, worker_startup_failed}, State}
    end.

worker_for(Fitting, #state{workers=Workers, worker_limit=Limit}=State) ->
    case worker_by_fitting(Fitting, State) of
        {ok, Worker} ->
            {ok, Worker};
        none ->
            case length(Workers) < Limit of
                true ->
                    new_worker(Fitting, State);
                false ->
                    worker_limit_reached
            end
    end.

new_worker(Fitting, #state{partition=P, worker_sup=Sup, worker_q_limit=WQL}) ->
    try
        %% TODO: would prefer to :monitor here instead of :link, but
        %% riak_core_vnode only handles 'EXIT' messages
        case riak_pipe_fitting:get_details(Fitting, P) of
            {ok, Details} ->
                erlang:link(Fitting#fitting.pid),
                {ok, Pid} = riak_pipe_vnode_worker_sup:start_worker(
                              Sup, Details),
                erlang:link(Pid),
                ?T(Details, [worker], {vnode, {start, P}}),
                {ok, #worker{pid=Pid,
                             fitting=Fitting,
                             details=Details,
                             state=init,
                             inputs_done=false,
                             q=queue:new(),
                             q_limit=WQL,
                             blocking=queue:new()}};
            gone ->
                error_logger:error_msg(
                  "Pipe worker startup failed:"
                  "fitting was gone before startup"),
                worker_startup_failed
        end
    catch Type:Reason ->
            error_logger:error_msg(
              "Pipe worker startup failed:~n"
              "   ~p:~p~n   ~p",
              [Type, Reason, erlang:get_stacktrace()]),
            worker_startup_failed
    end.

add_input(#worker{state=waiting}=Worker, Input, _Sender) ->
    %% worker has been waiting for something to enter its queue
    send_input(Worker, Input),
    {ok, Worker#worker{state={working, Input}}};
add_input(#worker{q=Q, q_limit=QL, blocking=Blocking}=Worker, Input, Sender) ->
    case queue:len(Q) < QL of
        true ->
            {ok, Worker#worker{q=queue:in(Input, Q)}};
        false ->
            NewBlocking = queue:in({Input, Sender}, Blocking),
            {queue_full, Worker#worker{blocking=NewBlocking}}
    end.

handoff_worker(#worker{q=Q, blocking=Blocking}=Worker,
               HandoffQ, HandoffBlocking, HandoffState) ->
    %% simply concatenate queues, and hold the handoff state for
    %% the next available time to ask the worker to deal with it
    MergedWorker = Worker#worker{
                     q=queue:join(Q, HandoffQ),
                     blocking=queue:join(Blocking, HandoffBlocking),
                     handoff={waiting, HandoffState}},
    maybe_wake_for_handoff(MergedWorker).

maybe_wake_for_handoff(#worker{state=waiting}=Worker) ->
    send_handoff(Worker),
    Worker#worker{state={working, handoff}, handoff=undefined};
maybe_wake_for_handoff(Worker) ->
    %% worker is doing something else - send handoff later
    Worker.

eoi_internal(#cmd_eoi{fitting=Fitting}, #state{partition=Partition}=State) ->
    NewState = case worker_by_fitting(Fitting, State) of
                   {ok, Worker} ->
                       case Worker#worker.state of
                           waiting ->
                               ?T(Worker#worker.details, [eoi],
                                  {vnode, {eoi, Partition}}),
                               send_input(Worker, done),
                               replace_worker(
                                 Worker#worker{state={working, done},
                                               inputs_done=true},
                                 State);
                           _ ->
                               replace_worker(
                                 Worker#worker{inputs_done=true},
                                 State)
                       end;
                       %% send_done(Fitting) should go in 'DOWN' handle
                   none ->
                       %% that worker never existed,
                       %% or the 'DOWN' messages are passing in flight
                       send_done(Fitting),
                       State
               end,
    {noreply, NewState}.

next_input_internal(#cmd_next_input{fitting=Fitting}, State) ->
    case worker_by_fitting(Fitting, State) of
        {ok, #worker{handoff=undefined}=Worker} ->
            next_input_nohandoff(Worker, State);
        {ok, Worker} ->
            send_handoff(Worker),
            HandoffWorker = Worker#worker{state={working, handoff},
                                          handoff=undefined},
            {noreply, replace_worker(HandoffWorker, State)}
    end.

next_input_nohandoff(Worker, #state{partition=Partition}=State) ->
    case queue:out(Worker#worker.q) of
        {{value, Input}, NewQ} ->
            ?T(Worker#worker.details, [queue],
               {vnode, {dequeue, Partition}}),
            send_input(Worker, Input),
            WorkingWorker = Worker#worker{state={working, Input},
                                          q=NewQ},
            BlockingWorker = 
                case {queue:len(NewQ) < Worker#worker.q_limit,
                      queue:out(Worker#worker.blocking)} of
                    {true, {{value, {BlockInput, Blocker}}, NewBlocking}} ->
                        ?T(Worker#worker.details, [queue,queue_full],
                           {vnode, {unblocking, Partition}}),
                        %% move blocked input to queue
                        NewNewQ = queue:in(BlockInput, NewQ),
                        %% free up blocked sender
                        reply_to_blocker(Blocker, ok),
                        WorkingWorker#worker{q=NewNewQ,
                                             blocking=NewBlocking};
                    {False, {Empty, _}} when False==false; Empty==empty ->
                        %% nothing blocking, or handoff pushed queue
                        %% length over q_limit
                        WorkingWorker
                end,
            {noreply, replace_worker(BlockingWorker, State)};
        {empty, _} ->
            EmptyWorker = case Worker#worker.inputs_done of
                              true ->
                                  ?T(Worker#worker.details, [eoi],
                                     {vnode, {eoi, Partition}}),
                                  send_input(Worker, done),
                                  Worker#worker{state={working, done}};
                              false ->
                                  ?T(Worker#worker.details, [queue],
                                     {vnode, {waiting, Partition}}),
                                  Worker#worker{state=waiting}
                          end,
            {noreply, replace_worker(EmptyWorker, State)}
    end.

send_input(Worker, Input) ->
    riak_pipe_vnode_worker:send_input(Worker#worker.pid, Input).

send_archive(Worker) ->
    riak_pipe_vnode_worker:send_archive(Worker#worker.pid).

send_handoff(#worker{handoff={waiting, HO}}=Worker) ->
    riak_pipe_vnode_worker:send_handoff(Worker#worker.pid, HO).

worker_by_pid(Pid, #state{workers=Workers}) ->
    case lists:keyfind(Pid, #worker.pid, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.
            
worker_by_fitting(Fitting, #state{workers=Workers}) ->
    case lists:keyfind(Fitting, #worker.fitting, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.

worker_by_fitting_pid(Pid, #state{workers=Workers}) ->
    case [ W || #worker{fitting=F}=W <- Workers, F#fitting.pid =:= Pid ] of
        [#worker{}=Worker] -> {ok, Worker};
        []                 -> none
    end.

replace_worker(#worker{fitting=F}=Worker, #state{workers=Workers}=State) ->
    NewWorkers = lists:keystore(F, #worker.fitting, Workers, Worker),
    State#state{workers=NewWorkers}.

remove_worker(#worker{fitting=F}, #state{workers=Workers}=State) ->
    NewWorkers = lists:keydelete(F, #worker.fitting, Workers),
    State#state{workers=NewWorkers}.

restart_worker(Worker, State) ->
    CleanState = remove_worker(Worker, State),
    case new_worker(Worker#worker.fitting, CleanState) of
        {ok, NewWorker} ->
            CopiedWorker = NewWorker#worker{
                             q=Worker#worker.q,
                             blocking=Worker#worker.blocking,
                             inputs_done=Worker#worker.inputs_done},
            replace_worker(CopiedWorker, CleanState);
        _Error ->
            %% fail blockers, so they resubmit elsewhere
            [ reply_to_blocker(Blocker, fail)
              || {_, Blocker} <- queue:to_list(Worker#worker.blocking) ],
            %% ask fitting to find a better place for the other inputs
            riak_pipe_fitting:send_fail(Worker#worker.fitting,
                                        queue:to_list(Worker#worker.q)),
            CleanState
    end.

reply_to_blocker(Blocker, Reply) ->
    riak_core_vnode:reply(Blocker, Reply).

send_done(Fitting) ->
    riak_pipe_fitting:worker_done(Fitting).

status_internal(#cmd_status{sender=Sender},
                #state{partition=P, workers=Workers}=State) ->
    Reply = {P, [ worker_detail(W) || W <- Workers ]},
    %% riak_core_vnode:command(Pid) does not set reply properly
    riak_core_vnode:reply(Sender, Reply),
    {noreply, State}.

worker_detail(#worker{fitting=Fitting, details=Details,
                      state=State, inputs_done=Done,
                      q=Q, blocking=B}) ->
    [{fitting, Fitting#fitting.pid},
     {name, Details#fitting_details.name},
     {module, Details#fitting_details.module},
     {state, case State of
                 {working, _} -> working;
                 Other        -> Other
             end},
     {inputs_done, Done},
     {queue_length, queue:len(Q)},
     {blocking_lenght, queue:len(B)}].

handoff_cmd_internal(?FOLD_REQ{foldfun=Fold, acc0=Acc}, Sender,
              #state{workers=Workers}=State) ->
    {Ready, NotReady} = lists:partition(
                          fun(W) -> W#worker.state == waiting end,
                          Workers),
    %% ask waiting workers to produce archives
    Archiving = [ begin
                      send_archive(W),
                      W#worker{state={working, archive}}
                  end || W <- Ready ],
    {noreply, State#state{workers=NotReady, workers_archiving=Archiving,
                          handoff=#handoff{fold=Fold,
                                           acc=Acc,
                                           sender=Sender}}}.

archive_fitting(F, State) ->
    {ok, W} = worker_by_fitting(F, State),
    send_archive(W),
    State#state{workers=remove_worker(W, State),
                workers_archiving=[W#worker{state={working, archive}}
                                   |State#state.workers_archiving]}.

archive_internal(#cmd_archive{fitting=F, archive=A},
                 #state{handoff=Handoff,
                        workers=Workers,
                        workers_archiving=Archiving}=State) ->
    {value, Worker, NewArchiving} =
        lists:keytake(F, #worker.fitting, Archiving),
    HandoffVal = {Worker#worker.q, Worker#worker.blocking, A},
    NewAcc = (Handoff#handoff.fold)(F, HandoffVal, Handoff#handoff.acc),
    case {Workers, NewArchiving} of
        {[], []} ->
            %% handoff is done!
            riak_core_vnode:reply(Handoff#handoff.sender, NewAcc);
        _ ->
            %% still chugging
            ok
    end,
    {noreply, State#state{workers_archiving=NewArchiving,
                          handoff=Handoff#handoff{acc=NewAcc}}}.
