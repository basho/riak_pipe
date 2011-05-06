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

%% @doc The vnode, where the queues live.

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

-record(worker, {pid :: pid(),
                 fitting :: #fitting{},
                 details :: #fitting_details{},
                 state :: {working, term()} | waiting | init,
                 inputs_done :: boolean(),
                 q :: queue(),
                 q_limit :: pos_integer(),
                 blocking :: queue(),
                 handoff :: undefined | {waiting, term()} }).
-record(worker_handoff, {fitting :: #fitting{},
                         queue :: queue(),
                         blocking :: queue(),
                         archive :: term()}).
-record(handoff, {fold :: fun((Key::term(), Value::term(), Acc::term())
                              -> NewAcc::term()),
                  acc :: term(),
                  sender :: sender()}).

-record(state, {partition :: ring_idx(),
                worker_sup :: pid(),
                workers :: [#worker{}],
                worker_limit :: pos_integer(),
                worker_q_limit :: pos_integer(),
                workers_archiving :: [#worker{}],
                handoff :: undefined | starting | cancelled | finished
                         | #handoff{}}).

-record(cmd_enqueue, {fitting :: #fitting{},
                      input :: term()}).
-record(cmd_eoi, {fitting :: #fitting{}}).
-record(cmd_next_input, {fitting :: #fitting{}}).
-record(cmd_archive, {fitting :: #fitting{},
                      archive :: term()}).
-record(cmd_status, {sender :: term()}).

%% API

%% @doc Start the vnode, if it isn't started already.
-spec start_vnode(ring_idx()) -> {ok, pid()}.
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Evaluate `Validator(Thing)'; return `Thing' if the evaluation
%%      returns `true', or call {@link erlang:exit/1} if the
%%      evaluation returns `false'. (`Msg' is used in the error reason
%%      passed to exit/1.)
-spec validate_or_exit(term(), fun((term) -> boolean()), string()) ->
         term().
validate_or_exit(Thing, Validator, Msg) ->
    case Validator(Thing) of
        true -> Thing;
        false ->
            error_logger:error_msg(Msg++"~n   (found ~p)", [Thing]),
            exit({invalid_config, {Msg, Thing}})
    end.

%% @doc Initialize the vnode.  This function validates the limits set
%%      in the application environment, and starts the worker
%%      supervisor.
%%
%%      Two application environment variables matter to the vnode:
%%<dl><dt>
%%      `worker_limit'
%%</dt><dd>
%%      Positive integer, default 50. The maximum number of workers
%%      allowed to operate on this vnode.
%%</dd><dt>
%%      `worker_queue_limit'
%%</dt><dd>
%%      Positive integer, default 64. The maximum length of each
%%      worker's input queue.
%%</dd></dl>
-spec init([ring_idx()]) -> {ok, #state{}}.
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

%% @doc Get a ring partition index for any local vnode.  Used to
%%      semi-randomly choose a local vnode if the spec for the head
%%      fitting of a pipeline uses a partition-choice function of
%%      `follow'.
-spec any_local_vnode() -> ring_idx().
any_local_vnode() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    hd(riak_core_ring:my_indices(Ring)).

%% @doc Queue the given `Input' for processing by the `Fitting'.  This
%%      function handles getting the input to the correct vnode by
%%      evaluating the fitting's partition-choice function (`partfun')
%%      on the input.
-spec queue_work(#fitting{}, term()) ->
         ok | {error, worker_limit_reached | worker_startup_failed}.
queue_work(#fitting{partfun=follow}=Fitting, Input) ->
    %% this should only happen if someone sets up a pipe with
    %% the first fitting as partfun=follow
    queue_work(Fitting, Input, any_local_vnode());
queue_work(#fitting{partfun=PartFun}=Fitting, Input) ->
    queue_work(Fitting, Input, PartFun(Input)).

%% @doc Queue the given `Input' for processing the the `Fitting' on
%%      the vnode specified by `PartitionOverride'.  This version of
%%      the function is used to support the `follow' partfun, by
%%      allowing a worker to send the input directly to the vnode it
%%      works for.
-spec queue_work(#fitting{}, term(), ring_idx()) ->
         ok | {error, worker_limit_reached | worker_startup_failed}.
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

%% @doc Send end-of-inputs for a fitting to a vnode.  Note: this
%%      should only be called by `riak_pipe_fitting' processes.  This
%%      will cause the vnode to shutdown the worker, dispose of the
%%      queue, and send a `done' to the fitting, once the queue is
%%      emptied.
-spec eoi(pid(), #fitting{}) -> ok.
eoi(Pid, Fitting) ->
    riak_core_vnode:send_command(Pid, #cmd_eoi{fitting=Fitting}).

%% @doc Request the next input from the queue for the given fitting
%%      from a vnode.  Note: this should only be called by the worker
%%      process for that fitting-vnode pair.  This will cause the
%%      vnode to send the next input to the worker process for this
%%      fitting.
-spec next_input(pid(), #fitting{}) -> ok.
next_input(Pid, Fitting) ->
    riak_core_vnode:send_command(Pid, #cmd_next_input{fitting=Fitting}).

%% @doc Send the result of archiving a worker to the vnode that owns
%%      that worker.  Note: this should only be called by the worker
%%      being archived.  This will cause the vnode to send that
%%      worker's queue and archive to its handoff partner when
%%      instructed to do so.
-spec reply_archive(pid(), #fitting{}, term()) -> ok.
reply_archive(Pid, Fitting, Archive) ->
    riak_core_vnode:send_command(Pid, #cmd_archive{fitting=Fitting,
                                                   archive=Archive}).

%% @doc Get some information about the worker queues on this vnode.
%%      The result is a tuple of the form `{PartitionNumber,
%%      [WorkerProplist]}'.  Each WorkerProplist contains tagged
%%      tuples, such as:
%%<dl><dt>
%%      `fitting'
%%</dt><dd>
%%      The pid of the fitting the worker implements.
%%</dd><dt>
%%      `name'
%%</dt><dd>
%%      The name of the fitting.
%%</dd><dt>
%%      `module'
%%</dt><dd>
%%      The module that implements the fitting.
%%</dd><dt>
%%      `state'
%%</dt><dd>
%%      The state of the worker.  One of `working', `waiting', `init'.
%%</dd><dt>
%%      `inputs_done'
%%</dt><dd>
%%      Boolean: true if `eoi' has been delivered for this fitting,
%%      false otherwise.
%%</dd><dt>
%%      `queue_length'
%%</dt><dd>
%%      Integer number of items in the worker's queue.
%%</dd><dt>
%%      `blocking_lenght'
%%</dt><dd>
%%      Integer number of requests blocking on the queue.
%%</dd></dl>
-spec status(pid()) -> {ring_idx(), [[{atom(), term()}]]}.
status(Pid) ->
    Ref = make_ref(),
    riak_core_vnode:send_command(Pid, #cmd_status{sender={raw, Ref, self()}}),
    receive
        {Ref, Reply} -> Reply
    end.

%% @doc Handle a vnode command.
-spec handle_command(term(), sender(), #state{}) ->
          {reply, term(), #state{}}
        | {noreply, #state{}}.
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

%% @doc Handle a handoff command.
-spec handle_handoff_command(term(), sender(), #state{}) ->
         {reply, term(), #state{}}
       | {noreply, #state{}}
       | {forward, #state{}}.
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

%% @doc Be prepared to handoff.
-spec handoff_starting(node(), #state{}) -> {true, #state{}}.
handoff_starting(_TargetNode, State) ->
    {true, State#state{handoff=starting}}.

%% @doc Stop handing off before getting started.
-spec handoff_cancelled(#state{}) -> {ok, #state{}}.
handoff_cancelled(#state{handoff=starting, workers_archiving=[]}=State) ->
    %%TODO: handoff is only cancelled before anything is handed off, right?
    {ok, State#state{handoff=cancelled}}.

%% @doc Note that handoff has completed.
-spec handoff_finished(node(), #state{}) -> {ok, #state{}}.
handoff_finished(_TargetNode, #state{workers=[]}=State) ->
    %% #state.workers should be empty, because they were all handed off
    %% clear out list of handed off items
    {ok, State#state{handoff=finished}}.

%% @doc Accept handoff data from some other node.  `Data' should be a
%%      term_to_binary-ed `#worker_handoff{}' record.  See {@link
%%      encode_handoff_item/1}.
%%
%%      Ensure that a worker is running for the fitting, merge queues,
%%      and prepare to handle archive transfer.
-spec handle_handoff_data(binary(), #state{}) ->
         {reply, ok | {error, term()}, #state{}}.
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

%% @doc Produce a binary representing the worker data to handoff.
-spec encode_handoff_item(#fitting{}, {queue(), queue(), term()}) ->
         binary().
encode_handoff_item(Fitting, {Queue, Blocking, Archive}) ->
    term_to_binary(#worker_handoff{fitting=Fitting,
                                   queue=Queue,
                                   blocking=Blocking,
                                   archive=Archive}).

%% @doc Determine whether this vnode has any running workers.
-spec is_empty(#state{}) -> {boolean(), #state{}}.
is_empty(#state{workers=Workers}=State) ->
    {Workers==[], State}.

%% @doc Unused.
-spec delete(#state{}) -> {ok, #state{}}.
delete(#state{workers=[]}=State) ->
    %%TODO: delete is only called if is_empty/1==true, right?
    {ok, State}.

%% @doc Unused.
-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @doc Handle an 'EXIT' message from a linked process.
%%
%%      If the linked process was a worker that died normally after
%%      receiving end-of-inputs and emptying its queue, send `done' to
%%      the fitting, and remove the worker's entry in the vnodes list.
%%
%%      If the linked process was a worker that died abnormally,
%%      attempt to restart it.
%%
%%      If the linked process was a fitting, kill the worker associated
%%      with that fitting and dispose of its queue.
-spec handle_exit(pid(), term(), #state{}) -> {noreply, #state{}}.
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

%% @doc Handle a command to add an input to the work queue.  This
%%      function ensures there is a worker running for the fitting (if
%%      there is room within `worker_limit').  It then adds the input
%%      to the queue and replies `ok' if the queue is below capacity
%%      (`worker_queue_limit').  If the queue is at capacity, the
%%      request is added to the blocking queue, and no reponse is sent
%%      (until later, when the input is moved from the blocking queue
%%      to the work queue).
-spec enqueue_internal(#cmd_enqueue{}, sender(), #state{}) ->
         {reply,
          ok | {error, worker_limit_reached | worker_startup_failed},
          #state{}}
       | {noreply, #state{}}.
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

%% @doc Find the worker for the given `Fitting', or start one if there
%%      is room on this vnode.  Returns `{ok, Worker}' if a worker
%%      [now] exists, or `worker_limit_reached' otherwise.
-spec worker_for(#fitting{}, #state{}) ->
         {ok, #worker{}} | worker_limit_reached | worker_startup_failed.
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

%% @doc Start a new worker for the given `Fitting'.  This function
%%      requests the details from the fitting process, links to the
%%      fitting process (TODO: as soon as vnodes can receive 'DOWN',
%%      use links), starts and links the worker process, and sets up
%%      the queues for it.
-spec new_worker(#fitting{}, #state{}) ->
         {ok, #worker{}} | worker_startup_failed.
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

%% @doc Add an input to the worker's queue.  If the worker is
%%      `waiting', send the input to it, skipping the queue.  If the
%%      queue is full, add the request to the blocking queue instead.
-spec add_input(#worker{}, term(), sender()) ->
         {ok | queue_full, #worker{}}.
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

%% @doc Merge the worker on this vnode with the worker from another
%%      vnode.  (The grungy part of {@link, handle_handoff_data/2}.)
-spec handoff_worker(#worker{}, queue(), queue(), Archive::term()) ->
          #worker{}.
handoff_worker(#worker{q=Q, blocking=Blocking}=Worker,
               HandoffQ, HandoffBlocking, HandoffState) ->
    %% simply concatenate queues, and hold the handoff state for
    %% the next available time to ask the worker to deal with it
    MergedWorker = Worker#worker{
                     q=queue:join(Q, HandoffQ),
                     blocking=queue:join(Blocking, HandoffBlocking),
                     handoff={waiting, HandoffState}},
    maybe_wake_for_handoff(MergedWorker).

%% @doc If the worker is `waiting', send it the handoff data to
%%      process.  Otherwise, just leave it be until it asks for the
%%      next input.
-spec maybe_wake_for_handoff(#worker{}) -> #worker{}.
maybe_wake_for_handoff(#worker{state=waiting}=Worker) ->
    send_handoff(Worker),
    Worker#worker{state={working, handoff}, handoff=undefined};
maybe_wake_for_handoff(Worker) ->
    %% worker is doing something else - send handoff later
    Worker.

%% @doc Handle an end-of-inputs command.  If the worker for the given
%%      fitting is `waiting', ask it to shutdown immediately.
%%      Otherwise, mark that eoi was received, and ask the worker to
%%      shut down when it empties its queue.
-spec eoi_internal(#cmd_eoi{}, #state{}) -> {noreply, #state{}}.
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

%% @doc Handle a request from a worker for its next input.
%%
%%      If this vnode is handing data off, ask the worker to archive.
%%
%%      If this vnode is not handing off, send the next input.
-spec next_input_internal(#cmd_next_input{}, #state{}) ->
          {noreply, #state{}}.
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

%% @doc Handle pulling data off of a worker's queue and sending it to
%%      the worker.
%%
%%      If there are no inputs in the worker's queue, mark the worker
%%      as `waiting' if it has not yet received its end-of-inputs
%%      message, or ask it to shutdown if eoi was received.
%%
%%      If there are inputs in the queue, pull off the front one and
%%      send it along.  If there are items in the blocking queue, move
%%      the front one to the end of the work queue, and reply `ok' to
%%      the process that requested its addition (unblocking it).
-spec next_input_nohandoff(#worker{}, #state{}) -> {noreply, #state{}}.
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

%% @doc Send an input to a worker.
-spec send_input(#worker{}, term()) -> ok.
send_input(Worker, Input) ->
    riak_pipe_vnode_worker:send_input(Worker#worker.pid, Input).

%% @doc Send an request to archive to a worker.
-spec send_archive(#worker{}) -> ok.
send_archive(Worker) ->
    riak_pipe_vnode_worker:send_archive(Worker#worker.pid).

%% @doc Send a request to merge another node's archived worker state
%%      with this worker.
-spec send_handoff(#worker{}) -> ok.
send_handoff(#worker{handoff={waiting, HO}}=Worker) ->
    riak_pipe_vnode_worker:send_handoff(Worker#worker.pid, HO).

%% @doc Find a worker by its pid.
-spec worker_by_pid(pid(), #state{}) -> {ok, #worker{}} | none.
worker_by_pid(Pid, #state{workers=Workers}) ->
    case lists:keyfind(Pid, #worker.pid, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.

%% @doc Find a worker by the fitting it works for.
-spec worker_by_fitting(#fitting{}, #state{}) -> {ok, #worker{}} | none.
worker_by_fitting(Fitting, #state{workers=Workers}) ->
    case lists:keyfind(Fitting, #worker.fitting, Workers) of
        #worker{}=Worker -> {ok, Worker};
        false            -> none
    end.

%% @doc Find a worker by the pid of the fitting it works for.
-spec worker_by_fitting_pid(pid(), #state{}) -> {ok, #worker{}} | none.
worker_by_fitting_pid(Pid, #state{workers=Workers}) ->
    case [ W || #worker{fitting=F}=W <- Workers, F#fitting.pid =:= Pid ] of
        [#worker{}=Worker] -> {ok, Worker};
        []                 -> none
    end.

%% @doc Update the worker's entry in the vnode's state.  Matching
%%      is done by fitting.
-spec replace_worker(#worker{}, #state{}) -> #state{}.
replace_worker(#worker{fitting=F}=Worker, #state{workers=Workers}=State) ->
    NewWorkers = lists:keystore(F, #worker.fitting, Workers, Worker),
    State#state{workers=NewWorkers}.

%% @doc Remove the worker's entry from the vnode's state.  Matching is
%%      done by fitting.
-spec remove_worker(#worker{}, #state{}) -> #state{}.
remove_worker(#worker{fitting=F}, #state{workers=Workers}=State) ->
    NewWorkers = lists:keydelete(F, #worker.fitting, Workers),
    State#state{workers=NewWorkers}.

%% @doc Restart the worker after failure.  The input that the worker
%%      was processing is skipped.  If the worker fails to restart,
%%      the inputs in its work queue are sent to the fitting process,
%%      and the requests in its block queue are sent `fail' responses.
-spec restart_worker(#worker{}, #state{}) -> #state{}.
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

%% @doc Reply to a request that has been waiting in a worker's blocked
%%      queue.
-spec reply_to_blocker(term(), term()) -> true.
reply_to_blocker(Blocker, Reply) ->
    riak_core_vnode:reply(Blocker, Reply).

%% @doc Send a `done' message to the fitting specified.
-spec send_done(#fitting{}) -> ok.
send_done(Fitting) ->
    riak_pipe_fitting:worker_done(Fitting).

%% @doc Handle a request for status.  Generate the worker detail
%%      list, and send it to the requester.
-spec status_internal(#cmd_status{}, #state{}) -> {noreply, #state{}}.
status_internal(#cmd_status{sender=Sender},
                #state{partition=P, workers=Workers}=State) ->
    Reply = {P, [ worker_detail(W) || W <- Workers ]},
    %% riak_core_vnode:command(Pid) does not set reply properly
    riak_core_vnode:reply(Sender, Reply),
    {noreply, State}.

%% @doc Generate the status details for the given worker.
-spec worker_detail(#worker{}) -> [{atom(), term()}].
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

%% @doc Handle the fold request to start handoff.  Immediately ask all
%%      `waiting' workers to archive, and note that others should
%%      archive as they finish their current inputs.
-spec handoff_cmd_internal(term(), sender(), #state{}) ->
         {noreply, #state{}}.
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

%% @doc The vnode is in handoff, and a worker requested its next
%%      input.  Instead of giving it the next input, ask it to
%%      archive, so it can be sent to the handoff partner.
-spec archive_fitting(#fitting{}, #state{}) -> #state{}.
archive_fitting(F, State) ->
    {ok, W} = worker_by_fitting(F, State),
    send_archive(W),
    State#state{workers=remove_worker(W, State),
                workers_archiving=[W#worker{state={working, archive}}
                                   |State#state.workers_archiving]}.

%% @doc A worker finished archiving, and sent the archive back to the
%%      vnode.  Evaluate the handoff fold function, and remove the
%%      worker from the vnode's state.
%%
%%      If there are no more workers to archive, reply to the handoff
%%      requester with the accumulated result.
-spec archive_internal(#cmd_archive{}, #state{}) -> {noreply, #state{}}.
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
