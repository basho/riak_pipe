%%--------------------------------------------------------------------
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
%%--------------------------------------------------------------------

-module(riak_pipe_fitting).

-behaviour(gen_fsm).

%% API
-export([start_link/4]).
-export([eoi/1,
         get_details/2,
         worker_done/1,
         workers/1]).
-export([validate_fitting/1,
         format_name/1]).

%% gen_fsm callbacks
-export([init/1,
         wait_upstream_eoi/2, wait_upstream_eoi/3,
         wait_workers_done/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_pipe.hrl").
-include("riak_pipe_log.hrl").
-include("riak_pipe_debug.hrl").

-record(state, {builder, details, workers}).
-record(worker, {partition, pid, monitor}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Builder, Spec, Output, Options) ->
    gen_fsm:start_link(?MODULE, [Builder, Spec, Output, Options], []).

eoi(#fitting{partfun=sink}=Sink) ->
    riak_pipe:eoi(Sink);
eoi(#fitting{pid=Pid}) ->
    gen_fsm:send_event(Pid, eoi).

get_details(Fitting, Partition) ->
    try
        gen_fsm:sync_send_event(Fitting#fitting.pid,
                                {get_details, Partition, self()})
    catch exit:{noproc,_} ->
            gone
    end.

worker_done(Fitting) ->
    gen_fsm:sync_send_event(Fitting#fitting.pid, {done, self()}).

workers(Fitting) ->
    try 
        {ok, gen_fsm:sync_send_all_state_event(Fitting, workers)}
    catch exit:{noproc, _} ->
            gone
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Builder,
      #fitting_spec{name=Name, module=Module, arg=Arg, partfun=PartFun},
      Output,
      Options]) ->
    Fitting = #fitting{pid=self(),
                       ref=make_ref(),
                       partfun=PartFun},
    Details = #fitting_details{fitting=Fitting,
                               name=Name,
                               module=Module,
                               arg=Arg,
                               output=Output,
                               options=Options},
    
    ?T(Details, [], {fitting, init_started}),

    %% TODO: fix riak_core_vnode to pass DOWN to handle_down
    process_flag(trap_exit, true),

    erlang:link(Builder),
    riak_pipe_builder:fitting_started(Builder, Fitting),

    ?T(Details, [], {fitting, init_finished}),

    {ok, wait_upstream_eoi,
     #state{builder=Builder, details=Details, workers=[]}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
wait_upstream_eoi(eoi, #state{workers=Workers, details=Details}=State) ->
    ?T(Details, [eoi], {fitting, receive_eoi}),
    [ riak_pipe_vnode:eoi(Pid, Details#fitting_details.fitting)
      || #worker{pid=Pid} <- Workers ],
    {next_state, wait_workers_done, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
wait_upstream_eoi({get_details, Partition, Pid}=M, _From, State) ->
    ?T(State#state.details, [get_details], {fitting, M}),
    NewState = add_worker(Partition, Pid, State),
    {reply,
     {ok, State#state.details},
     wait_upstream_eoi,
     NewState};
wait_upstream_eoi({done, Pid}=M, _From, State) ->
    %% handoff caused early done
    ?T(State#state.details, [done], {early_fitting, M}),
    case lists:keytake(Pid, #worker.pid, State#state.workers) of
        {value, Worker, Rest} ->
            erlang:demonitor(Worker#worker.monitor);
        false ->
            Rest = State#state.workers
    end,
    %% don't check for empty Rest like in wait_workers_done, though
    %% because we haven't seen eoi yet
    {reply, ok, wait_upstream_eoi, State#state{workers=Rest}}.
        
wait_workers_done({get_details, Partition, Pid}=M, _From, State) ->
    %% handoff caused a late get_details
    ?T(State#state.details, [get_details], {late_fitting, M}),
    %% send details, and monitor as usual
    NewState = add_worker(Partition, Pid, State),
    %% also send eoi, to have worker immediately finish up
    Details = NewState#state.details,
    riak_pipe_vnode:eoi(Pid, Details#fitting_details.fitting),
    {reply,
     {ok, NewState#state.details},
     wait_workers_done,
     NewState};
wait_workers_done({done, Pid}=M, _From, State) ->
    ?T(State#state.details, [done], {fitting, M}),
    case lists:keytake(Pid, #worker.pid, State#state.workers) of
        {value, Worker, Rest} ->
            erlang:demonitor(Worker#worker.monitor);
        false ->
            Rest = State#state.workers
    end,
    case Rest of
        [] ->
            Details = State#state.details,
            ?T(Details, [eoi], {fitting, send_eoi}),
            riak_pipe_fitting:eoi(Details#fitting_details.output),
            {stop, normal, ok, State#state{workers=[]}};
        _ ->
            {reply, ok, wait_workers_done, State#state{workers=Rest}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(workers, _From, StateName, #state{workers=Workers}=State) ->
    Partitions = [ P || #worker{partition=P} <- Workers ],
    {reply, Partitions, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', Ref, _, _, _}, StateName, State) ->
    Rest = lists:keydelete(Ref, #worker.monitor, State#state.workers),
    %% TODO: timeout in case we were waiting for 'done'
    {next_state, StateName, State#state{workers=Rest}};
handle_info({'EXIT', Builder, _}, _StateName, #state{builder=Builder}=S) ->
    %% TODO: this will be unnecessary when not trapping exits for
    %%       vnode monitoring
    {stop, builder_exited, S};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

add_worker(Partition, Pid, State) ->
    %% check if we're already monitoring this pid before setting up a
    %% new monitor (in case pid re-requests details)
    case worker_by_partpid(Partition, Pid, State) of
        {ok, _Worker} ->
            %% already monitoring
            State;
        none ->
            Ref = erlang:monitor(process, Pid),
            State#state{workers=[#worker{partition=Partition,
                                         pid=Pid,
                                         monitor=Ref}
                                 |State#state.workers]}
    end.

worker_by_partpid(Partition, Pid, #state{workers=Workers}) ->
    case [ W || #worker{partition=A, pid=I}=W <- Workers,
                A == Partition, I == Pid] of
        [#worker{}=Worker] -> {ok, Worker};
        []                 -> none
    end.

validate_fitting(#fitting_spec{name=Name,
                               module=Module,
                               arg=Arg,
                               partfun=PartFun}) ->
    case riak_pipe_v:validate_module("module", Module) of
        ok -> ok;
        {error, ModError} ->
            error_logger:error_msg(
              "Invalid module in fitting spec \"~s\":~n~s",
              [format_name(Name), ModError]),
            throw(badarg)
    end,
    case validate_argument(Module, Arg) of
        ok -> ok;
        {error, ArgError} ->
            error_logger:error_msg(
              "Invalid module argument in fitting spec \"~s\":~n~s",
              [format_name(Name), ArgError]),
            throw(badarg)
    end,
    case validate_partfun(PartFun) of
        ok -> ok;
        {error, PFError} ->
            error_logger:error_msg(
              "Invalid partfun in fitting spec \"~s\":~n~s",
              [format_name(Name), PFError]),
            throw(badarg)
    end;
validate_fitting(Other) ->
    error_logger:error_msg(
      "Invalid fitting_spec given (expected fitting_spec record):~n~P",
      [Other, 3]),
    throw(badarg).

%% assumes Module has already been validated
validate_argument(Module, Arg) ->
    case lists:member({validate_arg, 1}, Module:module_info(exports)) of
        true ->
            try
                Module:validate_arg(Arg)
            catch Type:Error ->
                    {error, io_lib:format(
                              "failed to validate module argument: ~p:~p",
                              [Type, Error])}
            end;
        false ->
            ok %% don't force modules to validate their args
    end.

validate_partfun(follow) ->
    ok;
validate_partfun(PartFun) ->
    riak_pipe_v:validate_function("partfun", 1, PartFun).

format_name(Name) when is_binary(Name) ->
    Name;
format_name(Name) when is_list(Name) ->
    case is_iolist(Name) of
        true -> Name;
        false -> io_lib:format("~p", [Name])
    end;
format_name(Name) ->
    io_lib:format("~p", [Name]).

is_iolist(Name) when is_list(Name) ->
    lists:all(fun is_iolist/1, Name);
is_iolist(Name) when is_binary(Name) ->
    true;
is_iolist(Name) when is_integer(Name), Name >= 0, Name =< 255 ->
    true;
is_iolist(_) ->
    false.
