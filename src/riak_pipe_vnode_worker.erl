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

-module(riak_pipe_vnode_worker).

-behaviour(gen_fsm).

%% API
-export([start_link/3]).
-export([send_input/2,
         send_handoff/2,
         send_archive/1,
         send_output/3,
         send_output/4]).
-export([behaviour_info/1]).

%% gen_fsm callbacks
-export([
         init/1,
         initial_input_request/2,
         wait_for_input/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4
        ]).

-include("riak_pipe.hrl").

-record(state, {details, vnode, modstate}).

behaviour_info(callbacks) ->
    [{init,2},
     {process,2},
     {done,1}];
behaviour_info(_Other) ->
    undefined.

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
start_link(Partition, VnodePid, FittingDetails) ->
    gen_fsm:start_link(?MODULE, [Partition, VnodePid, FittingDetails], []).

send_input(WorkerPid, Input) ->
    gen_fsm:send_event(WorkerPid, {input, Input}).

send_handoff(WorkerPid, Handoff) ->
    gen_fsm:send_event(WorkerPid, {handoff, Handoff}).

send_archive(WorkerPid) ->
    gen_fsm:send_event(WorkerPid, archive).

send_output(Output, FromPartition,
            #fitting_details{output=Fitting}=Details) ->
    send_output(Output, FromPartition, Details, Fitting).

send_output(Output, FromPartition,
            #fitting_details{name=Name}=_Details,
            FittingOverride) ->
    case FittingOverride#fitting.partfun of
        sink ->
            riak_pipe:result(Name, FittingOverride, Output),
            ok;
        follow ->
            ok = riak_pipe_vnode:queue_work(
                   FittingOverride, Output, FromPartition);
        _ ->
            ok = riak_pipe_vnode:queue_work(FittingOverride, Output)
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
init([Partition, VnodePid, #fitting_details{module=Module}=FittingDetails]) ->
    try
        {ok, ModState} = Module:init(Partition, FittingDetails),
        {ok, initial_input_request,
         #state{details=FittingDetails,
                vnode=VnodePid,
                modstate=ModState},
         0}
    catch Type:Error ->
            {stop, {init_failed, Type, Error}}
    end.

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
%% state_name(_Event, State) ->
%%     {next_state, state_name, State}.
initial_input_request(timeout, State) ->
    request_input(State),
    {next_state, wait_for_input, State}.

wait_for_input({input, done}, State) ->
    ok = process_done(State),
    {stop, normal, State}; %%TODO: monitor
wait_for_input({input, Input}, State) ->
    NewState = process_input(Input, State),
    request_input(NewState),
    {next_state, wait_for_input, NewState};
wait_for_input({handoff, HandoffState}, State) ->
    %% receiving handoff from another vnode
    NewState = handoff(HandoffState, State),
    request_input(NewState),
    {next_state, wait_for_input, NewState};
wait_for_input(archive, State) ->
    %% sending handoff to another vnode
    Archive = archive(State),
    reply_archive(Archive, State),
    {stop, normal, State}.

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
%% state_name(_Event, _From, State) ->
%%     Reply = ok,
%%     {reply, Reply, state_name, State}.

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

request_input(#state{vnode=Vnode, details=Details}) ->
    riak_pipe_vnode:next_input(Vnode, Details#fitting_details.fitting).

process_input(Input, #state{details=FD, modstate=ModState}=State) ->
    Module = FD#fitting_details.module,
    {ok, NewModState} = Module:process(Input, ModState),
    State#state{modstate=NewModState}.

process_done(#state{details=FD, modstate=ModState}) ->
    Module = FD#fitting_details.module,
    Module:done(ModState).

handoff(HandoffArchive, #state{details=FD, modstate=ModState}=State) ->
    Module = FD#fitting_details.module,
    case lists:member({handoff, 2}, Module:module_info(exports)) of
        true ->
            {ok, NewModState} = Module:handoff(HandoffArchive, ModState),
            State#state{modstate=NewModState};
        false ->
            %% module doesn't bother handing off state
            State
    end.

archive(#state{details=FD, modstate=ModState}) ->
    Module = FD#fitting_details.module,
    case lists:member({archive, 1}, Module:module_info(exports)) of
        true ->
            {ok, Archive} = Module:archive(ModState),
            Archive;
        false ->
            %% module doesn't bother handif off state
            undefined
    end.

reply_archive(Archive, #state{vnode=Vnode, details=Details}) ->
    riak_pipe_vnode:reply_archive(Vnode,
                                  Details#fitting_details.fitting,
                                  Archive).
