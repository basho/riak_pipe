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
-module(riak_pipe_builder).

-behaviour(gen_fsm).

%% API
-export([start_link/2]).
-export([fitting_started/2,
         fitting_pids/1,
         get_first_fitting/1]).

%% gen_fsm callbacks
-export([init/1,
         start_first_fitting/2,
         wait_fitting_start/2,
         wait_fitting_start/3,
         wait_pipeline_shutdown/2,
         wait_pipeline_shutdown/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

-record(state, {options, unstarted, alive, waiting}).

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
start_link(Spec, Options) ->
    gen_fsm:start_link(?MODULE, [Spec, Options], []).

fitting_started(Builder, Fitting) ->
    gen_fsm:send_event(Builder, {fitting_started, Fitting}).

fitting_pids(Builder) ->
    try
        {ok, gen_fsm:sync_send_all_state_event(Builder, fittings)}
    catch exit:{noproc, _} ->
            gone
    end.

get_first_fitting(Builder) ->
    gen_fsm:sync_send_event(Builder, get_first_fitting).

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
init([Spec, Options]) ->
    {ok, start_first_fitting,
     #state{unstarted=lists:reverse(Spec),
            options=Options,
            alive=[],
            waiting=[]},
     0}.

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
start_first_fitting(timeout, #state{unstarted=[Last|Rest]}=State) ->
    ClientOutput = client_output(State#state.options),
    start_fitting(Last,
                  ClientOutput,
                  State#state.options),
    {next_state, wait_fitting_start, State#state{unstarted=Rest}}.

wait_fitting_start({fitting_started, Fitting},
                   #state{alive=Alive}=State) ->
    Ref = erlang:monitor(process, Fitting#fitting.pid),
    AliveState = State#state{alive=[{Fitting,Ref}|Alive]},
    case AliveState#state.unstarted of
        [] ->
            %% toss the first fitting to anyone wanting it
            announce_first_fitting(Fitting, AliveState#state.waiting),
            {next_state, wait_pipeline_shutdown,
             AliveState#state{waiting=[]}};
        [Next|Rest] ->
            start_fitting(Next,
                          Fitting,
                          AliveState#state.options),
            {next_state, wait_fitting_start,
             AliveState#state{unstarted=Rest}}
    end.

wait_pipeline_shutdown(_Event, State) ->
    {next_state, wait_pipeline_shutdown, State}.

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
wait_fitting_start(get_first_fitting, From,
                   #state{waiting=Waiting}=State) ->
            %% something still unstarted
            %% reply once everything else is up
    {next_state, wait_fitting_start,
     State#state{waiting=[From|Waiting]}}.

wait_pipeline_shutdown(get_first_fitting, _From,
                       #state{alive=[{FirstFitting,_Ref}|_]}=State) ->
    %% everything is started - reply now
    {reply, {ok, FirstFitting}, wait_pipeline_shutdown, State}.

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
handle_sync_event(fittings, _From, StateName,
                  #state{unstarted=Unstarted, alive=Alive}=State) ->
    Reply = {length(Unstarted),
             [ Pid || {#fitting{pid=Pid},_Ref} <- Alive ]},
    {reply, Reply, StateName, State};
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
handle_info({'DOWN', Ref, process, Pid, Reason}, StateName,
            #state{alive=Alive}=State) ->
    %% stages should exit normally in order,
    %% but messages may be delivered out-of-order
    case lists:keytake(Ref, 2, Alive) of
        {value, {#fitting{pid=Pid}, Ref}, Rest} ->
            %% one of our fittings died
            maybe_shutdown(Reason,
                           StateName,
                           State#state{alive=Rest});
        false ->
            %% this wasn't meant for us - ignore
            {next_state, StateName, State}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

maybe_shutdown(normal, wait_pipeline_shutdown, #state{alive=[]}=S) ->
    %% all fittings stopped normally, and we were waiting for them
    {stop, normal, S};
maybe_shutdown(normal, wait_pipeline_shutdown, State) ->
    %% fittings are beginning to stop, but we're still waiting on some
    {next_state, wait_pipeline_shutdown, State};
maybe_shutdown(Reason, _StateName, State) ->
    %% some fitting exited abnormally
    %% (either non-normal status, or before we were ready)
    %% explode!
    {stop, {fitting_exited_abnormally, Reason}, State}.

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

start_fitting(Spec, Output, Options) ->
    ?DPF("Starting fitting for ~p", [Spec]),
    riak_pipe_fitting_sup:add_fitting(
      self(), Spec, Output, Options).

client_output(Options) ->
    proplists:get_value(sink, Options).

announce_first_fitting(Fitting, Waiting) ->
    [ gen_fsm:reply(W, {ok, Fitting}) || W <- Waiting ],
    ok.
