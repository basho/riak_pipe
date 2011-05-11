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

%% @doc The builder starts and monitors the fitting processes.
%%
%%      This startup process is how each fitting learns about the
%%      fitting that follows it.  The builder is also the process that
%%      the client asks to find the head fitting.
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

-export_type([builder/0]).
-record(state, {options :: riak_pipe:exec_opts(),
                ref :: reference(),
                unstarted :: [#fitting_spec{}],
                alive :: [{#fitting{}, reference()}], % monitor ref
                waiting :: [term()]}). % gen_fsm From reply handles

-opaque state() :: #state{}.
-opaque builder() :: {pid(), reference()}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a builder to setup the pipeline described by `Spec'.
-spec start_link([riak_pipe:fitting_spec()], riak_pipe:exec_opts()) ->
         {ok, pid(), builder()} | ignore | {error, term()}.
start_link(Spec, Options) ->
    case gen_fsm:start_link(?MODULE, [Spec, Options], []) of
        {ok, Pid} ->
            {sink, #fitting{ref=Ref}} = lists:keyfind(sink, 1, Options),
            {ok, Pid, {Pid, Ref}};
        Error ->
            Error
    end.

%% @doc Notify the `Builder' that the fitting has completed its
%%      startup and is described by `Fitting'.  The value of `Fitting'
%%      is what will be used to tag inputs to vnode queues.
-spec fitting_started(pid(), riak_pipe:fitting()) -> ok.
fitting_started(Builder, Fitting) ->
    gen_fsm:send_event(Builder, {fitting_started, Fitting}).

%% @doc Get the list of pids for fittings that this builder started.
%%      If the builder terminated before this call was made, the
%%      function returns the atom `gone'.
-spec fitting_pids(pid()) -> {ok, {integer(), [pid()]}} | gone.
fitting_pids(Builder) ->
    try
        {ok, gen_fsm:sync_send_all_state_event(Builder, fittings)}
    catch exit:{noproc, _} ->
            gone
    end.

%% @doc Get the `#fitting{}' record describing the lead fitting in
%%      this builder's pipeline.  This function will block until the
%%      builder has finished building the pipeline.
-spec get_first_fitting(builder()) -> {ok, riak_pipe:fitting()}.
get_first_fitting({BuilderPid, Ref}) ->
    gen_fsm:sync_send_event(BuilderPid, {get_first_fitting, Ref}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Initialize the builder fsm (gen_fsm callback).
-spec init([ [riak_pipe:fitting_spec()] | riak_pipe:exec_opts() ]) ->
         {ok, start_first_fitting, state(), 0}.
init([Spec, Options]) ->
    {sink, #fitting{ref=Ref}} = lists:keyfind(sink, 1, Options),
    {ok, start_first_fitting,
     #state{unstarted=lists:reverse(Spec),
            options=Options,
            ref=Ref,
            alive=[],
            waiting=[]},
     0}.

%% @doc Start the tail fitting (gen_fsm callback).  The "first" in the
%%      name comes from the fact that the "tail" fitting is started
%%      first, so its handle can be passed to its predecessor. (TODO)
-spec start_first_fitting(timeout, state()) ->
         {next_state, wait_fitting_start, state()}.
start_first_fitting(timeout, #state{unstarted=[Last|Rest]}=State) ->
    ClientOutput = client_output(State#state.options),
    start_fitting(Last,
                  ClientOutput,
                  State#state.options),
    {next_state, wait_fitting_start, State#state{unstarted=Rest}}.

%% @doc The builder asked the fitting supervisor to start a new
%%      fitting, and is now waiting for confirmation that that fitting
%%      started successfully.  When it receives that confirmation, it
%%      will start the next fitting up the pipe, or go into a wait
%%      state pending pipeline shutdown.  Before going into that wait
%%      state, the head fitting is sent to any clients that asked for it.
-spec wait_fitting_start({fitting_started, riak_pipe:fitting()}, state()) ->
         {next_state, wait_fitting_start | wait_pipeline_shutdown, state()}.
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

%% @doc All fittings have been started, and the builder is just
%%      monitoring the pipeline (and replying to clients looking
%%      for the head fitting).
-spec wait_pipeline_shutdown(term(), state()) ->
         {next_state, wait_pipeline_shutdown, state()}.
wait_pipeline_shutdown(_Event, State) ->
    {next_state, wait_pipeline_shutdown, State}.

%% @doc A client is asking for the head fitting, but that fitting
%%      hasn't started yet.  Delay response for later.
-spec wait_fitting_start({get_first_fitting, reference()},
                         term(), state()) ->
         {next_state, wait_fitting_start, state()}.
wait_fitting_start({get_first_fitting, Ref}, From,
                   #state{ref=Ref, waiting=Waiting}=State) ->
            %% something still unstarted
            %% reply once everything else is up
    {next_state, wait_fitting_start,
     State#state{waiting=[From|Waiting]}};
wait_fitting_start(_, _, State) ->
    %% unknown message - reply {error, unknown} to get rid of it
    {reply, {error, unknown}, wait_fitting_start, State}.

%% @doc A client is asking for the head fitting.  Respond.
-spec wait_pipeline_shutdown({get_first_fitting, reference()},
                             term(), state()) ->
         {reply,
          {ok, riak_pipe:fitting()},
          wait_pipeline_shutdown,
          state()}.
wait_pipeline_shutdown({get_first_fitting, Ref}, _From,
                       #state{ref=Ref,
                              alive=[{FirstFitting,_Ref}|_]}=State) ->
    %% everything is started - reply now
    {reply, {ok, FirstFitting}, wait_pipeline_shutdown, State};
wait_pipeline_shutdown(_, _, State) ->
    %% unknown message - reply {error, unknown} to get rid of it
    {reply, {error, unknown}, wait_pipeline_shutdown, State}.

%% @doc Unused.
-spec handle_event(term(), atom(), state()) ->
         {next_state, atom(), state()}.
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc The only sync event recognized in all states is `fittings',
%%      which retrieves a count of fittings waiting to be started,
%%      and pids for fittings already started.
-spec handle_sync_event(fittings, term(), atom(), state()) ->
         {reply,
          {UnstartedCount::integer(), FittingPids::[pid()]},
          StateName::atom(),
          state()}.
handle_sync_event(fittings, _From, StateName,
                  #state{unstarted=Unstarted, alive=Alive}=State) ->
    Reply = {length(Unstarted),
             [ Pid || {#fitting{pid=Pid},_Ref} <- Alive ]},
    {reply, Reply, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc The only non-gen_fsm message this process expects are `'DOWN''
%%      messages from monitoring the fittings it has started.  When
%%      normal `'DOWN'' messages have been received from all monitored
%%      fittings, this gen_fsm stops with reason `normal'.  If an
%%      error `'DOWN'' message is received for any fitting, this
%%      process exits immediately, with an error reason.
-spec handle_info({'DOWN', reference(), process, pid(), term()},
                  atom(), state()) ->
         {next_state, atom(), state()}
       | {stop, term(), state()}.
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

%% @doc Decide whether to shutdown, or continue waiting for `'DOWN''
%%      messages from other fittings.
-spec maybe_shutdown(term(), atom(), state()) ->
         {stop, normal, state()}
       | {stop, {fitting_exited_abnormally, term()}, state()}
       | {next_state, wait_pipeline_shutdown, state()}.
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

%% @doc Unused.
-spec terminate(term(), atom(), state()) -> ok.
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Start a new fitting, as specified by `Spec', sending its
%%      output to `Output'.
-spec start_fitting(riak_pipe:fitting_spec(),
                    riak_pipe:fitting(),
                    riak_pipe:exec_opts()) ->
         {ok, pid()}.
start_fitting(Spec, Output, Options) ->
    ?DPF("Starting fitting for ~p", [Spec]),
    riak_pipe_fitting_sup:add_fitting(
      self(), Spec, Output, Options).

%% @doc Find the sink in the options passed.
-spec client_output(riak_pipe:exec_opts()) -> riak_pipe:fitting().
client_output(Options) ->
    proplists:get_value(sink, Options).

%% @doc Reply to all waiting requests for the head fitting.
%%      `Waiting' is a list of the "`From'" parameters that gen_fsm
%%      passed to the sync event handlers.
-spec announce_first_fitting(riak_pipe:fitting(), [term()]) -> ok.
announce_first_fitting(Fitting, Waiting) ->
    [ gen_fsm:reply(W, {ok, Fitting}) || W <- Waiting ],
    ok.
