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
-export([fitting_pids/1,
         get_first_fitting/2]).

%% gen_fsm callbacks
-export([init/1,
         wait_pipeline_shutdown/2,
         wait_pipeline_shutdown/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

-record(state, {options :: riak_pipe:exec_opts(),
                ref :: reference(),
                alive :: [{#fitting{}, reference()}]}). % monitor ref

-opaque state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a builder to setup the pipeline described by `Spec'.
-spec start_link([riak_pipe:fitting_spec()], riak_pipe:exec_opts()) ->
         {ok, pid(), reference()} | ignore | {error, term()}.
start_link(Spec, Options) ->
    case gen_fsm:start_link(?MODULE, [Spec, Options], []) of
        {ok, Pid} ->
            {sink, #fitting{ref=Ref}} = lists:keyfind(sink, 1, Options),
            {ok, Pid, Ref};
        Error ->
            Error
    end.

%% @doc Get the list of pids for fittings that this builder started.
%%      If the builder terminated before this call was made, the
%%      function returns the atom `gone'.
-spec fitting_pids(pid()) -> {ok, FittingPids::[pid()]} | gone.
fitting_pids(Builder) ->
    try
        {ok, gen_fsm:sync_send_all_state_event(Builder, fittings)}
    catch exit:{noproc, _} ->
            gone
    end.

%% @doc Get the `#fitting{}' record describing the lead fitting in
%%      this builder's pipeline.  This function will block until the
%%      builder has finished building the pipeline.
-spec get_first_fitting(pid(), reference()) -> {ok, riak_pipe:fitting()}.
get_first_fitting(BuilderPid, Ref) ->
    gen_fsm:sync_send_event(BuilderPid, {get_first_fitting, Ref}).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Initialize the builder fsm (gen_fsm callback).
-spec init([ [riak_pipe:fitting_spec()] | riak_pipe:exec_opts() ]) ->
         {ok, wait_pipeline_shutdown, state()}.
init([Spec, Options]) ->
    {sink, #fitting{ref=Ref}} = lists:keyfind(sink, 1, Options),
    Fittings = start_fittings(Spec, Options),
    {ok, wait_pipeline_shutdown,
     #state{options=Options,
            ref=Ref,
            alive=Fittings}}.

%% @doc All fittings have been started, and the builder is just
%%      monitoring the pipeline (and replying to clients looking
%%      for the head fitting).
-spec wait_pipeline_shutdown(term(), state()) ->
         {next_state, wait_pipeline_shutdown, state()}.
wait_pipeline_shutdown(_Event, State) ->
    {next_state, wait_pipeline_shutdown, State}.

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
          FittingPids::[pid()],
          StateName::atom(),
          state()}.
handle_sync_event(fittings, _From, StateName,
                  #state{alive=Alive}=State) ->
    Reply = [ Pid || {#fitting{pid=Pid},_Ref} <- Alive ],
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


%% @doc Start and monitor all of the fittings for this builder's
%%      pipeline.
-spec start_fittings([riak_pipe:fitting_spec()],
                     riak_pipe:exec_opts()) ->
           [{riak_pipe:fitting(), reference()}].
start_fittings(Spec, Options) ->
    [Tail|Rest] = lists:reverse(Spec),
    ClientOutput = client_output(Options),
    lists:foldl(fun(FitSpec, [{Output,_}|_]=Acc) ->
                        [start_fitting(FitSpec, Output, Options)|Acc]
                end,
                [start_fitting(Tail, ClientOutput, Options)],
                Rest).

%% @doc Start a new fitting, as specified by `Spec', sending its
%%      output to `Output'.
-spec start_fitting(riak_pipe:fitting_spec(),
                    riak_pipe:fitting(),
                    riak_pipe:exec_opts()) ->
         {riak_pipe:fitting(), reference()}.
start_fitting(Spec, Output, Options) ->
    ?DPF("Starting fitting for ~p", [Spec]),
    {ok, Pid, Fitting} = riak_pipe_fitting_sup:add_fitting(
                           self(), Spec, Output, Options),
    Ref = erlang:monitor(process, Pid),
    {Fitting, Ref}.

%% @doc Find the sink in the options passed.
-spec client_output(riak_pipe:exec_opts()) -> riak_pipe:fitting().
client_output(Options) ->
    proplists:get_value(sink, Options).
