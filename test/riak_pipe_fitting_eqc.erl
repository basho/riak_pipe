%%--------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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

%% @doc Exercise riak_pipe_fitting.
-module(riak_pipe_fitting_eqc).

-compile(export_all).

-ifdef(EQC).

-include("riak_pipe.hrl").

-include_lib("eqc/include/eqc.hrl").
-include_lib("eqc/include/eqc_fsm.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).

-record(state, {
          vnodes = [] :: [pid()] %% the "vnode" processes
         }).

-record(setup, {
          builder :: pid(),
          sink :: #fitting{},
          fitting :: #fitting{}
         }).

-type sink_result() :: {ok, Messages::list()}
                     | {error, Reason::term()}.
-type vnode_result() :: {ok, GotDetails::boolean, Messages::list()}
                      | {error, Reason::term()}.

-record(output, {
          sink :: sink_result(), %% messages the sink received
          vnodes :: [vnode_result()] %% messages each vnode received
         }).

prop_eoi_test_() ->
    % {spawn, 
    %  [{setup,
    %    fun setup/0,
    %    fun cleanup/1,
    %    [%% Check networking/clients are set up 
    %     ?_assert(node() /= 'nonode@nohost'),
    %     ?_assertEqual(pong, net_adm:ping(node())),
    %     {timeout, 60, [?_assertEqual(pang, net_adm:ping('nonode@nohost'))]},
    %     ?_assertMatch({ok,_C}, riak:local_client()),
    %     %% Run the quickcheck tests
    {timeout, 60000, % do not trust the docs - timeout is in msec
      ?_assertEqual(true, quickcheck(numtests(100, ?QC_OUT(prop_eoi()))))
    }.
    %    ]
    %   }
    %  ]
    % }.


%% @doc Make sure that all vnodes that obtain details from a fitting
%% get an eoi message from the fitting.
prop_eoi() ->
    ?FORALL(Cmds, commands(?MODULE),
            begin
                Setup = setup_fitting(),
                try
                    Result = run_commands(?MODULE, Cmds,
                                          [{fitting, Setup#setup.fitting}]),
                    Output = gather_output(Result, Setup),
                    aggregate(zip(state_names(element(1, Result)),
                                  command_names(Cmds)),
                    ?WHENFAIL(
                       print_helpful(Result, Output),
                       check_result(Result, Output, Setup)))
                after
                    cleanup_fitting(Setup)
                end
            end).

print_helpful({_,{Last,_},R}, Output) ->
    io:format("R: ~p~nLast: ~p~nSink: ~p~nVnodes: ~p~n",
              [R, Last, Output#output.sink, Output#output.vnodes]).

%% @doc start a builder and sink for support, then start the fitting
%% we will test
setup_fitting() ->
    Builder = spawn_link(?MODULE, fake_builder, []),
    Sink = spawn_link(?MODULE, fake_sink, []),
    SinkFitting = #fitting{pid=Sink, ref=make_ref(), chashfun=sink},
    Spec = #fitting_spec{name=eqc},
    Options = [],
    {ok, _Pid, Fitting} = riak_pipe_fitting:start_link(
                            Builder, Spec, SinkFitting, Options),
    #setup{builder=Builder,
           sink=SinkFitting,
           fitting=Fitting}.

%% @doc ask the sink and the vnodes for all messages they have received
gather_output({_, {Last, S}, _}, Setup) ->
    case Last of
        stopping ->
            %% make sure the fitting finishes sending
            %% messages before we go digging
            ok = wait_for_fitting_exit(Setup#setup.fitting);
        _ ->
            ok
    end,
    {ok, SMsgs} = get_sink_msgs(Setup#setup.sink),
    VMsgs = [get_vnode_msgs(V) || V <- S#state.vnodes],
    #output{sink=SMsgs,
            vnodes=VMsgs}.

check_result({_,{stopping,_},R}, Output, Setup) ->
    %% if the test decided to transition from running to stopping...
    Eoi = #pipe_eoi{ref=(Setup#setup.sink)#fitting.ref},
    IfDetailsAlsoEoi =
        fun({ok, true, M}) ->
                %% all vnodes with details must receive eoi
                lists:member({cmd_eoi, Setup#setup.fitting}, M);
           ({ok, false, M}) ->
                %% all vnodes without details must receive nothing
                [] == M;
           (_) ->
                %% unrecognized message
                false
        end,
    %% test must have finished ok
    ok == R andalso
        %% sink must have recevied an eoi
        lists:member(Eoi, Output#output.sink) andalso
        lists:all(IfDetailsAlsoEoi, Output#output.vnodes);
check_result({_,{running,_},R}, Output, Setup) ->
    %% if the test did not decide to transition from running to
    %% stopping (it never sent eoi to the fitting)...
    %% (is this part of the test useful?)
    FittingAlive = fitting_is_alive(Setup#setup.fitting),
    NoVnodeMessages = fun({ok, _, M}) -> [] == M;
                         (_)          -> false
                      end,
    %% test must have finished ok
    ok == R andalso
        %% the fitting process must still be running
        FittingAlive andalso
        %% the sink must have received nothing
        [] == Output#output.sink andalso
        %% the vnodes must have received nothing
        lists:all(NoVnodeMessages, Output#output.vnodes).

%% tear down all of our processes
cleanup_fitting(Setup) ->
    stop_sink(Setup#setup.sink),
    stop_builder(Setup#setup.builder),
    erlang:unlink((Setup#setup.fitting)#fitting.pid),
    exit((Setup#setup.fitting)#fitting.pid, kill).

initial_state() ->
    running.

initial_state_data() ->
    #state{}.

%%% STATES

%% make the test stay in running a bit more often:
%% with equal weights, aggregate shows 85% stopping state;
%% with 10x running->running, aggregate shows 60% stopping, 40% running
weight(running, running, _Call)  -> 10;
weight(_, _, _Call)              -> 1.

%% these are available in all states
nontransitional(_s) ->
    %% always okay to start a vnode
    [{history, {call, ?MODULE, start_vnode, [{var, fitting}]}}].

running(S) ->
    [{stopping, {call, riak_pipe_fitting, eoi, [{var, fitting}]}}]
        ++nontransitional(S).

stopping(S) ->
    nontransitional(S).

precondition(_From,_To,_S,_Call) ->
    true.

next_state_data(_F,_T,S,R,{call,?MODULE,start_vnode,_}) ->
    S#state{vnodes=[R|S#state.vnodes]};
next_state_data(_,_,S,_,_) ->
    S.

postcondition(_, _, _, {call,?MODULE,start_vnode,_}, R) ->
    is_pid(R);
postcondition(_, _, _, {call,riak_pipe_fitting,eoi,_}, R) ->
    ok == R;
postcondition(_, _, _, _, _) ->
    true.

%% MOCKS

fake_builder() ->
    receive
        stop -> ok
    end.

stop_builder(B) ->
    B ! stop.

%% just accumulates messages
fake_sink() ->
    fake_sink([]).
fake_sink(Msgs) ->
    receive
        stop ->
            ok;
        {get, Ref, Pid} ->
            Pid ! {msgs, Ref, Msgs},
            fake_sink([]);
        Any ->
            fake_sink([Any|Msgs])
    end.

stop_sink(#fitting{pid=S}) ->
    S ! stop.

get_sink_msgs(#fitting{pid=S}) ->
    R = make_ref(),
    M = erlang:monitor(process, S),
    S ! {get, R, self()},
    receive
        {msgs, R, Msgs} ->
            erlang:demonitor(M, [flush]),
            {ok, Msgs};
        {'DOWN', M, process, S, _} ->
            {error, down}
    after 5000 ->
            {error, timeout}
    end.

wait_for_fitting_exit(#fitting{pid=Pid}) ->
    M = erlang:monitor(process, Pid),
    receive
        {'DOWN', M, process, Pid, _} ->
            ok
    end.

fitting_is_alive(#fitting{pid=Pid}) ->
    lists:member(Pid, erlang:processes()).

start_vnode(#fitting{}=F) ->
    P = spawn_link(?MODULE, fake_vnode, [F, self()]),
    receive
        {up, P} -> P
    end.

%% ask for details, then enter message receive loop
fake_vnode(#fitting{}=F, Test) ->
    %% TODO: define Partition, instead of make_ref()
    D = case riak_pipe_fitting:get_details(F, make_ref()) of
            {ok, #fitting_details{}} -> true;
            gone -> false
        end,
    Test ! {up, self()},
    fake_vnode_loop(F, D, []).

fake_vnode_loop(F, D, Msgs) ->
    receive
        {'$gen_event',
         {riak_vnode_req_v1,_, _,Msg}} ->
            riak_pipe_fitting:worker_done(F),
            fake_vnode_loop(F, D, [Msg|Msgs]);
        {msgs, Ref, Pid} ->
            Pid ! {msgs, Ref, D, Msgs},
            fake_vnode_loop(F, D, [])
    end.

get_vnode_msgs(V) ->
    R = make_ref(),
    M = erlang:monitor(process, V),
    V ! {msgs, R, self()},
    receive
        {msgs, R, D, Msgs} ->
            erlang:demonitor(M, [flush]),
            {ok, D, Msgs};
        {'DOWN', M, process, V, _} ->
            {error, down}
    end.

-endif.
