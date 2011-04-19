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

-module(riak_pipe).

%% client API
-export([exec/2,
         wait_first_fitting/1,
         receive_result/1,
         collect_results/1]).
%% worker/fitting API
-export([result/3, eoi/1, log/3]).
%% examples
-export([example/0,
         example_start/0,
         example_send/1,
         example_receive/1,

         example_transform/0,
         example_reduce/0]).

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").

exec(Spec, Options) ->
    [ riak_pipe_fitting:validate_fitting(F) || F <- Spec ],
    {Sink, SinkOptions} = ensure_sink(Options),
    TraceOptions = correct_trace(SinkOptions),
    {ok, Pid} = riak_pipe_builder_sup:new_pipeline(Spec, TraceOptions),
    erlang:link(Pid),
    {ok, Pid, Sink}.

wait_first_fitting(Builder) ->
    riak_pipe_builder:get_first_fitting(Builder).

ensure_sink(Options) ->
    case lists:keyfind(sink, 1, Options) of
        {sink, #fitting{pid=Pid}=Sink} ->
            if is_pid(Pid) ->
                    PFSink = case Sink#fitting.partfun of
                                 undefined ->
                                     Sink#fitting{partfun=sink};
                                 _ ->
                                     Sink
                             end,
                    {PFSink, lists:keyreplace(sink, 1, Options, PFSink)};
               true ->
                    throw({invalid_sink, nopid})
            end;
        false ->
            Sink = #fitting{pid=self(), ref=make_ref(), partfun=sink},
            {Sink, [{sink, Sink}|Options]};
        _ ->
            throw({invalid_sink, not_fitting})
    end.

correct_trace(Options) ->
    case lists:keyfind(trace, 1, Options) of
        {trace, all} ->
            %% nothing to correct
            Options;
        {trace, List} when is_list(List) ->
            %% convert trace list to set for comparison later
            lists:keyreplace(trace, 1, Options,
                             {trace, sets:from_list(List)});
        {trace, Tuple} ->
            case sets:is_set(Tuple) of
                true ->
                    %% nothing to correct
                    Options;
                false ->
                    throw({invalid_trace, "not list or set"})
            end;
        false ->
            %% nothing to correct
            Options
    end.

result(From, #fitting{pid=Pid, ref=Ref}, Output) ->
    Pid ! #pipe_result{ref=Ref, from=From, result=Output}.

log(From, #fitting{pid=Pid, ref=Ref}, Msg) ->
    Pid ! #pipe_log{ref=Ref, from=From, msg=Msg}.

eoi(#fitting{pid=Pid, ref=Ref}) ->
    Pid ! #pipe_eoi{ref=Ref}.

receive_result(#fitting{ref=Ref}) ->
    receive
        #pipe_result{ref=Ref, from=From, result=Result} ->
            {result, {From, Result}};
        #pipe_log{ref=Ref, from=From, msg=Msg} ->
            {log, {From, Msg}};
        #pipe_eoi{ref=Ref} ->
            eoi
    after 5000 ->
            timeout
    end.

collect_results(#fitting{}=Fitting) ->
    collect_results(Fitting, [], []).

collect_results(Fitting, ResultAcc, LogAcc) ->
    case receive_result(Fitting) of
        {result, {From, Result}} ->
            collect_results(Fitting, [{From,Result}|ResultAcc], LogAcc);
        {log, {From, Result}} ->
            collect_results(Fitting, ResultAcc, [{From,Result}|LogAcc]);
        End ->
            %% result order shouldn't matter,
            %% but it's useful to have logging output in time order
            {End, ResultAcc, lists:reverse(LogAcc)}
    end.

example() ->
    {ok, Builder, Sink} = example_start(),
    {ok, Head} = wait_first_fitting(Builder),
    example_send(Head),
    example_receive(Sink).

example_start() ->
    riak_pipe:exec(
      [#fitting_spec{name=empty_pass,
                     module=riak_pipe_w_pass,
                     partfun=fun(_) -> 0 end}],
      [{log, sink},
       {trace, all}]).

example_send(Head) ->
    ok = riak_pipe_vnode:queue_work(Head, "hello"),
    riak_pipe_fitting:eoi(Head).

example_receive(Sink) ->
    collect_results(Sink).

example_transform() ->
    SumFun = fun(Input, Partition, FittingDetails) ->
                     riak_pipe_vnode_worker:send_output(
                       lists:sum(Input),
                       Partition,
                       FittingDetails),
                     ok
             end,
    {ok, Builder, Sink} =
        riak_pipe:exec(
          [#fitting_spec{name="sum transform",
                         module=riak_pipe_w_xform,
                         arg=SumFun,
                         partfun=fun(_) -> 0 end}],
          []),
    {ok, Head} = wait_first_fitting(Builder),
    ok = riak_pipe_vnode:queue_work(Head, lists:seq(1, 10)),
    riak_pipe_fitting:eoi(Head),
    example_receive(Sink).

example_reduce() ->
    SumFun = fun(_Key, Inputs, _Partition, _FittingDetails) ->
                     {ok, [lists:sum(Inputs)]}
             end,
    {ok, Builder, Sink} =
        riak_pipe:exec(
          [#fitting_spec{name="sum reduce",
                         module=riak_pipe_w_reduce,
                         arg=SumFun,
                         partfun=fun riak_pipe_w_reduce:partfun/1}],
          []),
    {ok, Head} = wait_first_fitting(Builder),
    [ok,ok,ok,ok,ok] =
        [ riak_pipe_vnode:queue_work(Head, {a, N})
          || N <- lists:seq(1, 5) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe_vnode:queue_work(Head, {b, N})
          || N <- lists:seq(11, 15) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe_vnode:queue_work(Head, {a, N})
          || N <- lists:seq(6, 10) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe_vnode:queue_work(Head, {b, N})
          || N <- lists:seq(16, 20) ],
    riak_pipe_fitting:eoi(Head),
    example_receive(Sink).
