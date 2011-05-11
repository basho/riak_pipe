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

%% @doc Basic interface to riak_pipe.
%%
%%      Clients of riak_pipe are most likely to be interested in
%%      {@link exec/2}, {@link wait_first_fitting/1},
%%      {@link receive_result/1}, and {@link collect_results/1}.
%%
%%      Basic client usage should go something like this:
%% ```
%% % define the pipeline
%% PipelineSpec = [#fitting_spec{name="passer"
%%                               module=riak_pipe_w_pass,
%%                               partfun=fun(_) -> 0 end}],
%%
%% % start things up
%% {ok, Builder, Sink} = riak_pipe:exec(PipelineSpec),
%% {ok, Head} = riak_pipe:wait_first_fitting(Builder),
%%
%% % send in some work
%% riak_pipe_vnode:queue_work(Head, "work item 1"),
%% riak_pipe_vnode:queue_work(Head, "work item 2"),
%% riak_pipe_vnode:queue_work(Head, "work item 3"),
%% riak_pipe_fitting:eoi(Head),
%%
%% % wait for results (alternatively use receive_result/1 repeatedly)
%% {ok, Results} = riak_pipe:collect_results().
%% '''
%%
%%      Many examples are included in the source code, and exported
%%      as functions named `example'*.
%%
%%      The functions {@link result/3}, {@link eoi/1}, and {@link
%%      log/3} are used by workers and fittings to deliver messages to
%%      the sink.
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

-export_type([fitting/0,
              fitting_spec/0,
              exec_opts/0]).
-type fitting() :: #fitting{}.
-type fitting_spec() :: #fitting_spec{}.
-type exec_opts() :: [exec_option()].
-type exec_option() :: {sink, fitting()}
                     | {trace, all | list() | set()}
                     | {log, sink | sasl}.

%% @doc Setup a pipeline.  This function starts up fitting/monitoring
%%      processes according the fitting specs given, returning a
%%      handle to the builder and the sink.  Calling code should then
%%      call `wait_first_fitting(Builder)' to get the lead fitting.
%%      Inputs may then be sent to vnodes, tagged with that lead
%%      fitting.
%%
%%      The pipeline is specified as an ordered list of
%%      `#fitting_spec{}' records.  Each record has the fields:
%%<dl><dt>
%%      `name'
%%</dt><dd>
%%      Any term. Will be used in logging, trace, and result messages.
%%</dd><dt>
%%      `module'
%%</dt><dd>
%%      Atom. The name of the module implementing the fitting.  This
%%      module must implement the `riak_pipe_vnode_worker' behavior.
%%</dd><dt>
%%      `arg'
%%</dt><dd>
%%      Any term. Will be available to the fitting-implementation
%%      module's initialization function.  This is a good way to
%%      parameterize general fittings.
%%</dd><dt>
%%     `partfun'
%%</dt><dd>
%%      A function of arity 1.  Used to determine which vnode should
%%      receive an input.  This function will be evaluated as
%%      `Fun(Input)'.  The result of that evaluation should be a
%%      partition index, which will be used to find the owning node in
%%      a `riak_core_ring'.
%%</dd></dl>
%%
%%      Defined elements of the `Options' list are:
%%<dl><dt>
%%      `{sink, Sink}'
%%</dt><dd>
%%      If no `sink' option is provided, one will be created, such
%%      that the calling process will receive all messages sent to the
%%      sink (all output, logging, and trace messages).  If specified,
%%      `Sink' should be a `#fitting{}' record, filled with the pid of
%%      the process prepared to receive these messages.
%%</dd><dt>
%%      `{trace, TraceMatches}'
%%</dt><dd>
%%      If no `trace' option is provided, tracing will be disabled for
%%      this pipeline.  If specified, `TraceMatches' should be either
%%      the atom `all', in which case all trace messages will be
%%      delivered, or a list of trace tags to match, in which case
%%      only messages with matching tags will be delivered.
%%</dd><dt>
%%      `{log, LogTarget}'
%%</dt><dd>
%%      If no `log' option is provided, logging will be disabled for
%%      this pipelien.  If specified, `LogTarget' should be either the
%%      atom `sink', in which case all log (and trace) messages will
%%      be delivered to the sink, or the atom `sasl', in which case
%%      all log (and trace) messages will be printed via
%%      `error_logger' to the SASL log.
%%</dd></dl>
%%
%%      Other values are allowed, but ignored, in `Options'.  The
%%      value of `Options' is provided to all fitting modules during
%%      initialization, so it can be a good vector for global
%%      configuration of general fittings.
-spec exec([fitting_spec()], exec_opts()) ->
         {ok, Builder::riak_pipe_builder:builder(), Sink::fitting()}.
exec(Spec, Options) ->
    [ riak_pipe_fitting:validate_fitting(F) || F <- Spec ],
    {Sink, SinkOptions} = ensure_sink(Options),
    TraceOptions = correct_trace(SinkOptions),
    {ok, Pid, Builder} = riak_pipe_builder_sup:new_pipeline(
                           Spec, TraceOptions),
    erlang:link(Pid),
    {ok, Builder, Sink}.

%% @doc Ask the pipeline builder for the handle of the first fitting.
%%      This handle may be used to queue work on vnodes.  The
%%      builder's handle was returned from the call to {@link exec/2}.
-spec wait_first_fitting(riak_pipe_builder:builder()) -> {ok, fitting()}.
wait_first_fitting(Builder) ->
    riak_pipe_builder:get_first_fitting(Builder).

%% @doc Ensure that the `{sink, Sink}' exec/2 option is defined
%%      correctly, or define a fresh one pointing to the current
%%      process if the option is absent.
-spec ensure_sink(exec_opts()) ->
         {fitting(), exec_opts()}.
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
                    RPFSink = case PFSink#fitting.ref of
                                  undefined ->
                                      PFSink#fitting{ref=make_ref()};
                                  _ ->
                                      PFSink
                              end,
                    {RPFSink, lists:keyreplace(sink, 1, Options, RPFSink)};
               true ->
                    throw({invalid_sink, nopid})
            end;
        false ->
            Sink = #fitting{pid=self(), ref=make_ref(), partfun=sink},
            {Sink, [{sink, Sink}|Options]};
        _ ->
            throw({invalid_sink, not_fitting})
    end.

%% @doc Validate the trace option.  Converts `{trace, list()}' to
%%      `{trace, set()}' for easier comparison later.
-spec correct_trace(exec_opts()) -> exec_opts().
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

%% @doc Send a result to the sink (used by worker processes).  The
%%      result is delivered as a `#pipe_result{}' record in the sink
%%      process's mailbox.
-spec result(term(), Sink::fitting(), term()) -> #pipe_result{}.
result(From, #fitting{pid=Pid, ref=Ref}, Output) ->
    Pid ! #pipe_result{ref=Ref, from=From, result=Output}.

%% @doc Send a log message to the sink (used by worker processes and
%%      fittings).  The message is delivered as a `#pipe_log{}' record
%%      in the sink process's mailbox.
-spec log(term(), Sink::fitting(), term()) -> #pipe_log{}.
log(From, #fitting{pid=Pid, ref=Ref}, Msg) ->
    Pid ! #pipe_log{ref=Ref, from=From, msg=Msg}.

%% @doc Send an end-of-inputs message to the sink (used by fittings).
%%      The message is delivered as a `#pipe_eoi{}' record in the sink
%%      process's mailbox.
-spec eoi(Sink::fitting()) -> #pipe_eoi{}.
eoi(#fitting{pid=Pid, ref=Ref}) ->
    Pid ! #pipe_eoi{ref=Ref}.

%% @doc Pull the next pipeline result out of the sink's mailbox.
%%      The `From' element of the `result' and `log' messages will
%%      be the name of the fitting that generated them, as specified
%%      in the `#fitting_spec{}' record used to start the pipeline.
%%      This function assumes that it is called in the sink's process.
%%      Passing the #fitting{} structure is only needed for reference
%%      to weed out misdirected messages from forgotten pipelines.
%%      A static timeout of five seconds is hard-coded (TODO).
-spec receive_result(Sink::fitting()) ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
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

%% @doc Receive all results and log messages, up to end-of-inputs
%%      (unless {@link receive_result} times out before the eoi
%%      arrives).
%%
%%      If end-of-inputs was the last message received, the first
%%      element of the returned tuple will be the atom `eoi'.  If the
%%      receive timed out before receiving end-of-inputs, the first
%%      element of the returned tuple will be the atom `timeout'.
%%
%%      The second element will be a list of all result messages
%%      received, while the third element will be a list of all log
%%      messages received.
%%
%%      This function assumes that it is called in the sink's process.
%%      Passing the #fitting{} structure is only needed for reference
%%      to weed out misdirected messages from forgotten pipelines.  A
%%      static inter-message timeout of five seconds is hard-coded
%%      (TODO).
-spec collect_results(Sink::fitting()) ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#fitting{}=Fitting) ->
    collect_results(Fitting, [], []).

%% @doc Internal implementation of collect_results/1.  Just calls
%%      receive_result/1, and accumulates lists of result and log
%%      messages.
-spec collect_results(Sink::fitting(),
                      ResultAcc::[{From::term(), Result::term()}],
                      LogAcc::[{From::term(), Result::term()}]) ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
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

%% @doc An example run of a simple pipe.  Uses {@link example_start/0},
%%      {@link example_send/0}, and {@link example_receive/0} to send
%%      nonsense through a pipe.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{empty_pass, "hello"}], _LogMessages}.
%% '''
-spec example() -> {eoi | timeout, list(), list()}.
example() ->
    {ok, Builder, Sink} = example_start(),
    {ok, Head} = wait_first_fitting(Builder),
    example_send(Head),
    example_receive(Sink).

%% @doc An example of starting a simple pipe.  Starts a pipe with one
%%      "pass" fitting.  Sink is pointed at the current process.
%%      Logging is pointed at the sink.  All tracing is enabled.
-spec example_start() ->
         {ok, Builder::riak_pipe_builder:builder(), Sink::fitting()}.
example_start() ->
    riak_pipe:exec(
      [#fitting_spec{name=empty_pass,
                     module=riak_pipe_w_pass,
                     partfun=fun(_) -> 0 end}],
      [{log, sink},
       {trace, all}]).

%% @doc An example of sending data into a pipeline.  Queues the string
%%      `"hello"' for the fitting provided, then signals end-of-inputs
%%      to that fitting.
-spec example_send(fitting()) -> ok.
example_send(Head) ->
    ok = riak_pipe_vnode:queue_work(Head, "hello"),
    riak_pipe_fitting:eoi(Head).

%% @doc An example of receiving data from a pipeline.  Reads all
%%      results sent to the given sink.
-spec example_receive(Sink::fitting()) ->
         {eoi | timeout, list(), list()}.
example_receive(Sink) ->
    collect_results(Sink).

%% @doc Another example pipeline use.  This one sets up a simple
%%      "transform" fitting, which expects lists of numbers as
%%      input, and produces the sum of that list as output.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{"sum transform", 55}], []}.
%% '''
-spec example_transform() -> {eoi | timeout, list(), list()}.
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

%% @doc Another example pipeline use.  This one sets up a simple
%%      "reduce" fitting, which expects tuples of the form
%%      `{Key::term(), Value::number()}', and produces results of the
%%      same form, where the output value is the sum of all of the
%%      input values for a given key.
%%
%%      If everything behaves correctly, this function should return
%% ```
%% {eoi, [{"sum reduce", {a, [55]}}, {"sum reduce", {b, [155]}}], []}.
%% '''
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
