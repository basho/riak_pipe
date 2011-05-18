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
%% {ok, Head, Sink} = riak_pipe:exec(PipelineSpec, []),
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
         receive_result/1,
         receive_result/2,
         collect_results/1,
         collect_results/2]).
%% worker/fitting API
-export([result/3, eoi/1, log/3]).
%% examples
-export([example/0,
         example_start/0,
         example_send/1,
         example_receive/1,

         example_transform/0,
         generic_transform/4,
         example_reduce/0,
         example_tick/3,
         example_tick/4]).
-ifdef(TEST).
-export([do_dep_apps/1, t/0]).
-endif.

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
%%      handle to the head (first) fitting and the sink.  Inputs may
%%      then be sent to vnodes, tagged with that head fitting.
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
         {ok, Head::fitting(), Sink::fitting()}.
exec(Spec, Options) ->
    [ riak_pipe_fitting:validate_fitting(F) || F <- Spec ],
    {Sink, SinkOptions} = ensure_sink(Options),
    TraceOptions = correct_trace(SinkOptions),
    {ok, Head} = riak_pipe_builder_sup:new_pipeline(Spec, TraceOptions),
    {ok, Head, Sink}.

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
receive_result(Fitting) ->
    receive_result(Fitting, 5000).

-spec receive_result(Sink::fitting(), Timeout::integer() | 'infinity') ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
receive_result(#fitting{ref=Ref}, Timeout) ->
    receive
        #pipe_result{ref=Ref, from=From, result=Result} ->
            {result, {From, Result}};
        #pipe_log{ref=Ref, from=From, msg=Msg} ->
            {log, {From, Msg}};
        #pipe_eoi{ref=Ref} ->
            eoi
    after Timeout ->
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
    collect_results(Fitting, [], [], 5000).

-spec collect_results(Sink::fitting(), Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#fitting{}=Fitting, Timeout) ->
    collect_results(Fitting, [], [], Timeout).

%% @doc Internal implementation of collect_results/1.  Just calls
%%      receive_result/1, and accumulates lists of result and log
%%      messages.
-spec collect_results(Sink::fitting(),
                      ResultAcc::[{From::term(), Result::term()}],
                      LogAcc::[{From::term(), Result::term()}],
                      Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(Fitting, ResultAcc, LogAcc, Timeout) ->
    case receive_result(Fitting, Timeout) of
        {result, {From, Result}} ->
            collect_results(Fitting, [{From,Result}|ResultAcc], LogAcc, Timeout);
        {log, {From, Result}} ->
            collect_results(Fitting, ResultAcc, [{From,Result}|LogAcc], Timeout);
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
    {ok, Head, Sink} = example_start(),
    example_send(Head),
    example_receive(Sink).

%% @doc An example of starting a simple pipe.  Starts a pipe with one
%%      "pass" fitting.  Sink is pointed at the current process.
%%      Logging is pointed at the sink.  All tracing is enabled.
-spec example_start() ->
         {ok, Head::fitting(), Sink::fitting()}.
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
    MsgFun = fun lists:sum/1,
    DriverFun = fun(Head, _Sink) ->
                        ok = riak_pipe_vnode:queue_work(Head, lists:seq(1, 10)),
                        riak_pipe_fitting:eoi(Head),
                        ok
                end,
    generic_transform(MsgFun, DriverFun, [], 1).

generic_transform(MsgFun, DriverFun, ExecOpts, NumFittings) ->
    MsgFunThenSendFun = fun(Input, Partition, FittingDetails) ->
                                riak_pipe_vnode_worker:send_output(
                                  MsgFun(Input),
                                  Partition,
                                  FittingDetails),
                                ok
                        end,
    {ok, Head, Sink} =
        riak_pipe:exec(
          lists:duplicate(NumFittings,
                          #fitting_spec{name="generic transform",
                                        module=riak_pipe_w_xform,
                                        arg=MsgFunThenSendFun,
                                        partfun=fun(_) -> 0 end}),
          ExecOpts),
    ok = DriverFun(Head, Sink),
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
    {ok, Head, Sink} =
        riak_pipe:exec(
          [#fitting_spec{name="sum reduce",
                         module=riak_pipe_w_reduce,
                         arg=SumFun,
                         partfun=fun riak_pipe_w_reduce:partfun/1}],
          []),
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

example_tick(TickLen, NumTicks, ChainLen) ->
    example_tick(TickLen, 1, NumTicks, ChainLen).

example_tick(TickLen, BatchSize, NumTicks, ChainLen) ->
    {ok, _Ring} = riak_core_ring_manager:get_my_ring(),
    Specs = [#fitting_spec{name=list_to_atom("tick_pass" ++ integer_to_list(F_num)),
                           module=riak_pipe_w_pass,
                           partfun = fun(_) -> 0 end}
             %% partfun=fun(X) ->
             %%     DocIdx = riak_core_util:chash_key({X,X}),
             %%     Nodes = riak_core_node_watcher:nodes(riak_pipe),
             %%     [{{Part,_},_}] = riak_core_apl:get_apl_ann(
             %%                        DocIdx, 1, _Ring, Nodes),
             %%     Part
             %% end}
             || F_num <- lists:seq(1, ChainLen)],
    {ok, Head, Sink} = riak_pipe:exec(Specs, [{log, sink},
                                              {trace, all}]),
    [begin
         [riak_pipe_vnode:queue_work(Head, {tick, {TickSeq, X}, now()}) ||
             X <- lists:seq(1, BatchSize)],
         if TickSeq /= NumTicks -> timer:sleep(TickLen);
            true                -> ok
         end
     end || TickSeq <- lists:seq(1, NumTicks)],
    riak_pipe_fitting:eoi(Head),
    example_receive(Sink).

-ifdef(TEST).

extract_trace_errors(Trace) ->
    [Ps || {_, {trace, [error], {error, Ps}}} <- Trace].

extract_fitting_died_errors(Trace) ->
    [X || {_, {trace, [error], {vnode, {fitting_died, _}} = X}} <- Trace].

extract_queued(Trace) ->
    [{Partition, X} ||
        {_, {trace, _, {vnode, {queued, Partition, X}}}} <- Trace].

kill_all_pipe_vnodes() ->
    [exit(VNode, kill) ||
        VNode <- riak_core_vnode_master:all_nodes(riak_pipe_vnode)].

t() ->
    eunit:test(?MODULE).

dep_apps() ->
    DelMe = "./EUnit-SASL.log",
    KillDamnFilterProc = fun() ->
                                 timer:sleep(5),
                                 catch exit(whereis(riak_sysmon_filter), kill),
                                 timer:sleep(5)
                         end,                                 
    [fun(start) ->
             _ = application:stop(sasl),
             _ = application:load(sasl),
             put(old_sasl_l, app_helper:get_env(sasl, sasl_error_logger)),
             ok = application:set_env(sasl, sasl_error_logger, {file, DelMe}),
             ok = application:start(sasl),
             error_logger:tty(false);
        (stop) ->
             ok = application:stop(sasl),
             ok = application:set_env(sasl, sasl_error_logger, erase(old_sasl_l));
        (fullstop) ->
             _ = application:stop(sasl)
     end,
     %% public_key and ssl are not needed here but started by others so
     %% stop them when we're done.
     crypto, public_key, ssl,
     fun(start) ->
             ok = application:start(riak_sysmon);
        (stop) ->
             ok = application:stop(riak_sysmon),
             KillDamnFilterProc();
        (fullstop) ->
             _ = application:stop(riak_sysmon),
             KillDamnFilterProc()
     end,
     webmachine,
     fun(start) ->
             _ = application:load(riak_core),
             put(old_hand_ip, app_helper:get_env(riak_core, handoff_ip)),
             put(old_hand_port, app_helper:get_env(riak_core, handoff_port)),
             ok = application:set_env(riak_core, handoff_ip, "0.0.0.0"),
             ok = application:set_env(riak_core, handoff_port, 9183),
             ok = application:start(riak_core);
        (stop) ->
             ok = application:stop(riak_core),
             ok = application:set_env(riak_core, handoff_ip, get(old_hand_ip)),
             ok = application:set_env(riak_core, handoff_port, get(old_hand_port));
        (fullstop) ->
             _ = application:stop(riak_core)
     end,
    riak_pipe].

do_dep_apps(fullstop) ->
    lists:map(fun(A) when is_atom(A) -> _ = application:stop(A);
                 (F)                 -> F(fullstop)
              end, lists:reverse(dep_apps()));
do_dep_apps(StartStop) ->
    Apps = if StartStop == start -> dep_apps();
              StartStop == stop  -> lists:reverse(dep_apps())
           end,
    lists:map(fun(A) when is_atom(A) -> ok = application:StartStop(A);
                 (F)                 -> F(StartStop)
              end, Apps).

prepare_runtime() ->
     fun() ->
             do_dep_apps(fullstop),
             timer:sleep(5),
             do_dep_apps(start),
             timer:sleep(5),
             [foo1, foo2]
     end.

teardown_runtime() ->
     fun(_PrepareThingie) ->
             do_dep_apps(stop),
             timer:sleep(5)
     end.    

basic_test_() ->
    AllLog = [{log, sink}, {trace, all}],
    OrderFun = fun(Head, _Sink) ->
                    ok = riak_pipe_vnode:queue_work(Head, 1),
                    riak_pipe_fitting:eoi(Head),
                    ok
           end,
    MultBy2 = fun(X) -> 2 * X end,
    {foreach,
     prepare_runtime(),
     teardown_runtime(),
     [
      fun(_) ->
              {"example()",
               fun() ->
                       {eoi, [{empty_pass, "hello"}], _Trc} =
                           ?MODULE:example()
               end}
      end,
      fun(_) ->
              {"example_transform()",
               fun() ->
                       {eoi, [{"generic transform", 55}], []} =
                           ?MODULE:example_transform()
               end}
      end,
      fun(_) ->
              {"example_reduce()",
               fun() ->
                       {eoi, Res, []} = ?MODULE:example_reduce(),
                       [{"sum reduce", {a, [55]}},
                        {"sum reduce", {b, [155]}}] = lists:sort(Res)
               end}
      end,
      fun(_) ->
              {"pipeline order",
               fun() ->
                       {eoi, Res, Trace} = 
                           generic_transform(MultBy2, OrderFun, 
                                             AllLog, 5),
                       [{_, 32}] = Res,
                       0 = length(extract_trace_errors(Trace)),
                       Qed = extract_queued(Trace),
                       %% NOTE: The msg to the sink doesn't appear in Trace
                       [1,2,4,8,16] = [X || {_, X} <- Qed]
               end}
      end
     ]
    }.

exception_test_() ->
    AllLog = [{log, sink}, {trace, [error]}],
    DecrOrCrashFun = fun(0) -> exit(blastoff);
                        (N) -> N - 1
                     end,
    XBad1 = fun(Head, _Sink) ->
                    ok = riak_pipe_vnode:queue_work(Head, [1, 2, 3]),
                    ok = riak_pipe_vnode:queue_work(Head, [4, 5, 6]),
                    ok = riak_pipe_vnode:queue_work(Head, [7, 8, bummer]),
                    ok = riak_pipe_vnode:queue_work(Head, [10, 11, 12]),
                    riak_pipe_fitting:eoi(Head),
                    ok
           end,
    XBad2 =
        fun(Head, _Sink) ->
                [ok = riak_pipe_vnode:queue_work(Head, N) ||
                    N <- lists:seq(0,2)],
                ok = riak_pipe_vnode:queue_work(Head, 500),
                exit({success_so_far, collect_results(_Sink, 100)})
        end,
    TailWorkerCrash =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, 100),
                timer:sleep(100),
                ok = riak_pipe_vnode:queue_work(Head, 1),
                riak_pipe_fitting:eoi(Head),
                ok
        end,
    VnodeCrash =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, 100),
                timer:sleep(100),
                kill_all_pipe_vnodes(),
                timer:sleep(100),
                riak_pipe_fitting:eoi(Head),
                ok
        end,
    HeadFittingCrash =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, [1, 2, 3]),
                (catch riak_pipe_fitting:crash(Head, fun() -> exit(die) end)),
                {error, worker_startup_failed} =
                    riak_pipe_vnode:queue_work(Head, [4, 5, 6]),
                %% Again, just for fun ... still fails
                {error, worker_startup_failed} =
                    riak_pipe_vnode:queue_work(Head, [4, 5, 6]),
                exit({success_so_far, collect_results(_Sink, 100)})
        end,
    MiddleFittingNormal =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, 20),
                timer:sleep(100),
                [{_, BuilderPid, _, _}] = riak_pipe_builder_sup:builder_pids(),
                {ok, FittingPids} = riak_pipe_builder:fitting_pids(BuilderPid),

                %% Aside: exercise riak_pipe_fitting:workers/1.
                %% There's a single worker on vnode 0, whee.
                {ok, [0]} = riak_pipe_fitting:workers(hd(FittingPids)),

                %% Aside: send fitting bogus messages
                gen_fsm:send_event(hd(FittingPids), bogus_message),
                {error, unknown} =
                    gen_fsm:sync_send_event(hd(FittingPids), bogus_message),
                gen_fsm:sync_send_all_state_event(hd(FittingPids), bogus_message),
                hd(FittingPids) ! bogus_message,

                %% Aside: send bogus done message
                MyRef = Head#fitting.ref,
                ok = gen_fsm:sync_send_event(hd(FittingPids),
                                             {done, MyRef, asdf}),

                Third = lists:nth(3, FittingPids),
                (catch riak_pipe_fitting:crash(Third, fun() -> exit(normal) end)),
                Fourth = lists:nth(4, FittingPids),
                (catch riak_pipe_fitting:crash(Fourth, fun() -> exit(normal) end)),
                %% This message will be lost in the middle of the pipe,
                %% but we'll be able to notice it via extract_trace_errors/1.
                ok = riak_pipe_vnode:queue_work(Head, 30),
                exit({success_so_far, collect_results(_Sink, 100)})
        end,
    MiddleFittingCrash =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, 20),
                timer:sleep(100),
                [{_, BuilderPid, _, _}] = riak_pipe_builder_sup:builder_pids(),
                {ok, FittingPids} = riak_pipe_builder:fitting_pids(BuilderPid),
                Third = lists:nth(3, FittingPids),
                (catch riak_pipe_fitting:crash(Third, fun() -> exit(diedie) end)),
                Fourth = lists:nth(4, FittingPids),
                (catch riak_pipe_fitting:crash(Fourth, fun() -> exit(diedie) end)),
                timer:sleep(100),   %% try to avoid racing w/pipeline shutdown
                {error,worker_startup_failed} =
                    riak_pipe_vnode:queue_work(Head, 30),
                riak_pipe_fitting:eoi(Head),
                exit({success_so_far, collect_results(_Sink, 100)})
        end,
    %% TODO: It isn't clear to me if TailFittingCrash is really any different
    %%       than MiddleFittingCrash.  I'm trying to exercise the patch in
    %%       commit cb0447f3c46 but am not having much luck.  {sigh}
    TailFittingCrash =
        fun(Head, _Sink) ->
                ok = riak_pipe_vnode:queue_work(Head, 20),
                timer:sleep(100),
                [{_, BuilderPid, _, _}] = riak_pipe_builder_sup:builder_pids(),
                {ok, FittingPids} = riak_pipe_builder:fitting_pids(BuilderPid),
                Last = lists:last(FittingPids),
                (catch riak_pipe_fitting:crash(Last, fun() -> exit(diedie) end)),
                timer:sleep(100),   %% try to avoid racing w/pipeline shutdown
                {error,worker_startup_failed} =
                    riak_pipe_vnode:queue_work(Head, 30),
                riak_pipe_fitting:eoi(Head),
                exit({success_so_far, collect_results(_Sink, 100)})
        end,
    {foreach,
     prepare_runtime(),
     teardown_runtime(),
     [fun(_) ->
              {"generic_transform(XBad1)",
               fun() ->
                       {eoi, Res, Trace} =
                           generic_transform(fun lists:sum/1, XBad1, AllLog, 1),
                       [{_, 6}, {_, 15}, {_, 33}] = lists:sort(Res),
                       [{_, {trace, [error], {error, Ps}}}] = Trace,
                       error = proplists:get_value(type, Ps),
                       badarith = proplists:get_value(error, Ps),
                       [7, 8, bummer] = proplists:get_value(input, Ps)
               end}
      end,
      fun(_) ->
              {"generic_transform(XBad2)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(DecrOrCrashFun,
                                                    XBad2,
                                                    AllLog, 3)),
                       [{_, 497}] = Res,
                       3 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(TailWorkerCrash)",
               fun() ->
                       {eoi, Res, Trace} = generic_transform(DecrOrCrashFun,
                                                             TailWorkerCrash,
                                                             AllLog, 2),
                       [{_, 98}] = Res,
                       1 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(VnodeCrash)",
               fun() ->
                       {eoi, Res, Trace} = generic_transform(DecrOrCrashFun,
                                                             VnodeCrash,
                                                             AllLog, 2),
                       [{_, 98}] = Res,
                       0 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(HeadFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun lists:sum/1,
                                                    HeadFittingCrash,
                                                    AllLog, 1)),
                       [{_, 6}] = Res,
                       1 = length(extract_fitting_died_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(MiddleFittingNormal)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(DecrOrCrashFun,
                                                    MiddleFittingNormal,
                                                    AllLog, 5)),
                       [{_, 15}] = Res,
                       2 = length(extract_fitting_died_errors(Trace)),
                       1 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(MiddleFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(DecrOrCrashFun,
                                                    MiddleFittingCrash,
                                                    AllLog, 5)),
                       [{_, 15}] = Res,
                       5 = length(extract_fitting_died_errors(Trace)),
                       0 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"generic_transform(TailFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(DecrOrCrashFun,
                                                    TailFittingCrash,
                                                    AllLog, 5)),
                       [{_, 15}] = Res,
                       5 = length(extract_fitting_died_errors(Trace)),
                       0 = length(extract_trace_errors(Trace))
               end}
      end
     ]
    }.

validate_test_() ->
    {foreach,
     prepare_runtime(),
     teardown_runtime(),
     [
      fun(_) ->
              {"very bad fitting",
               fun() ->
                       badarg = (catch
                                     riak_pipe_fitting:validate_fitting(x))
               end}
      end,
      fun(_) ->
              {"bad fitting module",
               fun() ->
                       badarg = (catch
                                     riak_pipe_fitting:validate_fitting(
                                       #fitting_spec{name=empty_pass,
                                                     module=does_not_exist,
                                                     partfun=fun(_) -> 0 end}))
               end}
      end,
      fun(_) ->
              {"bad fitting argument",
               fun() ->
                       badarg = (catch
                                     riak_pipe_fitting:validate_fitting(
                                       #fitting_spec{name=empty_pass,
                                                     module=riak_pipe_w_reduce,
                                                     arg=bogus_arg,
                                                     partfun=fun(_) -> 0 end}))
               end}
      end,
      fun(_) ->
              {"good partfun",
               fun() ->
                       ok = (catch
                                 riak_pipe_fitting:validate_fitting(
                                   #fitting_spec{name=empty_pass,
                                                 module=riak_pipe_w_pass,
                                                 partfun=follow}))
               end}
      end,
      fun(_) ->
              {"bad partfun",
               fun() ->
                       badarg = (catch
                                     riak_pipe_fitting:validate_fitting(
                                      #fitting_spec{name=empty_pass,
                                                    module=riak_pipe_w_pass,
                                                    partfun=fun(_,_) -> 0 end}))
               end}
      end,
      fun(_) ->
              {"format_name coverage",
               fun() ->
                       <<"foo">> = riak_pipe_fitting:format_name(<<"foo">>),
                       "foo" = riak_pipe_fitting:format_name("foo"),
                       "[foo]" = lists:flatten(
                                   riak_pipe_fitting:format_name([foo]))
               end}
      end
     ]}.

-endif.  % TEST
