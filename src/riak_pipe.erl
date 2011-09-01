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
%%                               module=riak_pipe_w_pass}],
%%
%% % start things up
%% {ok, Pipe} = riak_pipe:exec(PipelineSpec, []),
%%
%% % send in some work
%% riak_pipe:queue_work(Pipe, "work item 1"),
%% riak_pipe:queue_work(Pipe, "work item 2"),
%% riak_pipe:queue_work(Pipe, "work item 3"),
%% riak_pipe:eoi(Pipe),
%%
%% % wait for results (alternatively use receive_result/1 repeatedly)
%% {ok, Results} = riak_pipe:collect_results(Pipe).
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
         collect_results/2,
         queue_work/2,
         queue_work/3,
         eoi/1,
         destroy/1,
         status/1,
         active_pipelines/1
        ]).
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
-export([do_dep_apps/1, t/0, exec_prepare_runtime/1]).
-endif.

-include("riak_pipe.hrl").
-include("riak_pipe_debug.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([pipe/0,
              fitting/0,
              fitting_spec/0,
              exec_opts/0]).
-type pipe() :: #pipe{}.
-type fitting() :: #fitting{}.
-type fitting_spec() :: #fitting_spec{}.
-type exec_opts() :: [exec_option()].
-type exec_option() :: {sink, fitting()}
                     | {trace, all | list() | set()}
                     | {log, sink | sasl}.
-type stat() :: {atom(), term()}.

%% @doc Setup a pipeline.  This function starts up fitting/monitoring
%%      processes according the fitting specs given, returning a
%%      handle to the pipeline.  Inputs may then be sent to vnodes,
%%      tagged with that head fitting.
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
%%     `chashfun'
%%</dt><dd>
%%      A function of arity 1.  The consistent-hashing function used
%%      to determine which vnode should receive an input.  This
%%      function will be evaluated as `Fun(Input)'.  The result of
%%      that evaluation should be a binary, 160 bits in length, which
%%      will be used to choose the working vnode from a
%%      `riak_core_ring'.  (Very similar to the `chash_keyfun' bucket
%%      property used in `riak_kv'.)
%%
%%      The default is `fun chash:key_of/1', which will distribute
%%      inputs according to the SHA-1 hash of the input.
%%</dd><dt>
%%      `nval'
%%</dt><dd>
%%      Either a positive integer, or a function of arity 1 that
%%      returns a positive integer.  This field determines the maximum
%%      number of vnodes that might be asked to handle the input.  If
%%      a worker is unable to process an input on a given vnode, it
%%      can ask to have the input sent to a different vnode.  Up to
%%      `nval' vnodes will be tried in this manner.
%%
%%      If `nval' is an integer, that static number is used for all
%%      inputs.  If `nval' is a function, the function is evaluated as
%%      `Fun(Input)' (much like `chashfun'), and is expected to return
%%      a positive integer.
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
%%      this pipeline.  If specified, `LogTarget' should be one of the
%%      following atoms:
%%   <dl><dt>
%%      `sink'
%%   </dt><dd>
%%      all log (and trace) messages will be delivered to the sink
%%   </dd><dt>
%%      `sasl'
%%   </dt><dd>
%%      all log (and trace) messages will be printed via
%%      `error_logger' to the SASL log
%%   </dd><dt>
%%      `lager'
%%   </dt><dd>
%%      all log (and trace) messages will be printed to the Riak
%%      node's log via the lager utility
%%   </dd></dl>
%%</dd></dl>
%%
%%      Other values are allowed, but ignored, in `Options'.  The
%%      value of `Options' is provided to all fitting modules during
%%      initialization, so it can be a good vector for global
%%      configuration of general fittings.
-spec exec([fitting_spec()], exec_opts()) ->
         {ok, Pipe::pipe()}.
exec(Spec, Options) ->
    [ riak_pipe_fitting:validate_fitting(F) || F <- Spec ],
    CorrectOptions = correct_trace(ensure_sink(Options)),
    riak_pipe_builder_sup:new_pipeline(Spec, CorrectOptions).

%% @doc Ensure that the `{sink, Sink}' exec/2 option is defined
%%      correctly, or define a fresh one pointing to the current
%%      process if the option is absent.
-spec ensure_sink(exec_opts()) -> exec_opts().
ensure_sink(Options) ->
    case lists:keyfind(sink, 1, Options) of
        {sink, #fitting{pid=Pid}=Sink} ->
            if is_pid(Pid) ->
                    HFSink = case Sink#fitting.chashfun of
                                 undefined ->
                                     Sink#fitting{chashfun=sink};
                                 _ ->
                                     Sink
                             end,
                    RHFSink = case HFSink#fitting.ref of
                                  undefined ->
                                      HFSink#fitting{ref=make_ref()};
                                  _ ->
                                      HFSink
                              end,
                    lists:keyreplace(sink, 1, Options, {sink, RHFSink});
               true ->
                    throw({invalid_sink, nopid})
            end;
        false ->
            Sink = #fitting{pid=self(), ref=make_ref(), chashfun=sink},
            [{sink, Sink}|Options];
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

%% @doc Send an end-of-inputs message to the head of the pipe.
-spec eoi(Pipe::pipe()) -> ok.
eoi(#pipe{fittings=[{_,Head}|_]}) ->
    riak_pipe_fitting:eoi(Head).

%% @equiv queue_work(Pipe, Input, infinity)
queue_work(Pipe, Input) ->
    queue_work(Pipe, Input, infinity).

%% @doc Send inputs to the head of the pipe.
%%
%%      Note that `Timeout' options are only `infinity' and `noblock',
%%      not generic durations yet.
-spec queue_work(Pipe::pipe(),
                 Input::term(),
                 Timeout::riak_pipe_vnode:qtimeout())
         -> ok | {error, riak_pipe_vnode:qerror()}.
queue_work(#pipe{fittings=[{_,Head}|_]}, Input, Timeout)
  when Timeout =:= infinity; Timeout =:= noblock ->
    riak_pipe_vnode:queue_work(Head, Input, Timeout).

%% @doc Pull the next pipeline result out of the sink's mailbox.
%%      The `From' element of the `result' and `log' messages will
%%      be the name of the fitting that generated them, as specified
%%      in the `#fitting_spec{}' record used to start the pipeline.
%%      This function assumes that it is called in the sink's process.
%%      Passing the #fitting{} structure is only needed for reference
%%      to weed out misdirected messages from forgotten pipelines.
%%      A static timeout of five seconds is hard-coded (TODO).
-spec receive_result(pipe()) ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
receive_result(Pipe) ->
    receive_result(Pipe, 5000).

-spec receive_result(Pipe::pipe(), Timeout::integer() | 'infinity') ->
         {result, {From::term(), Result::term()}}
       | {log, {From::term(), Message::term()}}
       | eoi
       | timeout.
receive_result(#pipe{sink=#fitting{ref=Ref}}, Timeout) ->
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
-spec collect_results(pipe()) ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#pipe{}=Pipe) ->
    collect_results(Pipe, [], [], 5000).

-spec collect_results(pipe(), Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(#pipe{}=Pipe, Timeout) ->
    collect_results(Pipe, [], [], Timeout).

%% @doc Internal implementation of collect_results/1.  Just calls
%%      receive_result/1, and accumulates lists of result and log
%%      messages.
-spec collect_results(Pipe::pipe(),
                      ResultAcc::[{From::term(), Result::term()}],
                      LogAcc::[{From::term(), Result::term()}],
                      Timeout::integer() | 'infinity') ->
          {eoi | timeout,
           Results::[{From::term(), Result::term()}],
           Logs::[{From::term(), Message::term()}]}.
collect_results(Pipe, ResultAcc, LogAcc, Timeout) ->
    case receive_result(Pipe, Timeout) of
        {result, {From, Result}} ->
            collect_results(Pipe, [{From,Result}|ResultAcc], LogAcc, Timeout);
        {log, {From, Result}} ->
            collect_results(Pipe, ResultAcc, [{From,Result}|LogAcc], Timeout);
        End ->
            %% result order shouldn't matter,
            %% but it's useful to have logging output in time order
            {End, ResultAcc, lists:reverse(LogAcc)}
    end.

%% @doc Brutally kill a pipeline.  Use this when it is necessary to
%%      stop all parts of a pipeline as quickly as possible, instead
%%      of waiting for an `eoi' to propagate through.
-spec destroy(pipe()) -> ok.
destroy(#pipe{builder=Builder}) ->
    erlang:exit(Builder, kill),
    ok.

%% @doc Get all active pipelines hosted on `Node'.  Pass the atom
%%      `global' instead of a node name to get all pipelines hosted on
%%      all nodes.
%%
%%      The return value for a Node is a list of `#pipe{}' records.
%%      When `global' is used, the return value is a list of `{Node,
%%      [#pipe{}]}' tuples.
-spec active_pipelines(node() | global) ->
         [#pipe{}] | error | [{node(), [#pipe{}] | error}].
active_pipelines(global) ->
    [ {Node, active_pipelines(Node)}
      || Node <- riak_core_node_watcher:nodes(riak_pipe) ];
active_pipelines(Node) when is_atom(Node) ->
    case rpc:call(Node, riak_pipe_builder_sup, pipelines, []) of
        {badrpc, _}=Reason ->
            {error, Reason};
        Pipes ->
            Pipes
    end.

%% @doc Retrieve details about the status of the workers in this
%%      pipeline.  The form of the return is a list with one entry per
%%      fitting in the pipe.  Each fitting's entry is a 2-tuple of the
%%      form `{FittingName, WorkerDetails}', where `FittingName' is
%%      the name that was given to the fitting in the call to {@link
%%      riak_pipe:exec/2}, and `WorkerDetails' is a list with one
%%      entry per worker.  Each worker entry is a proplist, of the
%%      form returned by {@link riak_pipe_vnode:status/1}, with two
%%      properties added: `node', the node on which the worker is
%%      running, and `partition', the index of the vnode that the
%%      worker belongs to.
-spec status(pipe())
         -> [{FittingName::term(),[PartitionStatus::[stat()]]}].
status(#pipe{fittings=Fittings}) ->
    %% get all fittings and their lists of workers
    FittingWorkers = [ fitting_workers(F) || {_, F} <- Fittings ],

    %% convert to a mapping of workers -> fittings they're performing
    %% this allows us to make one status call per vnode,
    %% instead of one per vnode per fitting
    WorkerFittings = invert_dict(fun(_K, V) -> V end,
                                 fun(K, _V) -> K end,
                                 dict:from_list(FittingWorkers)),
    
    %% grab all worker-fitting statuses at once
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    WorkerStatuses = dict:map(worker_status(Ring), WorkerFittings),

    %% regroup statuses by fittings
    PidNames = [ {Name, Pid} || {Name, #fitting{pid=Pid}} <- Fittings ],
    FittingStatus = invert_dict(fun(_K, V) ->
                                        {fitting, Pid} =
                                            lists:keyfind(fitting, 1, V),
                                        {Name, Pid} =
                                            lists:keyfind(Pid, 2, PidNames),
                                        Name
                                end,
                                fun(_K, V) -> V end,
                                WorkerStatuses),
    dict:to_list(FittingStatus).

%% @doc Given a dict mapping keys to lists of values, "invert" the
%%      dict to map the values to their keys.
%%
%%      That is, for each `{K, [V]}' in `Dict', append `{KeyFun(K,V),
%%      ValFun(K,V)} to dict.
%%
%%      For example:
%% ```
%% D0 = dict:from_list([{a, [1, 2, 3]}, {b, [2, 3, 4]}]),
%% D1 = invert_dict(fun(_K, V) -> V end,
%%                  fun(K, _V) -> K end,
%%                  D0),
%% [{1, [a]}, {2, [a, b]}, {3, [a, b]}, {4, [b]}] = dict:to_list(D1).
%% '''
-spec invert_dict(fun((term(), term()) -> term()),
                  fun((term(), term()) -> term()),
                  dict()) -> dict().
invert_dict(KeyFun, ValFun, Dict) ->
    dict:fold(
      fun(Key, Vals, DAcc) ->
              lists:foldl(fun(V, LAcc) ->
                                  dict:append(KeyFun(Key, V),
                                              ValFun(Key, V),
                                              LAcc)
                          end,
                          DAcc,
                          Vals)
      end,
      dict:new(),
      Dict).

%% @doc Get the list of vnodes working for a fitting.
-spec fitting_workers(#fitting{})
         -> {#fitting{}, [riak_pipe_vnode:partition()]}.
fitting_workers(#fitting{pid=Pid}=Fitting) ->
    case riak_pipe_fitting:workers(Pid) of
        {ok, Workers} ->
            {Fitting, Workers};
        gone ->
            {Fitting, []}
    end.

%% @doc Produce a function that can be handed a partition number and a
%%      list of fittings, and will return the status for those
%%      fittings on that partition.  The closure over the Ring is a
%%      way to map over a list without having to fetch the ring
%%      repeatedly.
-spec worker_status(riak_core_ring:ring())
         -> fun( (riak_pipe_vnode:partition(), [#fitting{}])
                 -> [ [{atom(), term()}] ] ).
worker_status(Ring) ->
    fun(Partition, Fittings) ->
            %% lookup vnode pid
            Node = riak_core_ring:index_owner(Ring, Partition),
            {ok, Vnode} = rpc:call(Node,
                                   riak_core_vnode_master, get_vnode_pid,
                                   [Partition, riak_pipe_vnode]),
            
            %% get status of each worker
            {Partition, Workers} = riak_pipe_vnode:status(Vnode, Fittings),

            %% add 'node' and 'partition' to status
            [ [{node, Node}, {partition, Partition} | W]
              || W <- Workers ]
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
    {ok, Pipe} = example_start(),
    example_send(Pipe),
    example_receive(Pipe).

%% @doc An example of starting a simple pipe.  Starts a pipe with one
%%      "pass" fitting.  Sink is pointed at the current process.
%%      Logging is pointed at the sink.  All tracing is enabled.
-spec example_start() -> {ok, Pipe::pipe()}.
example_start() ->
    riak_pipe:exec(
      [#fitting_spec{name=empty_pass,
                     module=riak_pipe_w_pass,
                     chashfun=fun(_) -> <<0:160/integer>> end}],
      [{log, sink},
       {trace, all}]).

%% @doc An example of sending data into a pipeline.  Queues the string
%%      `"hello"' for the fitting provided, then signals end-of-inputs
%%      to that fitting.
-spec example_send(pipe()) -> ok.
example_send(Pipe) ->
    ok = riak_pipe:queue_work(Pipe, "hello"),
    riak_pipe:eoi(Pipe).

%% @doc An example of receiving data from a pipeline.  Reads all
%%      results sent to the given sink.
-spec example_receive(pipe()) ->
         {eoi | timeout, list(), list()}.
example_receive(Pipe) ->
    collect_results(Pipe).

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
    DriverFun = fun(Pipe) ->
                        ok = riak_pipe:queue_work(Pipe, lists:seq(1, 10)),
                        riak_pipe:eoi(Pipe),
                        ok
                end,
    generic_transform(MsgFun, DriverFun, [], 1).

generic_transform(MsgFun, DriverFun, ExecOpts, NumFittings) ->
    MsgFunThenSendFun = fun(Input, Partition, FittingDetails) ->
                                ok = riak_pipe_vnode_worker:send_output(
                                       MsgFun(Input),
                                       Partition,
                                       FittingDetails)
                        end,
    {ok, Pipe} =
        riak_pipe:exec(
          lists:duplicate(NumFittings,
                          #fitting_spec{name="generic transform",
                                        module=riak_pipe_w_xform,
                                        arg=MsgFunThenSendFun,
                                        chashfun=fun zero_part/1}),
          ExecOpts),
    ok = DriverFun(Pipe),
    example_receive(Pipe).

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
    {ok, Pipe} =
        riak_pipe:exec(
          [#fitting_spec{name="sum reduce",
                         module=riak_pipe_w_reduce,
                         arg=SumFun,
                         chashfun=fun riak_pipe_w_reduce:chashfun/1}],
          []),
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {a, N})
          || N <- lists:seq(1, 5) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {b, N})
          || N <- lists:seq(11, 15) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {a, N})
          || N <- lists:seq(6, 10) ],
    [ok,ok,ok,ok,ok] =
        [ riak_pipe:queue_work(Pipe, {b, N})
          || N <- lists:seq(16, 20) ],
    riak_pipe:eoi(Pipe),
    example_receive(Pipe).

example_tick(TickLen, NumTicks, ChainLen) ->
    example_tick(TickLen, 1, NumTicks, ChainLen).

example_tick(TickLen, BatchSize, NumTicks, ChainLen) ->
    Specs = [#fitting_spec{name=list_to_atom("tick_pass" ++ integer_to_list(F_num)),
                           module=riak_pipe_w_pass,
                           chashfun = fun zero_part/1}
             || F_num <- lists:seq(1, ChainLen)],
    {ok, Pipe} = riak_pipe:exec(Specs, [{log, sink},
                                        {trace, all}]),
    [begin
         [riak_pipe:queue_work(Pipe, {tick, {TickSeq, X}, now()}) ||
             X <- lists:seq(1, BatchSize)],
         if TickSeq /= NumTicks -> timer:sleep(TickLen);
            true                -> ok
         end
     end || TickSeq <- lists:seq(1, NumTicks)],
    riak_pipe:eoi(Pipe),
    example_receive(Pipe).

%% @doc dummy chashfun for tests and examples
%%      sends everything to partition 0
zero_part(_) ->
    riak_pipe_vnode:hash_for_partition(0).

-ifdef(TEST).

extract_trace_errors(Trace) ->
    [Ps || {_, {trace, _, {error, Ps}}} <- Trace].

%% extract_trace_vnode_failures(Trace) ->
%%     [Partition || {_, {trace, _, {vnode_failure, Partition}}} <- Trace].

extract_fitting_died_errors(Trace) ->
    [X || {_, {trace, _, {vnode, {fitting_died, _}} = X}} <- Trace].

extract_queued(Trace) ->
    [{Partition, X} ||
        {_, {trace, _, {vnode, {queued, Partition, X}}}} <- Trace].

extract_queue_full(Trace) ->
    [Partition ||
        {_, {trace, _, {vnode, {queue_full, Partition, _}}}} <- Trace].

extract_unblocking(Trace) ->
    [Partition ||
        {_, {trace, _, {vnode, {unblocking, Partition}}}} <- Trace].

extract_restart_fail(Trace) ->
    [Partition ||
        {_, {trace, _, {vnode, {restart_fail, Partition, _}}}} <- Trace].

extract_restart(Trace) ->
    [Partition ||
        {_, {trace, _, {vnode, {restart, Partition}}}} <- Trace].

extract_vnode_done(Trace) ->
    [{Partition, Stats} ||
        {_, {trace, _, {vnode, {done, Partition, Stats}}}} <- Trace].

kill_all_pipe_vnodes() ->
    [exit(VNode, kill) ||
        VNode <- riak_core_vnode_master:all_nodes(riak_pipe_vnode)].

t() ->
    eunit:test(?MODULE).

save_and_set_env({App, Key, NewVal}) ->
    put({?MODULE, old, App, Key}, app_helper:get_env(App, Key)),
    ok = application:set_env(App, Key, NewVal).

restore_env({App, Key, _NewVal}) ->
    ok = application:set_env(App, Key, erase({?MODULE, old, App, Key})).

dep_apps() ->
    DelMe = "./EUnit-SASL.log",
    PortOffset = case atom_to_list(node()) of
                     [$s,$l,$a,$v,$e,SlaveNum|_] -> SlaveNum - $0 + 1;
                     _                           -> 0
                 end,
    KillDamnFilterProc = fun() ->
                                 timer:sleep(5),
                                 catch exit(whereis(riak_sysmon_filter), kill),
                                 timer:sleep(5)
                         end,                                 
    RingDir = "./data.ring-dir",
    CoreEnvVars = [{riak_core, handoff_ip, "0.0.0.0"},
                   {riak_core, handoff_port, 9183+PortOffset},
                   {riak_core, vnode_inactivity_timeout, 150},
                   {riak_core, ring_creation_size, 8},
                   {riak_core, ring_state_dir, RingDir}],
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
             [save_and_set_env(Triple) || Triple <- CoreEnvVars],
             ok = application:start(riak_core);
        (stop) ->
             ok = application:stop(riak_core),
             [restore_env(Triple) || Triple <- CoreEnvVars],
             %% Multi-node tests (e.g. using slaves) can leave behind
             %% ring state & other stuff that interferes with
             %% unit test repeatability.
             os:cmd("rm -rf " ++ RingDir);
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

default_nodename() ->
    %% 'localhost' hostname must be used 100% consistently everywhere
    'riak_pipe_testing@localhost'.              % short name!

slave0_nodename() ->
    'slave0_pipe@localhost'.

start_slave0() ->
   start_slv('localhost', 'slave0_pipe', slave0_nodename(),
             "-config ../priv/app.slave0.config").

%% start_slave1() ->
%%    start_slv('localhost', 'slave1_pipe', 'slave1_pipe@localhost',
%%              "-config ../priv/app.slave1.config").

%% Painful lessons learned while trying to use 'slave' and EUnit:
%%
%% * You need to set up the code path for your slave node.  Nevermind that
%%   you're sharing the file server with the master node, because the code
%%   server doesn't appear to be shared.
%% * You _are_ sharing the file server, so trying to use
%%   file:set_cwd/1 is a really bad idea, even for a noble reason
%%   (like trying to get the slave to use a different SASL logging
%%   file).  So don't ever bother trying to do it.

start_slv(RHS, LHS, NodeName, Extra) ->
    Res = slave:start_link(RHS, LHS, Extra),
    [rpc:call(NodeName, code, add_pathz, [Dir]) || Dir <- code:get_path()],
    rpc:call(NodeName, code, add_patha, ["."]), % eunit-compiled stuff first
    %% NO! rpc:call(NodeName, file, make_dir, [atom_to_list(NodeName)]),
    %% NO! ok = rpc:call(NodeName, file, set_cwd, [atom_to_list(NodeName)]),
    rpc:call(NodeName, ?MODULE, exec_prepare_runtime, [NodeName]),
    Res.

prepare_runtime() ->
    prepare_runtime(default_nodename()).

prepare_runtime(NodeName) ->
     fun() ->
             do_dep_apps(fullstop),
             timer:sleep(5),
             %% Must start epmd/net_kernel before starting apps
             [] = os:cmd("epmd -daemon"),
             net_kernel:start([NodeName, shortnames]), 
             do_dep_apps(start),
             timer:sleep(5),
             [foo1, foo2]
     end.

exec_prepare_runtime(NodeName) ->
    (prepare_runtime(NodeName))().

teardown_runtime() ->
     fun(_PrepareThingie) ->
             do_dep_apps(stop),
             net_kernel:stop(),
             timer:sleep(5),
             dbg:stop_clear()
     end.    

basic_test_() ->
    AllLog = [{log, sink}, {trace, all}],
    OrderFun = fun(Pipe) ->
                    ok = riak_pipe:queue_work(Pipe, 1),
                    riak_pipe:eoi(Pipe),
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
      end,
      fun(_) ->
              {"trace filtering",
               fun() ->
                       {eoi, _Res, Trace1} = 
                           generic_transform(MultBy2, OrderFun, 
                                             [{log,sink}, {trace, [eoi]}], 5),
                       {eoi, _Res, Trace2} = 
                           generic_transform(MultBy2, OrderFun, 
                                             [{log,sink}, {trace, all}], 5),
                       %% Looking too deeply into the format of the trace
                       %% messages, since they haven't gelled yet, is madness.
                       length(Trace1) < length(Trace2)
               end}
      end,
      fun(_) ->
              {"recursive countdown test #1",
               fun() ->
                       Spec = [#fitting_spec{name=counter,
                                             module=riak_pipe_w_rec_countdown}],
                       {ok, Pipe} = riak_pipe:exec(Spec, []),
                       riak_pipe:queue_work(Pipe, 3),
                       riak_pipe:eoi(Pipe),
                       {eoi, Res, []} = riak_pipe:collect_results(Pipe),
                       [{counter,0},{counter,1},{counter,2},{counter,3}] = Res
               end}
      end,
      fun(_) ->
              {"recursive countdown test #2 (nondeterministic)",
               fun() ->
                       Spec = [#fitting_spec{name=counter,
                                             module=riak_pipe_w_rec_countdown,
                                             arg=testeoi}],
                       Options = [{trace,[restart]},{log,sink}],
                       {ok, Pipe} = riak_pipe:exec(Spec, Options),
                       riak_pipe:queue_work(Pipe, 3),
                       riak_pipe:eoi(Pipe),
                       {eoi, Res, Trc} = riak_pipe:collect_results(Pipe),
                       [{counter,0},{counter,0},{counter,0},
                        {counter,1},{counter,2},{counter,3}] = Res,
                       try
                           [{counter,
                             {trace,[restart],{vnode,{restart,_}}}}] = Trc
                       catch error:{badmatch,[]} ->
                               %% If `Trace' is empty, the done/eoi race was
                               %% not triggered.  So, in theory, we should
                               %% re-run the test.  Except that EUnit tests
                               %% aren't good at checking non-deterministic
                               %% tests.
                               ?debugMsg("Warning: recursive countdown test"
                                         " #2 did not trigger the done/eoi"
                                         " race it tests."
                                         " Consider re-running."),
                               ok
                       end
               end}
      end
     ]
    }.

%% decr_or_crash() is usually used with the riak_pipe_w_xform
%% fitting: at each fitting, the xform worker will decrement the
%% count by one.  If the count ever gets to zero, then the worker
%% will exit.  So, we want a worker to crash if the pipeline
%% length > input # at head of pipeline.

decr_or_crash(0) ->
    exit(blastoff);
decr_or_crash(N) ->
    N - 1.

xform_or_crash(Input, Partition, FittingDetails) ->
    ok = riak_pipe_vnode_worker:send_output(
           decr_or_crash(Input),
           Partition,
           FittingDetails).

exception_test_() ->
    AllLog = [{log, sink}, {trace, all}],
    ErrLog = [{log, sink}, {trace, [error]}],

    Sleep1Fun = fun(X) ->
                        timer:sleep(1),
                        X
                end,
    Send_one_100 =
        fun(Pipe) ->
                ok = riak_pipe:queue_work(Pipe, 100),
                %% Sleep so that we don't have workers being shutdown before
                %% the above work item gets to the end of the pipe.
                timer:sleep(100),
                riak_pipe:eoi(Pipe)
        end,
    Send_onehundred_100 =
        fun(Pipe) ->
                [ok = riak_pipe:queue_work(Pipe, 100) ||
                    _ <- lists:seq(1,100)],
                %% Sleep so that we don't have workers being shutdown before
                %% the above work item gets to the end of the pipe.
                timer:sleep(100),
                riak_pipe:eoi(Pipe)
        end,
    DieFun = fun() ->
                     exit(diedie)
             end,
    {foreach,
     prepare_runtime(),
     teardown_runtime(),
     [fun(_) ->
              XBad1 =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, [1, 2, 3]),
                          ok = riak_pipe:queue_work(Pipe, [4, 5, 6]),
                          ok = riak_pipe:queue_work(Pipe, [7, 8, bummer]),
                          ok = riak_pipe:queue_work(Pipe, [10, 11, 12]),
                          riak_pipe:eoi(Pipe),
                          ok
                  end,
              {"generic_transform(XBad1)",
               fun() ->
                       {eoi, Res, Trace} =
                           generic_transform(fun lists:sum/1, XBad1, ErrLog, 1),
                       [{_, 6}, {_, 15}, {_, 33}] = lists:sort(Res),
                       [{_, {trace, [error], {error, Ps}}}] = Trace,
                       error = proplists:get_value(type, Ps),
                       badarith = proplists:get_value(error, Ps),
                       [7, 8, bummer] = proplists:get_value(input, Ps)
               end}
      end,
      fun(_) ->
              XBad2 =
                  fun(Pipe) ->
                          [ok = riak_pipe:queue_work(Pipe, N) ||
                              N <- lists:seq(0,2)],
                          ok = riak_pipe:queue_work(Pipe, 500),
                          exit({success_so_far, collect_results(Pipe, 100)})
                  end,
              {"generic_transform(XBad2)",
               fun() ->
                       %% 3 fittings, send 0, 1, 2, 500
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun decr_or_crash/1,
                                                    XBad2,
                                                    ErrLog, 3)),
                       [{_, 497}] = Res,
                       3 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              TailWorkerCrash =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, 100),
                          timer:sleep(100),
                          ok = riak_pipe:queue_work(Pipe, 1),
                          riak_pipe:eoi(Pipe),
                          ok
                  end,
              {"generic_transform(TailWorkerCrash)",
               fun() ->
                       {eoi, Res, Trace} = generic_transform(fun decr_or_crash/1,
                                                             TailWorkerCrash,
                                                             ErrLog, 2),
                       [{_, 98}] = Res,
                       1 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              VnodeCrash =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, 100),
                          timer:sleep(100),
                          kill_all_pipe_vnodes(),
                          timer:sleep(100),
                          riak_pipe:eoi(Pipe),
                          ok
                  end,
              {"generic_transform(VnodeCrash)",
               fun() ->
                       {eoi, Res, Trace} = generic_transform(fun decr_or_crash/1,
                                                             VnodeCrash,
                                                             ErrLog, 2),
                       [{_, 98}] = Res,
                       0 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              HeadFittingCrash =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, [1, 2, 3]),
                          [{_, Head}|_] = Pipe#pipe.fittings,
                          (catch riak_pipe_fitting:crash(Head, DieFun)),
                          {error, [worker_startup_failed]} =
                              riak_pipe:queue_work(Pipe, [4, 5, 6]),
                          %% Again, just for fun ... still fails
                          {error, [worker_startup_failed]} =
                              riak_pipe:queue_work(Pipe, [4, 5, 6]),
                          exit({success_so_far, collect_results(Pipe, 100)})
                  end,
              {"generic_transform(HeadFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun lists:sum/1,
                                                    HeadFittingCrash,
                                                    ErrLog, 1)),
                       [{_, 6}] = Res,
                       1 = length(extract_fitting_died_errors(Trace))
               end}
      end,
      fun(_) ->
              MiddleFittingNormal =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, 20),
                          timer:sleep(100),
                          FittingPids = [ P || {_, #fitting{pid=P}}
                                                   <- Pipe#pipe.fittings],

                          %% Aside: exercise riak_pipe_fitting:workers/1.
                          %% There's a single worker on vnode 0, whee.
                          {ok,[0]} = riak_pipe_fitting:workers(hd(FittingPids)),

                          %% Aside: send fitting bogus messages
                          gen_fsm:send_event(hd(FittingPids), bogus_message),
                          {error, unknown} =
                              gen_fsm:sync_send_event(hd(FittingPids),
                                                      bogus_message),
                          gen_fsm:sync_send_all_state_event(hd(FittingPids),
                                                            bogus_message),
                          hd(FittingPids) ! bogus_message,

                          %% Aside: send bogus done message
                          [{_, Head}|_] = Pipe#pipe.fittings,
                          MyRef = Head#fitting.ref,
                          ok = gen_fsm:sync_send_event(hd(FittingPids),
                                                       {done, MyRef, asdf}),

                          Third = lists:nth(3, FittingPids),
                          (catch riak_pipe_fitting:crash(Third, fun() ->
                                                                   exit(normal)
                                                                end)),
                          Fourth = lists:nth(4, FittingPids),
                          (catch riak_pipe_fitting:crash(Fourth, fun() ->
                                                                   exit(normal)
                                                                 end)),
                          %% This message will be lost in the middle of the
                          %% pipe, but we'll be able to notice it via
                          %% extract_trace_errors/1.
                          ok = riak_pipe:queue_work(Pipe, 30),
                          exit({success_so_far, collect_results(Pipe, 100)})
                  end,
              {"generic_transform(MiddleFittingNormal)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun decr_or_crash/1,
                                                    MiddleFittingNormal,
                                                    ErrLog, 5)),
                       [{_, 15}] = Res,
                       2 = length(extract_fitting_died_errors(Trace)),
                       1 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              MiddleFittingCrash =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, 20),
                          timer:sleep(100),
                          FittingPids = [ P || {_, #fitting{pid=P}}
                                                   <- Pipe#pipe.fittings ],
                          Third = lists:nth(3, FittingPids),
                          (catch riak_pipe_fitting:crash(Third, DieFun)),
                          Fourth = lists:nth(4, FittingPids),
                          (catch riak_pipe_fitting:crash(Fourth, DieFun)),
                          %% try to avoid racing w/pipeline shutdown
                          timer:sleep(100),
                          {error,[worker_startup_failed]} =
                              riak_pipe:queue_work(Pipe, 30),
                          riak_pipe:eoi(Pipe),
                          exit({success_so_far, collect_results(Pipe, 100)})
                  end,
              {"generic_transform(MiddleFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun decr_or_crash/1,
                                                    MiddleFittingCrash,
                                                    ErrLog, 5)),
                       [{_, 15}] = Res,
                       5 = length(extract_fitting_died_errors(Trace)),
                       0 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              %% TODO: It isn't clear to me if TailFittingCrash is
              %% really any different than MiddleFittingCrash.  I'm
              %% trying to exercise the patch in commit cb0447f3c46
              %% but am not having much luck.  {sigh}
              TailFittingCrash =
                  fun(Pipe) ->
                          ok = riak_pipe:queue_work(Pipe, 20),
                          timer:sleep(100),
                          FittingPids = [ P || {_, #fitting{pid=P}}
                                                   <- Pipe#pipe.fittings ],
                          Last = lists:last(FittingPids),
                          (catch riak_pipe_fitting:crash(Last, DieFun)),
                          %% try to avoid racing w/pipeline shutdown
                          timer:sleep(100),
                          {error,[worker_startup_failed]} =
                              riak_pipe:queue_work(Pipe, 30),
                          riak_pipe:eoi(Pipe),
                          exit({success_so_far,
                                collect_results(Pipe, 100)})
                  end,
              {"generic_transform(TailFittingCrash)",
               fun() ->
                       {'EXIT', {success_so_far, {timeout, Res, Trace}}} =
                           (catch generic_transform(fun decr_or_crash/1,
                                                    TailFittingCrash,
                                                    ErrLog, 5)),
                       [{_, 15}] = Res,
                       5 = length(extract_fitting_died_errors(Trace)),
                       0 = length(extract_trace_errors(Trace))
               end}
      end,
      fun(_) ->
              {"worker init crash 1",
               fun() ->
                       {ok, Pipe} =
                           riak_pipe:exec(
                             [#fitting_spec{name="init crash",
                                            module=riak_pipe_w_crash,
                                            arg=init_exit,
                                            chashfun=follow}], ErrLog),
                       {error, [worker_startup_failed]} =
                           riak_pipe:queue_work(Pipe, x),
                       riak_pipe:eoi(Pipe),
                       {eoi, [], []} = collect_results(Pipe, 500)
               end}
      end,
      fun(_) ->
              {"worker init crash 2 (only init arg differs from #1 above)",
               fun() ->
                       {ok, Pipe} =
                           riak_pipe:exec(
                             [#fitting_spec{name="init crash",
                                            module=riak_pipe_w_crash,
                                            arg=init_badreturn,
                                            chashfun=follow}], ErrLog),
                       {error, [worker_startup_failed]} =
                           riak_pipe:queue_work(Pipe, x),
                       riak_pipe:eoi(Pipe),
                       {eoi, [], []} = collect_results(Pipe, 500)
               end}
      end,
      fun(_) ->
              {"worker limit for one pipe",
               fun() ->
                       PipeLen = 90,
                       {eoi, Res, Trace} = generic_transform(fun decr_or_crash/1,
                                                             Send_one_100,
                                                             AllLog, PipeLen),
                       [] = Res,
                       Started = [x || {_, {trace, _,
                                            {fitting, init_started}}} <- Trace],
                       PipeLen = length(Started),
                       [Ps] = extract_trace_errors(Trace), % exactly one error!
                       %% io:format(user, "Ps = ~p\n", [Ps]),
                       {badmatch,{error,[worker_limit_reached]}} =
                           proplists:get_value(error, Ps)
               end}
      end,
      fun(_) ->
              {"worker limit for multiple pipes",
               fun() ->
                       PipeLen = 90,
                       Spec = lists:duplicate(
                                PipeLen,
                                #fitting_spec{name="worker limit mult pipes",
                                              module=riak_pipe_w_xform,
                                              arg=fun xform_or_crash/3,
                                              chashfun=fun zero_part/1}),
                       {ok, Pipe1} =
                           riak_pipe:exec(Spec, AllLog),
                       {ok, Pipe2} =
                           riak_pipe:exec(Spec, AllLog),
                       ok = riak_pipe:queue_work(Pipe1, 100),
                       timer:sleep(100),
                       %% At worker limit, can't even start 1st worker @ Head2
                       {error, [worker_limit_reached]} =
                           riak_pipe:queue_work(Pipe2, 100),
                       {timeout, [], Trace1} = collect_results(Pipe1, 500),
                       {timeout, [], Trace2} = collect_results(Pipe2, 500),
                       [_] = extract_trace_errors(Trace1), % exactly one error!
                       [] = extract_queued(Trace2)
               end}
      end,
      fun(_) ->
              {"under per-vnode worker limit for 1 pipe + many vnodes",
               fun() ->
                       %% 20 * Ring size > worker limit, if indeed the worker
                       %% limit were enforced per node instead of per vnode.
                       PipeLen = 20,
                       Spec = lists:duplicate(
                                PipeLen,
                                #fitting_spec{name="foo",
                                              module=riak_pipe_w_xform,
                                              arg=fun xform_or_crash/3}),
                       {ok, Pipe1} =
                           riak_pipe:exec(Spec, AllLog),
                       [ok = riak_pipe:queue_work(Pipe1, X) ||
                           X <- lists:seq(101, 200)],
                       riak_pipe:eoi(Pipe1),
                       {eoi, Res, Trace1} = collect_results(Pipe1, 500),
                       100 = length(Res),
                       [] = extract_trace_errors(Trace1)
               end}
      end,
      fun(_) ->
              {"Per worker queue limit enforcement",
               fun() ->
                       {eoi, Res, Trace} =
                           generic_transform(Sleep1Fun,
                                             Send_onehundred_100,
                                             AllLog, 1),
                       100 = length(Res),
                       Full = length(extract_queue_full(Trace)),
                       NoLongerFull = length(extract_unblocking(Trace)),
                       Full = NoLongerFull
               end}
      end,
      fun(_) ->
              {"worker restart failure, input forwarding",
               fun() ->
                       %% make a worker fail, and then also fail to restart,
                       %% and check that the input that killed it generates
                       %% a processing error, while the inputs that were
                       %% queued for it get sent to another vnode
                       Spec = [#fitting_spec{name=restarter,
                                             module=riak_pipe_w_crash,
                                             arg=init_restartfail,
                                             %% use nval=2 to get some failover
                                             nval=2}],
                       Opts = [{log, sink},
                               {trace,[error,restart,restart_fail,queue]}],
                       {ok, Pipe} = riak_pipe:exec(Spec, Opts),

                       Inputs1 = lists:seq(0,127),
                       Inputs2 = lists:seq(128,255),
                       Inputs3 = lists:seq(256,383),

                       %% sleep, send more inputs

                       %% this should make one of the
                       %% riak_pipe_w_crash workers die with
                       %% unprocessed items in its queue, and then
                       %% also deliver a few more inputs to that
                       %% worker, which will be immediately redirected
                       %% to an alternate vnode

                       %% send many inputs, send crash, send more inputs
                       [riak_pipe:queue_work(Pipe, N) || N <- Inputs1],
                       riak_pipe:queue_work(Pipe, init_restartfail),
                       [riak_pipe:queue_work(Pipe, N) || N <- Inputs2],
                       %% one worker should now have both the crashing input
                       %% and a valid input following it waiting in its queue
                       %% - the test is whether or not that valid input
                       %%   following the crash gets redirected correctly
                       
                       %% wait for the worker to crash,
                       %% then send more input at it
                       %% - the test is whether the new inputs are
                       %%   redirected correctly
                       timer:sleep(2000),
                       [riak_pipe:queue_work(Pipe, N) || N <- Inputs3],

                       %% flush the pipe
                       riak_pipe:eoi(Pipe),
                       {eoi, Results, Trace} = riak_pipe:collect_results(Pipe),

                       %% all results should have completed correctly
                       ?assertEqual(length(Inputs1++Inputs2++Inputs3),
                                    length(Results)),

                       %% There should be one trace errors:
                       %% - the processing error (worker crash)
                       Errors = extract_trace_errors(Trace),
                       ?assertEqual(1, length(Errors)),
                       ?assert(is_list(hd(Errors))),
                       ?assertMatch(init_restartfail,
                                    proplists:get_value(input, hd(Errors))),
                       Restarter = proplists:get_value(partition,
                                                       hd(Errors)),
                       %% ... and also one message about the worker
                       %% restart failure
                       ?assertMatch([Restarter],
                                    extract_restart_fail(Trace)),

                       Queued = extract_queued(Trace),

                       %% find out who caught the restartfail
                       Restarted = [ P || {P, init_restartfail}
                                              <- Queued ],
                       ?assertMatch([Restarter], Restarted),

                       %% what input arrived after the crashing input,
                       %% but before the crash?
                       {_PreCrashIn, PostCrashIn0} =
                           lists:splitwith(
                             fun is_integer/1,
                             [ I || {P,I} <- Queued,
                                    P == Restarter]),
                       %% drop actual crash input
                       PostCrashIn = tl(PostCrashIn0),
                       %% make sure the input was actually enqueued
                       %% before the crash (otherwise test was bogus)
                       ?assert(length(PostCrashIn) > 0),

                       %% so where did the post-crach inputs end up?
                       ReQueued = lists:map(
                                    fun(I) ->
                                            Re = [ P || {P,X} <- Queued,
                                                        X == I,
                                                        P /= Restarter ],
                                            ?assertMatch([_Part], Re),
                                            hd(Re)
                                    end,
                                    PostCrashIn),
                       ?assertMatch([_Requeuer], lists:usort(ReQueued)),
                       [Requeuer|_] = ReQueued,

                       %% finally, did the inputs destined for the
                       %% crashed worker that were sent *after* the
                       %% worker crashed, also get forwarded to the
                       %% correct location?
                       Destined = lists:filter(
                                    fun(I) ->
                                            [{P,_}] = riak_core_apl:get_apl(
                                                        chash:key_of(I),
                                                        1, riak_pipe),
                                            P == Restarter
                                    end,
                                    Inputs3),
                       Forwarded = lists:map(
                                     fun(I) ->
                                             [Part] = [P
                                                       || {P,X} <- Queued,
                                                          X == I],
                                             Part
                                     end,
                                     Destined),
                       ?assertMatch([_Forward], lists:usort(Forwarded)),
                       [Forward|_] = Forwarded,

                       %% consistent hash means this should be the same
                       ?assertEqual(Requeuer, Forward)
               end}
      end,
      fun(_) ->
              {"Vnode Death",
               fun() ->
                       {ok, Pipe} =
                           riak_pipe:exec(
                             [#fitting_spec{name=vnode_death_test,
                                            module=riak_pipe_w_crash}],
                             []),
                       %% this should kill vnode such that it never
                       %% responds to the enqueue request
                       riak_pipe:queue_work(Pipe, vnode_killer),
                       riak_pipe:eoi(Pipe),
                       {eoi, Res, []} = riak_pipe:collect_results(Pipe),
                       ?assertEqual([], Res)
               end}
      end,
      fun(_) ->
              %% workers restarted because of recursive inputs should
              %% not increase the "fail" counter
              
              %% methodology: send an input to partition A and
              %% imediately send eoi; have A send a recursive input to
              %% partition B; have B send a recursive input to C;
              %% finally have C send a recursive in put back to A
              %%
              %% this flow should give worker A time to start shutting
              %% down, but not to finish, resulting in an input in its
              %% queue after it completes its done/1 function
              {"restart after eoi",
               fun() ->
                       Inputs = [0, 1, 2, 0],
                       ChashFun = fun([Head|_]) ->
                                          chash:key_of(Head)
                                  end,
                       Spec = #fitting_spec{name=restarter,
                                            module=riak_pipe_w_crash,
                                            arg={recurse_done_pause, 500},
                                            chashfun=ChashFun},

                       %% just make sure we are bouncing between partitions
                       {ok, R} = riak_core_ring_manager:get_my_ring(),
                       ?assert(riak_core_ring:preflist(
                                 ChashFun(Inputs), R) /=
                                   riak_core_ring:preflist(
                                     ChashFun(tl(Inputs)), R)),
                       ?assert(riak_core_ring:preflist(
                                 ChashFun(Inputs), R) /=
                                   riak_core_ring:preflist(
                                     ChashFun(tl(tl(Inputs))), R)),

                       {ok, Pipe} =
                           riak_pipe:exec(
                             [Spec],
                             [{log, sink},{trace, [error, done, restart]}]),
                       riak_pipe:queue_work(Pipe, Inputs),
                       riak_pipe:eoi(Pipe),
                       {eoi, [], Trace} = riak_pipe:collect_results(Pipe),

                       %% no error traces -- the error will say
                       %% {reason, normal} if the worker received new
                       %% inputs while shutting down due to eoi
                       ?assertEqual([], extract_trace_errors(Trace)),
                       
                       %% A should have restarted, but not counted failure
                       [Restarted] = extract_restart(Trace),
                       Dones = extract_vnode_done(Trace),
                       RestartStats = proplists:get_value(Restarted, Dones),
                       ?assertEqual(0, proplists:get_value(failures,
                                                           RestartStats))
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
                                                     chashfun=fun zero_part/1}))
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
                                                     chashfun=fun zero_part/1}))
               end}
      end,
      fun(_) ->
              {"good partfun",
               fun() ->
                       ok = (catch
                                 riak_pipe_fitting:validate_fitting(
                                   #fitting_spec{name=empty_pass,
                                                 module=riak_pipe_w_pass,
                                                 chashfun=follow}))
               end}
      end,
      fun(_) ->
              {"bad partfun",
               fun() ->
                       badarg = (catch
                                     riak_pipe_fitting:validate_fitting(
                                      #fitting_spec{name=empty_pass,
                                                    module=riak_pipe_w_pass,
                                                    chashfun=fun(_,_) -> 0 end}))
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

limits_test_() ->
    AllLog = [{log, sink}, {trace, all}],

     {foreach,
     prepare_runtime(),
     teardown_runtime(),
      [
       {timeout, 60,
        {"Verify that handoff mechanism does what it is supposed to",
         fun() ->
                 PipeLen = 20,
                 Spec = [#fitting_spec{name="worker limit mult pipes " ++
                                           integer_to_list(X),
                                       module=riak_pipe_w_xform,
                                       arg=fun xform_or_crash/3
                                       %% , chashfun=fun zero_part/1
                                      } || X <- lists:seq(1, PipeLen)],
                 {ok, Slave0} = start_slave0(),
                 SlaveNode0 = slave0_nodename(),

                 riak_core_gossip:send_ring(node(), SlaveNode0),
                 timer:sleep(500),
                 riak_core_gossip:send_ring(SlaveNode0, node()),
                 timer:sleep(200),
                 {ok, SlaveRing} = rpc:call(SlaveNode0, riak_core_ring_manager,
                                            get_my_ring, []),
                 {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
                 ?assert(riak_core_ring:all_owners(SlaveRing) ==
                             riak_core_ring:all_owners(MyRing)),

                 {ok, Pipe1} =
                     riak_pipe:exec(Spec, AllLog),
                 {ok, Pipe2} =
                     riak_pipe:exec(Spec, AllLog),

                 ok = riak_pipe:queue_work(Pipe1, 100),
                 timer:sleep(100),

                 rpc:call(Slave0, erlang, halt, []),
                 slave:stop(Slave0),
                 timer:sleep(200),

                 rpc:call(Slave0, init, halt, []),
                 slave:stop(Slave0),
                 timer:sleep(200),
                 pang = net_adm:ping(SlaveNode0),

                 [ok = riak_pipe:queue_work(Pipe2, X) ||
                     X <- lists:seq(101, 120)],

                 %% We seem to have lots of SMP racing games when
                 %% using riak_core_tracer.
                 erlang:system_flag(multi_scheduling, block),

                 %% The ?T() macro is a bit annoying here, because we
                 %% need to know the #fitting_details in order to use
                 %% it.  But the tracing that I'd like to do at the
                 %% vnode level doesn't have that record available to
                 %% it, and it looks like it's a royal pain to add it.
                 %% So we'll steal something from Jon's playbook and
                 %% use riak_core_tracer instead.
                 riak_core_tracer:start_link(),
                 riak_core_tracer:reset(),
                 riak_core_tracer:filter(
                   [{riak_pipe_vnode, handle_handoff_command},
                    {riak_pipe_vnode, replace_worker}
                   ],
                   fun({trace, _Pid, call,
                        {riak_pipe_vnode, handle_handoff_command,
                         [Cmd, _Sender, _State]}}) ->
                           element(1, Cmd);
                      ({trace, _Pid, call,
                        {riak_pipe_vnode, replace_worker, _Args}}) ->
                           replace_worker
                   end),
                 riak_core_tracer:collect(10*1000),

                 %% Give slave a chance to start and master to notice it.
                 {ok, Slave0} = start_slave0(),
                 timer:sleep(5000),

                 [ok = riak_pipe:queue_work(Pipe2, X) ||
                     X <- lists:seq(121, 140)],

                 riak_pipe:eoi(Pipe1),
                 riak_pipe:eoi(Pipe2),
                 {eoi, _Out1, Trace1} = collect_results(Pipe1, 1000),
                 {eoi, _Out2, Trace2} = collect_results(Pipe2, 1000),

                 [] = extract_trace_errors(Trace1),
                 [] = extract_trace_errors(Trace2),
                 1 = length(_Out1),
                 40 = length(_Out2),

                 %% VM trace verification
                 timer:sleep(1000),
                 riak_core_tracer:stop_collect(),
                 Traces = riak_core_tracer:results(),
                 erlang:system_flag(multi_scheduling, unblock),

                 FoldReqs = length([x || {_, riak_core_fold_req_v1} <- Traces]),
                 ?assert(FoldReqs > 0),         % At least 4 ?FOLD_REQ{} ?
                 Archives = length([x || {_, cmd_archive} <- Traces]),
                 ?assert(Archives > 0),          % At least 67 ?
                 Replaces = length([x || {_, replace_worker} <- Traces]),
                 ?assert(Replaces > 0)          % At least 67 ?
         end}}
      ]
     }.

should_be_the_very_last_test() ->
    Leftovers = [{Pid, X} ||
                    Pid <- processes(),
                    {eunit, X} <- element(2, process_info(Pid, dictionary))],
    [] = Leftovers.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% NOTE: Do not put any EUnit tests after should_be_the_very_last_test()
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-endif.  % TEST
