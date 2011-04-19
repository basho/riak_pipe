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

-module(riak_pipe_cinfo).

-export([cluster_info_init/0,
         cluster_info_generator_funs/0]).

-include("riak_pipe_debug.hrl").

cluster_info_init() ->
    ok.

cluster_info_generator_funs() ->
    [
     {"Riak Pipelines", fun pipelines/1},
     {"Riak Pipeline Vnode Queues", fun queues/1}
    ].

pipelines(C) ->
    Pipes = riak_pipe_builder_sup:builder_pids(),
    cluster_info:format(C, "Pipelines active: ~b~n", [length(Pipes)]),
    [ pipeline(C, Pipe) || {_Id, Pipe, _Type, _Mods} <- Pipes],
    ok.

pipeline(C, Pipe) ->
    case riak_pipe_builder:fitting_pids(Pipe) of
        {ok, {UnstartedCount, Fits}} ->
            cluster_info:format(C, " - ~p fittings: ~b unstarted, ~b alive~n",
                                [Pipe, UnstartedCount, length(Fits)]),
            [ fitting(C, Fit) || Fit <- Fits ];
        gone ->
            cluster_info:format(C, " - ~p *gone*~n", [Pipe])
    end,
    ok.

fitting(C, Fit) ->
    case riak_pipe_fitting:workers(Fit) of
        {ok, Workers} ->
            %% TODO: add 'name' from details? maybe module/etc. too?
            cluster_info:format(C, "   + ~p worker partitions: ~b~n",
                                [Fit, length(Workers)]),
            [ fitting_worker(C, W) || W <- Workers ];
        gone ->
            cluster_info:format(C, "   + ~p *gone*~n", [Fit])
    end,
    ok.

fitting_worker(C, W) ->
    cluster_info:format(C, "     * ~p~n", [W]),
    ok.

queues(C) ->
    VnodePids = riak_core_vnode_master:all_nodes(riak_pipe_vnode),
    cluster_info:format(C, "Vnodes active: ~b~n", [length(VnodePids)]),
    [ queues(C, V) || V <- VnodePids],
    ok.

queues(C, V) ->
    {Partition, Workers} = riak_pipe_vnode:status(V),
    cluster_info:format(C, " - ~p workers: ~b~n",
                        [Partition, length(Workers)]),
    [ queue(C, W) || W <- Workers ],
    ok.

queue(C, WorkerProps) ->
    cluster_info:format(C, "   + ~p~n", [WorkerProps]),
    ok.
