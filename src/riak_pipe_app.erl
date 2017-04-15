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

%% @doc Start and stop handling of the `riak_pipe' application.

-module(riak_pipe_app).

-behaviour(application).

%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

%% @doc Start the `riak_pipe' application.
%%
%%      The `riak_core' application should already be started.  This
%%      function will register the `riak_pipe_vnode' module to setup
%%      the riak_pipe vnode master, and will also announce the
%%      riak_pipe service to the node watcher.
%%
%%      If cluster_info has also been started, this function will
%%      register the `riak_pipe_cinfo' module with it.
-spec start(term(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    %% startup mostly copied from riak_kv
    catch cluster_info:register_app(riak_pipe_cinfo),

    case riak_pipe_sup:start_link() of
        {ok, Pid} ->
            riak_core:register(riak_pipe, [
                {vnode_module, riak_pipe_vnode},
                {stat_mod, riak_pipe_stat}
            ]),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Prepare to stop - called before the supervisor tree is shutdown
prep_stop(_State) ->
    try %% wrap with a try/catch - application carries on regardless,
        %% no error message or logging about the failure otherwise.
        lager:info("Stopping application riak_pipe - marked service down.\n", []),
        riak_core_node_watcher:service_down(riak_pipe)
    catch
        Type:Reason ->
            lager:error("Stopping application riak_pipe - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

%% @doc Unused.
-spec stop(term()) -> ok.
stop(_State) ->
    lager:info("Stopped  application riak_pipe.\n", []),
    ok.
