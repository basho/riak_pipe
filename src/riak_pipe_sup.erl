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

-module(riak_pipe_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    VMaster = {riak_pipe_vnode_master,
               {riak_core_vnode_master, start_link, [riak_pipe_vnode]},
               permanent, 5000, worker, [riak_core_vnode_master]},
    BSup = {riak_pipe_builder_sup,
            {riak_pipe_builder_sup, start_link, []},
               permanent, 5000, supervisor, [riak_pipe_builder_sup]},
    FSup = {riak_pipe_fitting_sup,
            {riak_pipe_fitting_sup, start_link, []},
            permanent, 5000, supervisor, [riak_pipe_fitting_sup]},
    {ok, { {one_for_one, 5, 10}, [VMaster, BSup, FSup]} }.
