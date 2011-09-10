%% -------------------------------------------------------------------
%%
%% riak_pipe_qcover_sup: supervise the riak_pipe qcover state machines.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc supervise the riak_pipe qcover state machines

-module(riak_pipe_qcover_sup).

-behaviour(supervisor).

-export([start_qcover_fsm/1]).
-export([start_link/0]).
-export([init/1]).

start_qcover_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

%% @spec start_link() -> ServerRet
%% @doc API for starting the supervisor.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @spec init([]) -> SupervisorTree
%% @doc supervisor callback.
init([]) ->
    FsmSpec = {undefined,
               {riak_core_coverage_fsm, start_link, [riak_pipe_qcover_fsm]},
               temporary, 5000, worker, [riak_pipe_qcover_fsm]},

    {ok, {{simple_one_for_one, 10, 10}, [FsmSpec]}}.
