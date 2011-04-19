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

-module(riak_pipe_w_xform).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/2,
         done/1,
         validate_arg/1]).

-include_lib("riak_kv/include/riak_kv_vnode.hrl").
-include("riak_pipe.hrl").

-record(state, {p, fd}).

init(Partition, FittingDetails) ->
    {ok, #state{p=Partition, fd=FittingDetails}}.

process(Input, #state{p=Partition, fd=FittingDetails}=State) ->
    Fun = FittingDetails#fitting_details.arg,
    try
        ok = Fun(Input, Partition, FittingDetails)
    catch Type:Error ->
            %%TODO: forward
            error_logger:info_msg("~p:~p xforming:~n   ~P~n   ~P",
                                  [Type, Error, Input, 3,
                                   erlang:get_stacktrace(), 5])
    end,
    {ok, State}.

done(_State) ->
    ok.

validate_arg(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 3, Fun);
validate_arg(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.
