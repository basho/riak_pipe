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

-module(riak_pipe_w_reduce).
-behaviour(riak_pipe_vnode_worker).

-export([init/2,
         process/2,
         done/1,
         archive/1,
         handoff/2,
         validate_arg/1]).
-export([partfun/1]).

-include("riak_pipe.hrl").

-record(state, {accs, p, fd}).

init(Partition, FittingDetails) ->
    {ok, #state{accs=dict:new(), p=Partition, fd=FittingDetails}}.

process({Key, Input}, #state{accs=Accs}=State) ->
    case dict:find(Key, Accs) of
        {ok, OldAcc} -> ok;
        error        -> OldAcc=[]
    end,
    InAcc = [Input|OldAcc],
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            {ok, State#state{accs=dict:store(Key, OutAcc, Accs)}};
        {error, {Type, Error, Trace}} ->
            %%TODO: forward
            error_logger:error_msg(
              "~p:~p reducing:~n   ~P~n   ~P",
              [Type, Error, InAcc, 2, Trace, 5]),
            {ok, State}
    end.

done(#state{accs=Accs, p=Partition, fd=FittingDetails}) ->
    [ riak_pipe_vnode_worker:send_output(A, Partition, FittingDetails)
      || A <- dict:to_list(Accs)],
    ok.

archive(#state{accs=Accs}) ->
    %% just send state of reduce so far
    {ok, Accs}.

handoff(HandoffAccs, #state{accs=Accs}=State) ->
    %% for each Acc, add to local accs;
    NewAccs = dict:merge(fun(K, HA, A) ->
                                 handoff_acc(K, HA, A, State)
                         end,
                         HandoffAccs, Accs),
    {ok, State#state{accs=NewAccs}}.

handoff_acc(Key, HandoffAccs, LocalAccs, State) ->
    InAcc = HandoffAccs++LocalAccs,
    case reduce(Key, InAcc, State) of
        {ok, OutAcc} ->
            OutAcc;
        {error, {Type, Error, Trace}} ->
                error_logger:error_msg(
                  "~p:~p reducing handoff:~n   ~P~n   ~P",
                  [Type, Error, InAcc, 2, Trace, 5]),
            LocalAccs %% don't completely barf
    end.

reduce(Key, InAcc, #state{p=Partition, fd=FittingDetails}) ->
    Fun = FittingDetails#fitting_details.arg,
    try
        {ok, OutAcc} = Fun(Key, InAcc, Partition, FittingDetails),
        true = is_list(OutAcc), %%TODO: nicer error
        {ok, OutAcc}
    catch Type:Error ->
            {error, {Type, Error, erlang:get_stacktrace()}}
    end.

validate_arg(Fun) when is_function(Fun) ->
    riak_pipe_v:validate_function("arg", 4, Fun);
validate_arg(Fun) ->
    {error, io_lib:format("~p requires a function as argument, not a ~p",
                          [?MODULE, riak_pipe_v:type_of(Fun)])}.

partfun({Key,_}) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    DocIdx = chash:key_of(Key),
    [{Partition, _}|_] = riak_core_ring:preflist(DocIdx, Ring),
    Partition.
