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

-module(riak_pipe_log).

-export([log/2,
         trace/3]).

-include("riak_pipe.hrl").

log(#fitting_details{options=O, name=N}, Msg) ->
    case proplists:get_value(log, O) of
        undefined ->
            ok; %% no logging
        sink ->
            Sink = proplists:get_value(sink, O),
            riak_pipe:log(N, Sink, Msg);
        sasl ->
            error_logger:info_msg(
              "Pipe log -- ~s:~n~P",
              [riak_pipe_fitting:format_name(N), Msg, 9])
    end.

trace(#fitting_details{options=O, name=N}=FD, Types, Msg) ->
    TraceOn = case proplists:get_value(trace, O) of
                  all ->
                      {true, all};
                  undefined ->
                      false;
                  EnabledSet ->
                      MatchSet = sets:from_list([node(),N|Types]),
                      Intersection = sets:intersection(EnabledSet,
                                                       MatchSet),
                      case sets:size(Intersection) of
                          0 -> false;
                          _ -> {true, sets:to_list(Intersection)}
                      end
              end,
    case TraceOn of
        {true, Traces} -> log(FD, {trace, Traces, Msg});
        false          -> ok
    end.
