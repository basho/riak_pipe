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

%% @doc Methods for sending messages to the sink.
%%
%%      Sink messages are delivered as three record types:
%%      `#pipe_result{}', `#pipe_log{}', and `#pipe_eoi{}'.
-module(riak_pipe_sink).

-export([
         result/3,
         log/3,
         eoi/1
        ]).

-include("riak_pipe.hrl").

%% @doc Send a result to the sink (used by worker processes).  The
%%      result is delivered as a `#pipe_result{}' record in the sink
%%      process's mailbox.
-spec result(term(), Sink::riak_pipe:fitting(), term()) -> #pipe_result{}.
result(From, #fitting{pid=Pid, ref=Ref, chashfun=sink}, Output) ->
    Pid ! #pipe_result{ref=Ref, from=From, result=Output}.

%% @doc Send a log message to the sink (used by worker processes and
%%      fittings).  The message is delivered as a `#pipe_log{}' record
%%      in the sink process's mailbox.
-spec log(term(), Sink::riak_pipe:fitting(), term()) -> #pipe_log{}.
log(From, #fitting{pid=Pid, ref=Ref, chashfun=sink}, Msg) ->
    Pid ! #pipe_log{ref=Ref, from=From, msg=Msg}.

%% @doc Send an end-of-inputs message to the sink (used by fittings).
%%      The message is delivered as a `#pipe_eoi{}' record in the sink
%%      process's mailbox.
-spec eoi(Sink::riak_pipe:fitting()) -> #pipe_eoi{}.
eoi(#fitting{pid=Pid, ref=Ref, chashfun=sink}) ->
    Pid ! #pipe_eoi{ref=Ref}.
