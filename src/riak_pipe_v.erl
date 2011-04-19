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

-module(riak_pipe_v).

-export([validate_module/2,
         validate_function/3,
         type_of/1]).

validate_module(Label, Module) when is_atom(Module) ->
    case code:is_loaded(Module) of
        {file, _} -> ok;
        false ->
            case code:load_file(Module) of
                {module, Module} -> ok;
                {error, Error} ->
                    {error, io_lib:format(
                              "~s must be a valid module name"
                              " (failed to load ~p: ~p)",
                              [Label, Module, Error])}
            end
    end;
validate_module(Label, Module) ->
    {error, io_lib:format("~s must be an atom, not a ~p",
                          [Label, type_of(Module)])}.

validate_function(Label, Arity, Fun) when is_function(Fun) ->
    Info = erlang:fun_info(Fun),
    case proplists:get_value(arity, Info) of
        Arity ->
            case proplists:get_value(type, Info) of
                local ->
                    %% reference was validated by compiler
                    ok;
                external ->
                    Module = proplists:get_value(module, Info),
                    Function = proplists:get_value(name, Info),
                    validate_exported_function(
                      Label, Arity, Module, Function)
            end;
        N ->
            {error, io_lib:format("~s must be of arity ~b, not ~b",
                                  [Label, Arity, N])}
    end;
validate_function(Label, Arity, Fun) ->
    {error, io_lib:format("~s must be a function (arity ~b), not a ~p",
                          [Label, Arity, type_of(Fun)])}.

validate_exported_function(Label, Arity, Module, Function) ->
    case validate_module("", Module) of
        ok ->
            Exports = Module:module_info(exports),
            case lists:member({Function,Arity}, Exports) of
                true -> 
                    ok;
                false ->
                    {error, io_lib:format(
                              "~s specifies ~p:~p/~b, which is not exported",
                              [Label, Module, Function, Arity])}
            end;
        {error,Error} ->
            {error, io_lib:format("invalid module named in ~s function:~n~s",
                                  [Label, Error])}
    end.

type_of(Term) ->
    case erl_types:t_from_term(Term) of
        {c,identifier,[Type|_],_} ->
            Type; % pid,reference
        {c,Type,_,_} ->
            Type  % list,tuple,atom,number,binary,function
    end.
