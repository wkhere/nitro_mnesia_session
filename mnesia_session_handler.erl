%% (c) 2011-2013 Wojciech Kaczmarek <frk@kofeina.net>.
%% All rights reverved. See license for more details:
%% http://github.com/herenowcoder/nitro_mnesia_session/raw/master/LICENSE
-module(mnesia_session_handler).
-behaviour(session_handler).
-export([
    install/0,
    all_records/0,
    init/2, 
    finish/2,
    get_value/4, 
    set_value/4, 
    clear_all/2,
    session_id/2
]).
-include_lib("stdlib/include/qlc.hrl").
-define(cookie, "newcookie"). % cookie name if undefined in etc/app.config

%% types
-type unique_token() :: binary().
-type key() :: any().
-type val() :: any().
-type config() :: any().
-type state() :: unique_token() | [].

%% handler protocol

-spec init(config(), state()) -> {ok, state()}.
init(_Config, _State) -> 
    Cookie = wf:cookie(wf:config_default(cookie_name, ?cookie)),
    State = case wf:depickle(Cookie) of
        undefined -> unique();
        X -> X
    end,
    {ok, State}.

-spec finish(config(), state()) -> {ok, state()}.
finish(_Config, State) -> 
    Timeout = wf:config_default(session_timeout, 20),
    ok = wf:cookie(wf:config_default(cookie_name, ?cookie),
                   wf:pickle(State), "/", Timeout),
    {ok, []}.

-spec get_value(key(), val(), config(), state()) -> {ok, val(), state()}.
get_value(K, DefaultV, _Config, State) ->
    DbKey = cons_dbkey(K, State),
    F = fun()-> mnesia:read(session, DbKey) end,
    {atomic, Xs} = mnesia:transaction(F),
    {ok, value_or_default(Xs, DefaultV), State}.

-spec set_value(key(), val(), config(), state()) -> {ok, val(), state()}.
set_value(K, V, _Config, State) ->
    DbKey = cons_dbkey(K, State),
    F = fun()-> Olds = mnesia:read(session, DbKey),
                mnesia:write({session, DbKey, State, V, now()}),
                Olds
        end,
    {atomic, Olds} = mnesia:transaction(F),
    {ok, value_or_default(Olds, undefined), State}.

-spec clear_all(config(), state()) -> {ok, state()}.
clear_all(_Config, State) ->
    {atomic,_} = mnesia:transaction(fun delete_all_state/1, [State]),
    {ok, State}.

-spec session_id(config(), state()) -> {ok, binary(), state()}.
session_id(_Config, State) ->
    {ok, SessionId} = wf:hex_encode(State),
    {ok, SessionId, State}.

%% utilities

-spec install() -> ok.
install() ->
    ok = mnesia:start(),
    Me = node(),
    case mnesia:change_table_copy_type(schema, Me, disc_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists,schema,Me,disc_copies}} -> ok;
        {aborted, Err} -> throw({aborted, Err})
    end,
    case mnesia:create_table(
           session, [ {type, set}, {attributes, [key,skey,val,timestamp]},
                      {index, [skey]},
                      {disc_copies,[Me]} ])
    of
        {atomic, ok} -> ok;
        {aborted, {already_exists,session}} -> ok;
        {aborted, Err2} -> throw({aborted, Err2})
    end.

-spec all_records() -> [tuple()].
all_records() ->
    Q = qlc:q([X || X <- mnesia:table(session)]),
    {atomic, Xs} = mnesia:transaction(fun()-> qlc:e(Q) end),
    Xs.

%% todo: expire records


%%% private

-spec cons_dbkey(key(), state()) -> any().
cons_dbkey(K, State) ->
    {State, K}.

q_keys_with_state(State) ->
    qlc:q(
      [ K || {session,{S,K},S,_,_} <- mnesia:table(session),
             S =:= State ]).

delete_all_state(State) ->
    qlc:fold(fun(X,_)->
                     ok = mnesia:delete({session,{State,X}}),
                     anything
             end,
             anything,
             q_keys_with_state(State)),
    ok.

-spec unique() -> unique_token().
unique() -> term_to_binary(make_ref()).

value_or_default([{session,_,_,V,_}], _) -> V;
value_or_default([], Default) -> Default.


%%% tests
-include_lib("eunit/include/eunit.hrl").

simple_ts() ->
    State = unique(),
    {K1,V1} = {unique(), 42},
    {K2,V2} = {unique(), 23},
    K3 = unique(),
    {ok,OldV1,State} = set_value(K1,V1,[],State),
    {ok,OldV2,State} = set_value(K2,V2,[],State),
    {ok,NewV2,State} = set_value(K2,V2,[],State),
    {ok,V1_,State} = get_value(K1,default,[],State),
    {ok,V2_,State} = get_value(K2,default,[],State),
    {ok,V3_,State} = get_value(K3,default,[],State),
    {atomic,Ks_} = mnesia:transaction(fun()-> qlc:e(q_keys_with_state(State)) 
                                      end),
    State2 = unique(),
    {ok,_,State2} = set_value(K1,V1,[],State2),
    {ok,State} = clear_all([],State),
    {atomic,NoKs} = mnesia:transaction(fun()-> qlc:e(q_keys_with_state(State)) 
                                       end),
    {atomic,Ks2} = mnesia:transaction(fun()-> qlc:e(q_keys_with_state(State2))
                                      end),
    [ 
      ?_assertEqual(OldV1, undefined),
      ?_assertEqual(OldV2, undefined),
      ?_assertEqual(NewV2, V2),
      ?_assertEqual(V1_, V1),
      ?_assertEqual(V2_, V2),
      ?_assertEqual(V3_, default),
      ?_assertEqual(lists:sort(Ks_), lists:sort([K1,K2])),
      ?_assertEqual(NoKs, []),
      ?_assertEqual(Ks2, [K1]),
      []].

simple_test_() ->
    ok = install(),
    TestsRef = make_ref(),
    %% need to pass tests via dict, because transaction is aborted manually
    Wrapper = fun()-> put(TestsRef, simple_ts()),
                      mnesia:abort(my_rollback)
              end,
    {aborted, my_rollback} = mnesia:transaction(Wrapper),
    erase(TestsRef).
