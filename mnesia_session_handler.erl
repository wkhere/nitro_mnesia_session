%% (c) 2011 Wojciech Kaczmarek
-module(mnesia_session_handler).
-behaviour(session_handler).
-export([
    install/0,
    all_records/0,
    init/2, 
    finish/2,
    get_value/4, 
    set_value/4, 
    clear_all/2
]).
-include_lib("stdlib/include/qlc.hrl").


%% utilities

install() ->
    ok = mnesia:start(),
    Me = node(),
    case mnesia:change_table_copy_type(schema, Me, disc_copies) of
        {atomic, ok} -> ok;
        {aborted, {already_exists,schema,Me,disc_copies}} -> ok;
        {aborted, Err} -> throw({aborted, Err})
    end,
    case mnesia:create_table(
           session, [ {type, bag}, {attributes, [skey,k,v,timestamp]},
                      {disc_copies,[Me]} ])
    of
        {atomic, ok} -> ok;
        {aborted, {already_exists,_}} -> ok;
        {aborted, Err2} -> throw({aborted, Err2})
    end.

all_records() ->
    Q = qlc:q([X || X <- mnesia:table(session)]),
    {atomic, Xs} = mnesia:transaction(fun()-> qlc:e(Q) end),
    Xs.

%% todo: expire records


%% handler protocol

init(_Config, _State) -> 
    Cookie = wf:cookie(wf:config_default(cookie_name, newcookie)),
    State = case wf:depickle(Cookie) of
        undefined -> unique();
        X -> X
    end,
    {ok, State}.

finish(_Config, State) -> 
    Timeout = wf:config_default(session_timeout, 20),
    ok = wf:cookie(wf:config_default(cookie_name, newcookie),
                   wf:pickle(State), "/", Timeout),
    {ok, []}.

get_value(K, DefaultV, _Config, State) ->
    Q = qlc:q([ V || {session,SK,K0,V,_} <- mnesia:table(session),
                SK=:=State, K0=:=K ]),
    {atomic, Xs} = mnesia:transaction(fun()-> qlc:e(Q) end),
    {ok, value_or_default(Xs, DefaultV), State}.

set_value(K, V, _Config, State) ->
    Q = qlc:q([ {V0,T0} || {session,SK,K0,V0,T0} <- mnesia:table(session),
                SK=:=State, K0=:=K ]),
    F = fun()-> Xs0 = qlc:e(Q),
                [ mnesia:delete_object({session,State,K,V0,T0})
                  || {V0,T0} <- Xs0 ],
                mnesia:write({session, State, K, V, now()}),
                [ V0 || {V0,_} <- Xs0 ]
        end,
    {atomic, Olds} = mnesia:transaction(F),
    {ok, value_or_default(Olds, undefined), State}.

clear_all(_Config, State) ->
    {atomic,ok} = mnesia:transaction(fun mnesia:delete/1, [{session,State}]),
    {ok, State}.


%%% private

q_keys_with_state(State) ->
    qlc:q(
      [ K || {session,SK,K,_,_} <- mnesia:table(session),
             SK=:=State ]).

unique() -> make_ref().

value_or_default([V], _) -> V;
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
    {ok,State} = clear_all([],State),
    {atomic,NoKs} = mnesia:transaction(fun()-> qlc:e(q_keys_with_state(State)) 
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
