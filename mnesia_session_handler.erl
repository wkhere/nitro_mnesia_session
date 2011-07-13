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


% utilities

install() ->
    mnesia:start(),
    case mnesia:create_table(
           session, [ {type, set}, {attributes, [key,val,timestamp]},
                      {disc_copies,[node()]} ])
    of
        {atomic, ok} -> ok;
        {aborted, {already_exists,_}} -> ok;
        {aborted, Reason} -> {err, Reason}
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
    DbKey = cons_key(K, State),
    F = fun()-> mnesia:read(session, DbKey) end,
    {atomic, Xs} = mnesia:transaction(F),
    {ok, value_or_default(Xs, DefaultV), State}.

set_value(K, V, _Config, State) ->
    DbKey = cons_key(K, State),
    F = fun()-> Olds = mnesia:read(session, DbKey),
                mnesia:write({session, DbKey, V, now()}),
                Olds
        end,
    {atomic, Olds} = mnesia:transaction(F),
    {ok, value_or_default(Olds, undefined), State}.

clear_all(_Config, State) -> 
    %% todo: clear all keys with this unique state
    {ok, State}.


%%% private

cons_key(K, State) ->
    {State, K}.

unique() -> make_ref().

value_or_default([{session,_,V,_}], _) -> V;
value_or_default([], Default) -> Default.
