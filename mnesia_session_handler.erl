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
-record (state, {unique, node}).

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
    % Get the session cookie and node...
    Cookie = wf:cookie(get_cookie_name()),
    State = case wf:depickle(Cookie) of
        undefined -> new_state();
        Other -> Other
    end,
    {ok, State}.

finish(_Config, State) -> 
    % Drop the session cookie...
    Timeout = wf:config_default(session_timeout, 20),
    ok = wf:cookie(get_cookie_name(), wf:pickle(State), "/", Timeout),
    {ok, []}.

get_value(Key, DefaultValue, _Config, State) ->
    F = fun()-> mnesia:read(session, cons_key(Key, State)) end,
    {atomic, Xs} = mnesia:transaction(F),
    Value = case Xs of
        [{session,_,V,_}] -> V;
        [] -> DefaultValue
    end,
    {ok, Value, State}.

set_value(Key, Value, _Config, State) ->
    K = cons_key(Key, State),
    F = fun()-> Olds = mnesia:read(session, K),
                mnesia:write({session, K, Value, now()}),
                Olds
        end,
    {atomic, Olds} = mnesia:transaction(F),
    OldValue = case Olds of 
        [{session,_,V,_}] -> V;
        [] -> undefined
    end,
    {ok, OldValue, State}.

clear_all(_Config, State) -> 
    %% todo: clear all keys with this unique state
    {ok, State}.

%%% private

cons_key(Key, State) ->
    {State#state.unique, Key}.

get_cookie_name() ->
    wf:config_default(cookie_name, "newcookie").

new_state() ->
    Unique = erlang:md5(term_to_binary({now(), erlang:make_ref()})),
    #state { unique=Unique }.
