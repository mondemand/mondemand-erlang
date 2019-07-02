-module (mondemand_util).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-compile ([{parse_transform,ct_expand}]).

% lwes helper functions
-export ([
           context_from_context/2,
           context_from_lwes/1,
           context_to_lwes/3
         ]).

% util functions
-export ([ find_in_dict/2,
           find_in_dict/3,
           binaryify/1,
           binaryify/2,
           binaryify_context/1,
           stringify/1,
           stringify/2,
           integerify/1,
           floatify/1,
           join/2,
           first_defined/1
         ]).
%% Time functions
-export ([
          micros_since_epoch/0,
          millis_since_epoch/0,
          seconds_since_epoch/0,
          minutes_since_epoch/0,
          micros_epoch_to_erlang_now/1, % (MicrosTimeSinceEpoch) -> {_,_,_}
          millis_epoch_to_erlang_now/1, % (MillisSinceEpoch) -> {_,_,}
          now_to_mdyhms/1,              % ({_,_,_}) -> {{_,_,_},{_,_,_}}
          now_to_epoch_millis/1,        % ({_,_,_}) -> MillisSinceEpoch
          now_to_epoch_secs/1,          % ({_,_,_}) -> SecondsSinceEpoch
          now_to_epoch_minutes/1,       % ({_,_,_}) -> MinutesSinceEpoch
          now_diff_milliseconds/2,
          current/0,
          current_minute/0,
          millis_to_next_round_second/0,
          millis_to_next_round_second/1,
          millis_to_next_round_minute/0,
          millis_to_next_round_minute/1
         ]).

%% Other functions
-export ([ normalize_ip/1,
           listen/1,
           dummy/0 ]).

context_from_context (DefaultHost, Context) ->
  case lists:keytake (?MD_HOST, 1, Context) of
    false -> {DefaultHost, Context};
    {value, {?MD_HOST, Host}, NewContext} -> {Host, NewContext}
  end.

context_from_lwes (Data) ->
  Num = mondemand_util:find_in_dict (?MD_CTXT_NUM, Data, 0),
  { Host, Context } =
    lists:foldl ( fun (N, {H, A}) ->
                    K = dict:fetch (context_name_key (N), Data),
                    V = dict:fetch (context_value_key (N), Data),
                    case K of
                      ?MD_HOST -> { V, A };
                      _ -> { H, [ {K, V} | A ] }
                    end
                  end,
                  { <<"unknown">>, [] },
                  lists:seq (1,Num)
                ),
  { Host, length (Context), lists:keysort (1, Context) }.

context_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_CTXT_K).

context_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_CTXT_V).

context_to_lwes (Host, NumContexts, Context) ->
  case lists:keymember (?MD_HOST, 1, Context) of
    false ->
      case Host of
        undefined ->
          [
            { ?LWES_U_INT_16, ?MD_CTXT_NUM, NumContexts},
            lists:zipwith (fun context_to_lwes/2,
                           lists:seq (1, NumContexts),
                           Context)
          ];
        _ ->
          [
            { ?LWES_U_INT_16, ?MD_CTXT_NUM, NumContexts + 1},
            lists:zipwith (fun context_to_lwes/2,
                           lists:seq (1, NumContexts),
                           Context),
            context_to_lwes (NumContexts+1, { ?MD_HOST, Host })
          ]
     end;
    true ->
      [
        { ?LWES_U_INT_16, ?MD_CTXT_NUM, NumContexts},
        lists:zipwith (fun context_to_lwes/2,
                       lists:seq (1, NumContexts),
                       Context)
      ]
  end.

context_to_lwes (ContextIndex, {ContextKey, ContextValue}) ->
  [ { ?LWES_STRING,
      context_name_key (ContextIndex),
      stringify (ContextKey)
    },
    { ?LWES_STRING,
      context_value_key (ContextIndex),
      stringify (ContextValue)
    }
  ].

-define(KILO, 1000).
-define(MEGA, 1000000).
-define(GIGA, 1000000000).
-define(TERA, 1000000000000).

micros_since_epoch () ->
  now_to_epoch_micros (os:timestamp()).
millis_since_epoch () ->
  now_to_epoch_millis (os:timestamp()).
seconds_since_epoch () ->
  now_to_epoch_secs (os:timestamp()).
minutes_since_epoch () ->
  now_to_epoch_minutes (os:timestamp()).

micros_epoch_to_erlang_now (Ts) ->
  Mega = Ts div ?TERA,
  TempRes = Ts - Mega * ?TERA,
  Sec = TempRes div ?MEGA,
  Micro = TempRes - Sec * ?MEGA,
  {Mega, Sec, Micro}.

millis_epoch_to_erlang_now (Ts) ->
  Mega = Ts div ?GIGA, % since we are in millis we div by GIGA here
  TempRes = Ts - Mega * ?GIGA,
  Sec = TempRes div ?KILO,
  % take left over milliseconds and multiply by KILO to get micros
  Micro = (TempRes - Sec * ?KILO) * ?KILO,
  {Mega, Sec, Micro}.

now_to_mdyhms (Now = {_, _, _}) ->
  calendar:now_to_universal_time (Now).
now_to_epoch_micros ({Meg, Sec, Mic}) ->
  Meg * ?TERA + Sec * ?MEGA + Mic.
now_to_epoch_millis ({Meg, Sec, Mic}) ->
  trunc (Meg * ?GIGA + Sec * ?KILO + Mic / ?KILO).
now_to_epoch_secs ({Mega, Secs, _}) ->
  Mega * ?MEGA + Secs.
now_to_epoch_minutes (Now) ->
  trunc (now_to_epoch_secs (Now) / 60).

%% NOTE: Copied from webmachine with the mailing list link corrected,
%% webmachine license is
%%
%%    Licensed under the Apache License, Version 2.0 (the "License");
%%    you may not use this file except in compliance with the License.
%%    You may obtain a copy of the License at
%%
%%        http://www.apache.org/licenses/LICENSE-2.0
%%
%%    Unless required by applicable law or agreed to in writing, software
%%    distributed under the License is distributed on an "AS IS" BASIS,
%%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%    See the License for the specific language governing permissions and
%%    limitations under the License.
%%
%% File is/was
%%
%% https://github.com/webmachine/webmachine/blob/master/src/webmachine_util.erl
%%
%% Test is also copied below
%%
%% This is faster than timer:now_diff() because it does not use bignums.
%% But it returns *milliseconds*  (timer:now_diff returns microseconds.)
%% From http://erlang.org/pipermail/erlang-questions/2002-June/005037.html
%%
%% @doc  Compute the difference between two now() tuples, in milliseconds.
%% @spec now_diff_milliseconds(now(), now()) -> integer()
now_diff_milliseconds(undefined, undefined) ->
  0;
now_diff_milliseconds(undefined, T2) ->
  now_diff_milliseconds(os:timestamp(), T2);
now_diff_milliseconds({M,S,U}, {M,S1,U1}) ->
  ((S-S1) * 1000) + ((U-U1) div 1000);
now_diff_milliseconds({M,S,U}, {M1,S1,U1}) ->
  ((M-M1)*1000000+(S-S1))*1000 + ((U-U1) div 1000).

current () ->
  {{Year, Month, Day},{Hour,Minute,_}} = now_to_mdyhms (os:timestamp()),
  EpochStartSeconds =
    ct_expand:term (
      calendar:datetime_to_gregorian_seconds({{1970,1,1},{0,0,0}})
    ),
  NowSeconds =
    calendar:datetime_to_gregorian_seconds (
      {{Year, Month, Day},{Hour, Minute, 0}}),
  (NowSeconds - EpochStartSeconds) * 1000.

current_minute () ->
  {_, Minute, _} = time (),
  Minute.

millis_to_next_round_second () ->
  millis_to_next_round_second (os:timestamp()).
millis_to_next_round_second (Ts) ->
  SecsSinceEpoch = now_to_epoch_secs (Ts),
  MillisSinceEpoch = now_to_epoch_millis (Ts),
  NextMillisSec = (SecsSinceEpoch + 1) * ?KILO,
  NextMillisSec - MillisSinceEpoch.

millis_to_next_round_minute () ->
  millis_to_next_round_minute (os:timestamp()).
millis_to_next_round_minute (Ts) ->
  NextMinuteSinceEpochAsMillis = (now_to_epoch_minutes (Ts) + 1) * 60000,
  MillisSinceEpoch = now_to_epoch_millis (Ts),
  NextMinuteSinceEpochAsMillis - MillisSinceEpoch.

binaryify (undefined, Default) when is_binary(Default) ->
  Default;
binaryify (B, _) ->
  binaryify (B).

binaryify (B) when is_binary (B) ->
  B;
binaryify (O) ->
  list_to_binary (stringify (O)).

binaryify_context (Context) ->
  [ {binaryify (K), binaryify (V)} || {K,V} <- Context].

stringify (undefined, Default) when is_list(Default) ->
  Default;
stringify (V, _) ->
  stringify(V).

stringify (I) when is_integer (I) ->
  integer_to_list (I);
stringify (F) when is_float (F) ->
  float_to_list (F);
stringify (A) when is_atom (A) ->
  atom_to_list (A);
stringify (L) ->
  L.

integerify ("") -> undefined;
integerify (<<>>) -> undefined;
integerify (I) when is_integer (I) ->
  I;
integerify (F) when is_float (F) ->
  trunc (F);
integerify (B) when is_binary (B) ->
  integerify (binary_to_list (B));
integerify (L) when is_list (L) ->
  try list_to_integer (L) of
    I -> I
  catch
    _:_ -> undefined
  end.

floatify ("") -> undefined;
floatify (<<>>) -> undefined;
floatify (I) when is_integer (I) ->
  I + 0.0;
floatify (F) when is_float (F) ->
  F;
floatify (B) when is_binary (B) ->
  floatify (binary_to_list (B));
floatify (L) when is_list (L) ->
  try list_to_float (L) of
    F -> F
  catch
    _:_ ->
      case integerify (L) of
        undefined -> undefined;
        I -> I + 0.0
      end
  end.

find_in_dict (Key, Dict) ->
  find_in_dict (Key, Dict, undefined).

find_in_dict (Key, Dict, Default) ->
  case dict:find (Key, Dict) of
    error -> Default;
    {ok, T} -> T
  end.

join (L,S) when is_list (L) ->
  lists:reverse (join (L, S, [])).

join ([], _, A) ->
  A;
join ([H], _, []) ->
  [H];
join ([H], S, A) ->
  [H,S|A];
join ([H|T], S, []) ->
  join (T,S,[H]);
join ([H|T], S, A) ->
  join (T,S,[H,S|A]).

first_defined(L) when is_list(L) ->
  first_defined0(L).

first_defined0([undefined|R]) ->
  first_defined0(R);
first_defined0([D|_]) ->
  D;
first_defined0([]) ->
  undefined.

normalize_ip (undefined) -> undefined;
normalize_ip (IP = {_,_,_,_}) ->
  IP;
normalize_ip (L) when is_list (L) ->
  case inet_parse:address (L) of
    {ok, IP} -> IP;
    _ -> {0,0,0,0}
  end.

listen (Config) ->
  {ok, L} = lwes:open (listener, Config),
  lwes:listen (L,
               fun (E, S) ->
                 Stats = mondemand_event:from_udp (E),
                 io:format ("~p~n",[Stats]),
                 S
               end,
               raw,
               ok).

dummy () ->
  N = rand:uniform (100),
  [ mondemand:add_sample(foo,stuff,I)
   || I <- lists:seq (1,N)
  ],
  mondemand:increment(foo,bar),
  mondemand:set(foo,blah,50).

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

%% Copied from webmachine, see NOTE above
now_diff_milliseconds_test() ->
  Late = {10, 10, 10},
  Early1 = {10, 9, 9},
  Early2 = {9, 9, 9},
  ?assertEqual(1000, now_diff_milliseconds(Late, Early1)),
  ?assertEqual(1000001000, now_diff_milliseconds(Late, Early2)).

stringify_test_ () ->
  [
     ?_assertEqual (E, stringify(G))
     || {G, E}
     <- [
          {1, "1"},
          {1.5, "1.50000000000000000000e+00"},
          {a, "a"},
          {"a", "a"},
          {<<"a">>, <<"a">>}
        ]
  ].

binaryify_test_ () ->
  [
     ?_assertEqual (E, binaryify(G))
     || {G, E}
     <- [
          {1, <<"1">>},
          {1.5, <<"1.50000000000000000000e+00">>},
          {a, <<"a">>},
          {"a", <<"a">>},
          {<<"a">>, <<"a">>}
        ]
  ].

integerify_test_ () ->
  [
     ?_assertEqual (E, integerify(G))
     || {G, E}
     <- [
          {"", undefined},
          {<<>>, undefined},
          {1, 1},
          {1.2, 1},
          {"1", 1},
          {<<"1">>, 1},
          {"a", undefined}
        ]
  ].

floatify_test_ () ->
  [
     ?_assertEqual (E, floatify(G))
     || {G, E}
     <- [
          {"", undefined},
          {<<>>, undefined},
          {1,        1.0},
          {1.2,      1.2},
          {<<"1.2">>,1.2},
          {"1.2",    1.2},
          {<<"1">>,  1.0},
          {"a",      undefined}
        ]
  ].

-endif.
