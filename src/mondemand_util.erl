-module (mondemand_util).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

% lwes helper functions
-export ([ context_from_lwes/1,
           context_to_lwes/3
         ]).

% util functions
-export ([ find_in_dict/2,
           find_in_dict/3,
           key_in_dict/2,
           key_in_list/2,
           binaryify/1,
           stringify/1,
           integerify/1,
           floatify/1,
           join/2
         ]).
%% Time functions
-export ([
          micros_since_epoch/0,
          millis_since_epoch/0,
          seconds_since_epoch/0,
          minutes_since_epoch/0,
          micros_epoch_to_erlang_now/1, % (MicrosTimeSinceEpoch) -> {_,_,_}
          now_to_mdyhms/1,              % ({_,_,_}) -> {{_,_,_},{_,_,_}}
          now_to_epoch_millis/1,        % ({_,_,_}) -> MillisSinceEpoch
          now_to_epoch_secs/1,          % ({_,_,_}) -> SecondsSinceEpoch
          now_to_epoch_minutes/1,       % ({_,_,_}) -> MinutesSinceEpoch
          current_minute/0,
          millis_to_next_round_second/0,
          millis_to_next_round_second/1,
          millis_to_next_round_minute/0,
          millis_to_next_round_minute/1
         ]).

%% Other functions
-export ([ listen/1,
           dummy/0 ]).

context_from_lwes (Data) ->
  Num =
    case dict:find (?MD_CTXT_NUM, Data) of
      error -> 0;
      {ok, C} -> C
    end,
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
  [
    { ?LWES_U_INT_16, ?MD_CTXT_NUM, NumContexts + 1},
    lists:zipwith (fun context_to_lwes/2,
                   lists:seq (1, NumContexts),
                   Context),
    context_to_lwes (NumContexts+1, { ?MD_HOST, Host })
  ].

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

binaryify (B) when is_binary (B) ->
  B;
binaryify (O) ->
  list_to_binary (stringify (O)).

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

key_in_dict (Key, Dict) when is_list (Key) ->
  dict:is_key (Key, Dict)
    orelse dict:is_key (list_to_binary(Key), Dict)
    orelse dict:is_key (list_to_atom(Key), Dict).

key_in_list (Key, List) when is_list (Key), is_list (List) ->
  proplists:is_defined (Key, List)
   orelse proplists:is_defined (list_to_binary(Key), List)
   orelse proplists:is_defined (list_to_atom(Key), List).


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

listen (Config) ->
  {ok, L} = lwes:open (listener, Config),
  lwes:listen (L,
               fun (E, S) ->
                 Stats = mondemand_stats:from_lwes (E),
                 io:format ("~p~n",[Stats]),
                 S
               end,
               dict,
               ok).

dummy () ->
  [ mondemand:add_sample(foo,stuff,I)
   || I <- lists:seq (1,20)
  ],
  mondemand:increment(foo,bar),
  mondemand:set(foo,blah,50).

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").


-endif.
