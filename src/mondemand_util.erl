-module (mondemand_util).

-include ("mondemand_internal.hrl").

-compile({parse_transform, ct_expand}).

% metric functions
-export ([ stringify/1,
           key_in_dict/2,
           key_in_list/2,
           metric_name_key/1,
           metric_value_key/1,
           metric_type_key/1,
           context_name_key/1,
           context_value_key/1,
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

stringify (I) when is_integer (I) ->
  integer_to_list (I);
stringify (F) when is_float (F) ->
  float_to_list (F);
stringify (A) when is_atom (A) ->
  atom_to_list (A);
stringify (L) ->
  L.

key_in_dict (Key, Dict) when is_list (Key) ->
  dict:is_key (Key, Dict)
    orelse dict:is_key (list_to_binary(Key), Dict)
    orelse dict:is_key (list_to_atom(Key), Dict).

key_in_list (Key, List) when is_list (Key), is_list (List) ->
  proplists:is_defined (Key, List)
   orelse proplists:is_defined (list_to_binary(Key), List)
   orelse proplists:is_defined (list_to_atom(Key), List).

% generate lookup tables for lwes keys so save some time in production
-define (ELEMENT_OF_TUPLE_LIST(N,Prefix),
         element (N,
                  ct_expand:term (
                    begin
                      list_to_tuple (
                        [
                          list_to_binary (
                            lists:concat ([Prefix, integer_to_list(E-1)])
                          )
                          || E <- lists:seq(1,1024)
                        ]
                      )
                    end))).

metric_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_K).

metric_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_V).

metric_type_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_T).

context_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_K).

context_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_V).

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

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").


-endif.
