-module (mondemand_perfmsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ new/3,        % (Id, CallerLabel, [{Label, Start, End}])
           new/4,        % (Id, CallerLabel, [{Label, Start, End}], [{Key, Value}])
           new/5,        % (Id, CallerLabel, Label, Start, End)
           new/6,        % (Id, CallerLabel, Label, Start, End, [{Key, Value}])
           new_timing/3, % (Label, Start, End)
           to_lwes/1,
           from_lwes/1
         ]).

new (Id, CallerLabel, Timings) ->
  new (Id, CallerLabel, Timings, []).

new (Id, CallerLabel, Timings, Context) ->
  ValidatedTimings =
    lists:map (fun (T = #md_perf_timing {}) -> T;
                   ({L, S, E}) -> new_timing (L, S, E)
               end,
               Timings),
  ValidatedContext = mondemand_util:binaryify_context (Context),
  #md_perf_msg { id = Id,
                 caller_label = CallerLabel,
                 num_timings = length (ValidatedTimings),
                 timings = ValidatedTimings,
                 num_context = length (ValidatedContext),
                 context = ValidatedContext
               }.

new (Id, CallerLabel, Label, Start, End) ->
  new (Id, CallerLabel, [new_timing (Label, Start, End)], []).
new (Id, CallerLabel, Label, Start, End, Context) ->
  new (Id, CallerLabel, [new_timing (Label, Start, End)], Context).

new_timing (Label, Start, End) ->
  #md_perf_timing { label = Label,
                    start_time = validate_time (Start),
                    end_time = validate_time (End)
                  }.

validate_time (Timestamp = {_,_,_}) ->
  mondemand_util:now_to_epoch_millis (Timestamp);
validate_time (Timestamp) when is_integer (Timestamp) ->
  Timestamp.

timings_from_lwes (Data) ->
  Num = mondemand_util:find_in_dict (?MD_NUM, Data, 0),
  { Num,
    lists:map (fun(N) ->
                 timing_from_lwes (N, Data)
               end,
               lists:seq (1, Num))
  }.

timing_from_lwes (TimingIndex, Data) ->
  L = dict:fetch (perf_label_key (TimingIndex), Data),
  S = dict:fetch (perf_start_key (TimingIndex), Data),
  E = dict:fetch (perf_end_key (TimingIndex), Data),
  #md_perf_timing { label = L, start_time = S, end_time = E }.

timings_to_lwes (NumTimings, Timings) ->
  [ { ?LWES_U_INT_16, ?MD_NUM, NumTimings }
    | lists:zipwith (fun timing_to_lwes/2,
                     lists:seq (1, NumTimings),
                     Timings)].

timing_to_lwes (TimingIndex,
                #md_perf_timing { label = L, start_time = S, end_time = E }) ->
  [ { ?LWES_STRING, perf_label_key (TimingIndex), L },
    { ?LWES_INT_64, perf_start_key (TimingIndex), S },
    { ?LWES_INT_64, perf_end_key (TimingIndex), E } ].

to_lwes (L) when is_list (L) ->
  lists:map (fun to_lwes/1, L);
to_lwes (#md_perf_msg { id = Id,
                        caller_label = CallerLabel,
                        num_timings = NumTimings,
                        timings = Timings,
                        num_context = NumContexts,
                        context = Context
                      }) ->
  #lwes_event {
    name = ?MD_PERF_EVENT,
    attrs = lists:flatten ([ { ?LWES_STRING, ?MD_PERF_ID, Id },
                             { ?LWES_STRING, ?MD_PERF_CALLER_LABEL, CallerLabel},
                             timings_to_lwes (NumTimings, Timings),
                             mondemand_util:context_to_lwes (undefined,
                                                             NumContexts,
                                                             Context)
                           ])
  }.

from_lwes (#lwes_event { attrs = Data}) ->
  {NumTimings, Timings} = timings_from_lwes (Data),
  {_, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  #md_perf_msg { id = dict:fetch (?MD_PERF_ID, Data),
                 caller_label = dict:fetch (?MD_PERF_CALLER_LABEL, Data),
                 num_context = NumContexts,
                 context = Context,
                 num_timings = NumTimings,
                 timings = Timings
               }.

perf_label_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_LABEL).

perf_start_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_START).

perf_end_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_END).
