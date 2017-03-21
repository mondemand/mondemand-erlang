-module (mondemand_perfmsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ new/3,        % (Id, CallerLabel, [{Label, Start, End}])
           new/4,        % (Id, CallerLabel, [{Label, Start, End}], [{Key, Value}])
           new/5,        % (Id, CallerLabel, Label, Start, End)
           new/6,        % (Id, CallerLabel, Label, Start, End, [{Key, Value}])
           new_timing/3, % (Label, Start, End)
           id/1,
           caller_label/1,
           timings/1,
           num_timings/1,
           timing_label/1,
           timing_start_time/1,
           timing_end_time/1,
           context/1,
           context_value/2,
           add_contexts/2,
           add_context/3,
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

id (#md_perf_msg { id = Id }) -> Id.
caller_label (#md_perf_msg { caller_label = Caller }) -> Caller.
timings (#md_perf_msg { timings = Timings }) -> Timings.
num_timings (#md_perf_msg { num_timings = NumTimings }) -> NumTimings.

timing_label (#md_perf_timing { label = Label }) -> Label.
timing_start_time (#md_perf_timing { start_time = StartTime }) -> StartTime.
timing_end_time (#md_perf_timing { end_time = StartTime }) -> StartTime.

context (#md_perf_msg { context = Context }) -> Context.
context_value (#md_perf_msg { context = Context }, ContextKey) ->
  context_find (ContextKey, Context, undefined).

context_find (Key, Context, Default) ->
  case lists:keyfind (Key, 1, Context) of
    false -> Default;
    {_, H} -> H
  end.

add_contexts (P = #md_perf_msg { num_context = ContextNum,
                                  context = Context},
              L) when is_list (L) ->
  P#md_perf_msg { num_context = ContextNum + length (L),
                  context = L ++ Context }.

add_context (P = #md_perf_msg { num_context = ContextNum,
                                context = Context},
             ContextKey, ContextValue) ->
  P#md_perf_msg { num_context = ContextNum + 1,
                  context = [ {ContextKey, ContextValue} | Context ] }.

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


% deserializing was originally done by using the dict form of lwes deserializing
% and lots of dict:fetch/2 calls.  This proved to be very slow mostly because
% of dict calls.  So I switched to deserializing as a list, and walking the list
% once.  This requires some sorting and zipping afterwards but is about 5x
% faster.  When walking the list, I originally used the accum record below
% but that proved to be slow because of erlang:setelement/2, so I expanded
% out all the accumulators so that process has 11 args, but it's faster
% still.
-record(accum, { receipt_time,
                 id,
                 caller_label,
                 timing_num = 0,
                 labels = [],
                 starts = [],
                 ends = [],
                 context_num = 0,
                 context_keys = [],
                 context_vals = []
               }).

from_lwes (#lwes_event { attrs = Data}) ->
  #accum { id = Id,
           receipt_time = ReceiptTime,
           caller_label = CallerLabel,
           timing_num = TimingNum,
           labels = Labels,
           starts = Starts,
           ends = Ends,
           context_num = ContextNum,
           context_keys = CKeys,
           context_vals = CVals
         } = process (Data, undefined, 0, undefined, 0, [], [], [],
                       0, [], []),
  TimingsOut =
    lists:zipwith3 (fun ({K,Label},{K,Start},{K,End}) ->
                      #md_perf_timing {label = Label,
                                       start_time = Start,
                                       end_time = End}
                    end,
                    lists:sort(Labels),
                    lists:sort(Starts),
                    lists:sort(Ends)),
  TimingNum = length (TimingsOut),
  ContextsOut =
    lists:zipwith (fun ({K, Key},{K, Val}) ->
                     {Key, Val}
                   end,
                   lists:sort(CKeys),
                   lists:sort(CVals)),
  ContextNum = length (ContextsOut),
  { ReceiptTime,
    #md_perf_msg { id = Id,
                   caller_label = CallerLabel,
                   num_context = ContextNum,
                   context = lists:sort(ContextsOut),
                   num_timings = TimingNum,
                   timings = lists:sort(TimingsOut)
                 }
  }.

process ([], Id, ReceiptTime, CallerLabel,
              TimingNum, Labels, Starts, Ends,
              ContextNum, CKeys, CVals ) ->
  #accum { id = Id,
           receipt_time = ReceiptTime,
           caller_label = CallerLabel,
           timing_num = TimingNum,
           labels = Labels,
           starts = Starts,
           ends = Ends,
           context_num = ContextNum,
           context_keys = CKeys,
           context_vals = CVals
         };
process ([{?MD_PERF_ID,I} | Rest ],
          _, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            I, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process ([{?MD_RECEIPT_TIME,R} | Rest ],
          Id, _, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, R, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process ([{?MD_PERF_CALLER_LABEL,C} | Rest ],
          Id, ReceiptTime, _,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, C,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process ([{?MD_NUM,N} | Rest ],
          Id, ReceiptTime, CallerLabel,
          _, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, CallerLabel,
            N, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process ([{<<"label",N/binary>>,Label} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, [ {N,Label} | Labels ], Starts, Ends,
            ContextNum, CKeys, CVals);
process ([{<<"start",N/binary>>,Start} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, [ {N,Start} | Starts], Ends,
            ContextNum, CKeys, CVals);
process ([{<<"end",N/binary>>,End} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, [ {N,End} | Ends ],
            ContextNum, CKeys, CVals);
process ([{?MD_CTXT_NUM,N} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          _, CKeys, CVals) ->
  process (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            N, CKeys, CVals);
process ([{<<"ctxt_k",N/binary>>,Key} | Rest ],
         Id, ReceiptTime, CallerLabel,
         TimingNum, Labels, Starts, Ends,
         ContextNum, CKeys, CVals) ->
  process (Rest,
           Id, ReceiptTime, CallerLabel,
           TimingNum, Labels, Starts, Ends,
           ContextNum, [ {N,Key} | CKeys ], CVals);
process ([{<<"ctxt_v",N/binary>>,Val} | Rest ],
         Id, ReceiptTime, CallerLabel,
         TimingNum, Labels, Starts, Ends,
         ContextNum, CKeys, CVals) ->
  process (Rest,
           Id, ReceiptTime, CallerLabel,
           TimingNum, Labels, Starts, Ends,
           ContextNum, CKeys, [ {N,Val} | CVals ]);
process ([_|R], Id, ReceiptTime, CallerLabel,
              TimingNum, Labels, Starts, Ends,
              ContextNum, CKeys, CVals) ->
  % skip unrecognized
  process (R, Id, ReceiptTime, CallerLabel,
              TimingNum, Labels, Starts, Ends,
              ContextNum, CKeys, CVals).

perf_label_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_LABEL).

perf_start_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_START).

perf_end_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_END).

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

perfmsg_test_ () ->
  [
    { "basic constructor test",
      fun () ->
        P = new (<<"foo">>, <<"bar">>, [{<<"baz">>,5,10}]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (5, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([],context (P)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "basic constructor multi-test",
      fun () ->
        P = new (<<"foo">>, <<"bar">>, [{<<"baz">>,5,10},{<<"bob">>,6,8}]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (2, num_timings(P)),
        [ T1, T2 ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T1)),
        ?assertEqual (5, timing_start_time (T1)),
        ?assertEqual (10, timing_end_time (T1)),
        ?assertEqual (<<"bob">>,timing_label (T2)),
        ?assertEqual (6, timing_start_time (T2)),
        ?assertEqual (8, timing_end_time (T2)),
        ?assertEqual ([],context (P)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "basic constructor with constructor timings",
      fun () ->
        P = new (<<"foo">>, <<"bar">>, [new_timing(<<"baz">>,5,10)]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (5, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([],context (P)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "expanded args constructor",
      fun () ->
        P = new (<<"foo">>, <<"bar">>, <<"baz">>,5,10),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (5, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([],context (P)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "constructor with context",
      fun () ->
        P = new (<<"foo">>, <<"bar">>,
                 [{<<"baz">>,5,10}],
                 [{<<"cat">>,<<"bird">>}]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (5, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([{<<"cat">>,<<"bird">>}], context (P)),
        ?assertEqual (<<"bird">>,context_value (P, <<"cat">>)),
        ?assertEqual (undefined, context_value (P, <<"dog">>)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "constructor with expanded args with context",
      fun () ->
        P = new (<<"foo">>, <<"bar">>, <<"baz">>,5, 10, [{<<"cat">>,<<"bird">>}]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (5, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([{<<"cat">>,<<"bird">>}], context (P)),
        ?assertEqual (<<"bird">>,context_value (P, <<"cat">>)),
        ?assertEqual (undefined, context_value (P, <<"dog">>)),
        ?assertEqual ({0, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(P)),list)))
      end
    },
    { "add receipttime and others for full coverage",
      fun () ->
        % cheating a bit here, for coverage, captured the output of os:timestamp
        % and use that as the start time
        P = new (<<"foo">>, <<"bar">>,
                 [{<<"baz">>,{1456,534959,536878},10}],
                 [{<<"cat">>,<<"bird">>}]),
        ?assertEqual (<<"foo">>, id(P)),
        ?assertEqual (<<"bar">>,caller_label(P)),
        ?assertEqual (1, num_timings(P)),
        [ T ] = timings(P),
        ?assertEqual (<<"baz">>,timing_label (T)),
        ?assertEqual (1456534959536, timing_start_time (T)),
        ?assertEqual (10, timing_end_time (T)),
        ?assertEqual ([{<<"cat">>,<<"bird">>}], context (P)),
        ?assertEqual (<<"bird">>,context_value (P, <<"cat">>)),
        ?assertEqual (undefined, context_value (P, <<"dog">>)),
        [Event] = to_lwes([P]), % cheating here by 'testing' the list version
        NewEvent = Event#lwes_event { attrs = [ {?LWES_INT_64, ?MD_RECEIPT_TIME, 5}, {?LWES_U_INT_16, ?MD_SENDER_PORT, 10} | Event#lwes_event.attrs ]},
        ?assertEqual ({5, P},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(NewEvent),list)))
      end
    }
  ].

-endif.
