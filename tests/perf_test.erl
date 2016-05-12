-module(perf_test).

-include_lib ("lwes/include/lwes.hrl").

-export([
          dict_based/1,
          list_based/1,
          list2_based/1
        ]).
-include ("perf_common.hrl").

functions () ->
  [
    {dict, fun dict_based/1},
    {list, fun list_based/1},
    {list2, fun list2_based/1}
  ].

compare (U) ->
  D =normalize(dict_based (U)),
  L1=normalize(list_based (U)),
  L2=normalize(list2_based (U)),
  case D =:= L1 andalso D =:= L2 of
    true -> true;
    false ->
      io:format ("Dict    : ~p~n",[D]),
      io:format ("List    : ~p~n",[L1]),
      io:format ("List2   : ~p~n",[L2]),
      D =:= L1 andalso D =:= L2
  end.

normalize (
  #md_event { sender_ip = SenderIp,
              sender_port = SenderPort,
              receipt_time = ReceiptTime,
              name = Name,
              msg = #md_perf_msg { id = Id,
                      caller_label = CallerLabel,
                      num_context = ContextNum,
                      context = ContextsOut,
                      num_timings = TimingNum,
                      timings = TimingsOut
                    }
            }) ->
  #md_event { sender_ip = SenderIp,
              sender_port = SenderPort,
              receipt_time = ReceiptTime,
              name = Name,
              msg = #md_perf_msg { id = Id,
                      caller_label = CallerLabel,
                      num_context = ContextNum,
                      context = lists:sort(ContextsOut),
                      num_timings = TimingNum,
                      timings = lists:sort(TimingsOut)
                    }
  }.


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

timings_from_lwes (Data) ->
  Num = mondemand_util:find_in_dict (?MD_NUM, Data, 0),
  { Num,
    lists:map (fun(N) ->
                 timing_from_lwes (N, Data)
               end,
               lists:seq (1, Num))
  }.

perf_label_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_LABEL).

perf_start_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_START).

perf_end_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_PERF_END).

timing_from_lwes (TimingIndex, Data) ->
  L = dict:fetch (perf_label_key (TimingIndex), Data),
  S = dict:fetch (perf_start_key (TimingIndex), Data),
  E = dict:fetch (perf_end_key (TimingIndex), Data),
  #md_perf_timing { label = L, start_time = S, end_time = E }.

dict_based (Packet = {udp, _, SenderIp, SenderPort, _}) ->
  Name = lwes_event:peek_name_from_udp (Packet),
  #lwes_event { attrs = Data } = lwes_event:from_udp_packet (Packet, dict),
  ReceiptTime = dict:fetch (?MD_RECEIPT_TIME, Data),
  {NumTimings, Timings} = timings_from_lwes (Data),
  {_, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  Msg = #md_perf_msg { id = dict:fetch (?MD_PERF_ID, Data),
                       caller_label = dict:fetch (?MD_PERF_CALLER_LABEL, Data),
                       num_context = NumContexts,
                       context = Context,
                       num_timings = NumTimings,
                       timings = Timings
                     },
  #md_event { sender_ip = SenderIp,
              sender_port = SenderPort,
              receipt_time = ReceiptTime,
              name = Name,
              msg = Msg }.

list2_based (Packet = {udp, _, SenderIp, SenderPort, _}) ->
  Name = lwes_event:peek_name_from_udp (Packet),
  #lwes_event { attrs = Data } = lwes_event:from_udp_packet (Packet, list),
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
         } = process2 (Data, undefined, 0, undefined, 0, [], [], [],
                       0, [], []),
  TimingsOut =
    lists:zipwith3 (fun ({K,Label},{K,Start},{K,End}) ->
                      {md_perf_timing, Label, Start, End}
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

  #md_event { sender_ip = SenderIp,
              sender_port = SenderPort,
              receipt_time = ReceiptTime,
              name = Name,
              msg = #md_perf_msg { id = Id,
                      caller_label = CallerLabel,
                      num_context = ContextNum,
                      context = ContextsOut,
                      num_timings = TimingNum,
                      timings = TimingsOut
                    }
  }.

process2 ([], Id, ReceiptTime, CallerLabel,
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
process2 ([{<<"id">>,I} | Rest ],
          _, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            I, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"ReceiptTime">>,R} | Rest ],
          Id, _, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, R, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"caller_label">>,C} | Rest ],
          Id, ReceiptTime, _,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, C,
            TimingNum, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"num">>,N} | Rest ],
          Id, ReceiptTime, CallerLabel,
          _, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, CallerLabel,
            N, Labels, Starts, Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"label",N/binary>>,Label} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, [ {N,Label} | Labels ], Starts, Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"start",N/binary>>,Start} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, [ {N,Start} | Starts], Ends,
            ContextNum, CKeys, CVals);
process2 ([{<<"end",N/binary>>,End} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          ContextNum, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, [ {N,End} | Ends ],
            ContextNum, CKeys, CVals);
process2 ([{<<"ctxt_num">>,N} | Rest ],
          Id, ReceiptTime, CallerLabel,
          TimingNum, Labels, Starts, Ends,
          _, CKeys, CVals) ->
  process2 (Rest,
            Id, ReceiptTime, CallerLabel,
            TimingNum, Labels, Starts, Ends,
            N, CKeys, CVals);
process2 ([{<<"ctxt_k",N/binary>>,Key} | Rest ],
         Id, ReceiptTime, CallerLabel,
         TimingNum, Labels, Starts, Ends,
         ContextNum, CKeys, CVals) ->
  process2 (Rest,
           Id, ReceiptTime, CallerLabel,
           TimingNum, Labels, Starts, Ends,
           ContextNum, [ {N,Key} | CKeys ], CVals);
process2 ([{<<"ctxt_v",N/binary>>,Val} | Rest ],
         Id, ReceiptTime, CallerLabel,
         TimingNum, Labels, Starts, Ends,
         ContextNum, CKeys, CVals) ->
  process2 (Rest,
           Id, ReceiptTime, CallerLabel,
           TimingNum, Labels, Starts, Ends,
           ContextNum, CKeys, [ {N,Val} | CVals ]);
process2 ([_|R], Id, ReceiptTime, CallerLabel,
              TimingNum, Labels, Starts, Ends,
              ContextNum, CKeys, CVals) ->
  process2 (R, Id, ReceiptTime, CallerLabel,
              TimingNum, Labels, Starts, Ends,
              ContextNum, CKeys, CVals).

list_based (Packet = {udp, _, SenderIp, SenderPort, _}) ->
  Name = lwes_event:peek_name_from_udp (Packet),
  #lwes_event { attrs = Data } = lwes_event:from_udp_packet (Packet, list),
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
         } = process (Data, #accum{}),
  TimingsOut =
    lists:zipwith3 (fun ({K,Label},{K,Start},{K,End}) ->
                      {md_perf_timing, Label, Start, End}
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

  #md_event { sender_ip = SenderIp,
              sender_port = SenderPort,
              receipt_time = ReceiptTime,
              name = Name,
              msg = #md_perf_msg { id = Id,
                      caller_label = CallerLabel,
                      num_context = ContextNum,
                      context = ContextsOut,
                      num_timings = TimingNum,
                      timings = TimingsOut
                    }
  }.

process ([], A) -> A;
process ([{<<"id">>,I} | Rest ], A) ->
  process (Rest, A#accum { id = I });
process ([{<<"ReceiptTime">>,R} | Rest ], A) ->
  process (Rest, A#accum { receipt_time = R });
process ([{<<"caller_label">>,C} | Rest ], A) ->
  process (Rest, A#accum { caller_label = C });
process ([{<<"num">>,N} | Rest ], A) ->
  process (Rest, A#accum { timing_num = N });
process ([{<<"label",N/binary>>,Label} | Rest ], A = #accum {labels = L}) ->
  process (Rest, A#accum { labels = [ {N,Label} | L ]});
process ([{<<"start",N/binary>>,Start} | Rest ], A = #accum {starts = S}) ->
  process (Rest, A#accum { starts = [ {N,Start} | S ]});
process ([{<<"end",N/binary>>,End} | Rest ], A = #accum {ends = E}) ->
  process (Rest, A#accum { ends = [ {N,End} | E ]});
process ([{<<"ctxt_num">>,N} | Rest ], A) ->
  process (Rest, A#accum { context_num = N });
process ([{<<"ctxt_k",N/binary>>,Key} | Rest ], A = #accum {context_keys = C}) ->
  process (Rest, A#accum { context_keys = [ {N,Key} | C ]});
process ([{<<"ctxt_v",N/binary>>,Val} | Rest ], A = #accum {context_vals = C}) ->
  process (Rest, A#accum { context_vals = [ {N,Val} | C ]});
process ([_|R], A) ->
  process (R, A).


