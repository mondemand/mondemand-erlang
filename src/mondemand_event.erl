-module (mondemand_event).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([new/2,
          new/5,
          receipt_time/1,
          sender_ip/1,
          sender_port/1,
          name/1,
          msg/1,
          set_msg/2,
          peek_name_from_udp/1,
          peek_type_from_udp/1,
          to_lwes/1,
          to_udp/1,
          from_udp/1
         ]).

new (Name, Msg) ->
  #md_event { name = Name,
              msg = Msg }.
new (SenderIp, SenderPort, ReceiptTime, Name, Msg) ->
  #md_event { receipt_time = ReceiptTime,
              sender_ip = mondemand_util:normalize_ip (SenderIp),
              sender_port = SenderPort,
              name = Name,
              msg = Msg
            }.

receipt_time (#md_event { receipt_time = ReceiptTime }) -> ReceiptTime.
sender_ip (#md_event { sender_ip = SenderIP }) -> SenderIP.
sender_port (#md_event { sender_port = SenderPort }) -> SenderPort.
name (#md_event { name = Name }) -> Name.

msg (#md_event { msg = Msg }) -> Msg.
set_msg (E = #md_event {}, Msg) -> E#md_event {msg = Msg}.

peek_name_from_udp  (Event = {udp, _, _, _, _}) ->
  lwes_event:peek_name_from_udp (Event);
peek_name_from_udp (#md_event { name = Name }) ->
  Name.

peek_type_from_udp (Event = {udp, _, _, _, _}) ->
  name_to_type (lwes_event:peek_name_from_udp (Event));
peek_type_from_udp (#md_event { name = Name }) ->
  name_to_type (Name).

to_udp (UDP = {udp, _, _, _, _}) -> UDP;
to_udp (#md_event { sender_port = SenderPort,
                    sender_ip = SenderIP,
                    receipt_time = ReceiptTime,
                    name = Name,
                    msg = Msg }) ->
  case Name of
    ?MD_TRACE_EVENT -> ok;
    ?MD_STATS_EVENT -> {udp,
                        ReceiptTime,
                        mondemand_util:normalize_ip (SenderIP),
                        SenderPort,
                        lwes_event:to_binary (mondemand_statsmsg:to_lwes (Msg))
                       };
    ?MD_LOG_EVENT -> {udp,
                      ReceiptTime,
                      mondemand_util:normalize_ip (SenderIP),
                      SenderPort,
                      lwes_event:to_binary (mondemand_logmsg:to_lwes (Msg))
                     };
    ?MD_PERF_EVENT -> {udp,
                       ReceiptTime,
                       mondemand_util:normalize_ip (SenderIP),
                       SenderPort,
                       lwes_event:to_binary (mondemand_perfmsg:to_lwes (Msg))
                      };
    ?MD_ANNOTATION_EVENT -> {udp,
                             ReceiptTime,
                             mondemand_util:normalize_ip (SenderIP),
                             SenderPort,
                             lwes_event:to_binary (mondemand_annotationmsg:to_lwes (Msg))
                      }
  end.

from_udp (Event = #md_event {}) ->
  Event;
from_udp ({udp, _, _, _, Event = #md_event {}}) ->
  Event;
from_udp (Packet = {udp, _, SenderIp, SenderPort, _}) ->
  case lwes_event:peek_name_from_udp (Packet) of
    ?MD_TRACE_EVENT ->
      % TODO get receipt time from json_eep18 decoded lib.
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  name = ?MD_TRACE_EVENT,
                  msg = lwes_event:from_udp_packet (Packet, json_eep18) };
    ?MD_PERF_EVENT ->
      Event = lwes_event:from_udp_packet (Packet, list),
      {ReceiptTime, Msg} = mondemand_perfmsg:from_lwes (Event),
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  receipt_time = ReceiptTime,
                  name = ?MD_PERF_EVENT,
                  msg = Msg };
    ?MD_STATS_EVENT ->
      Event = lwes_event:from_udp_packet (Packet, dict),
      {ReceiptTime, Msg} = mondemand_statsmsg:from_lwes (Event),
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  receipt_time = ReceiptTime,
                  name = ?MD_STATS_EVENT,
                  msg = Msg };
    Name when Name =:= ?MD_LOG_EVENT;
              Name =:= ?MD_ANNOTATION_EVENT ->
      % deserialize the event as a dictionary
      Event = lwes_event:from_udp_packet (Packet, dict),
      {ReceiptTime, Msg} =
        case Name of
          ?MD_LOG_EVENT -> mondemand_logmsg:from_lwes (Event);
          ?MD_ANNOTATION_EVENT -> mondemand_annotationmsg:from_lwes (Event)
        end,
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  receipt_time = ReceiptTime,
                  name = Name,
                  msg = Msg };
    { error, _ } ->
      error_logger:error_msg ("Malformed Event ~p",[Packet]),
      undefined;
    Other ->
      error_logger:error_msg ("Unrecognized Event ~p : ~p",
                              [Other,
                               lwes_event:from_udp_packet (Packet, list)]),
      undefined
  end.

to_lwes (#md_event { name = Name, msg = Msg }) ->
  case Name of
    ?MD_TRACE_EVENT -> ok;
    ?MD_STATS_EVENT -> mondemand_statsmsg:to_lwes (Msg);
    ?MD_LOG_EVENT -> mondemand_logmsg:to_lwes (Msg);
    ?MD_PERF_EVENT -> mondemand_perfmsg:to_lwes (Msg);
    ?MD_ANNOTATION_EVENT -> mondemand_annotationmsg:to_lwes (Msg)
  end.

name_to_type (?MD_ANNOTATION_EVENT) -> annotation_msg;
name_to_type (?MD_STATS_EVENT) -> stats_msg;
name_to_type (?MD_LOG_EVENT) -> log_msg;
name_to_type (?MD_TRACE_EVENT) -> trace_msg;
name_to_type (?MD_PERF_EVENT) -> perf_msg;
name_to_type (_) -> undefined.

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

mondemand_event_test_ () ->
  [
    { "basic codec test (annotation)",
      fun() ->
        % simple tests for annotation events
        EA = new ({127,0,0,1}, 2000, 5, ?MD_ANNOTATION_EVENT,
                 mondemand_annotationmsg:new (<<"id">>,5,
                                              <<"description">>,
                                              <<"text">>,[<<"foo">>,<<"bar">>],
                                              [{<<"baz">>,<<"bob">>}])),
        UA = to_udp (EA),
        ?assertEqual (EA, from_udp (EA)),
        ?assertEqual (EA, from_udp (UA)),
        ?assertEqual (annotation_msg, peek_type_from_udp (EA)),
        ?assertEqual (annotation_msg, peek_type_from_udp (UA)),
        ?assertEqual (?MD_ANNOTATION_EVENT, peek_name_from_udp (EA)),
        ?assertEqual (?MD_ANNOTATION_EVENT, peek_name_from_udp (UA))
      end
    },
    { "basic codec test (stats)",
      fun() ->
        % simple tests for stats events
        ES = new ({127,0,0,1}, 2000, 5, ?MD_STATS_EVENT,
                 mondemand_statsmsg:new (<<"prog_id">>,[],
                                         [{gauge,<<"foo">>,5}],
                                         <<"known">>,6,7)),
        US = to_udp (ES),
        ?assertEqual (ES, from_udp (ES)),
        ?assertEqual (ES, from_udp (US)),
        ?assertEqual (stats_msg, peek_type_from_udp (ES)),
        ?assertEqual (stats_msg, peek_type_from_udp (US)),
        ?assertEqual (?MD_STATS_EVENT, peek_name_from_udp (ES)),
        ?assertEqual (?MD_STATS_EVENT, peek_name_from_udp (US))
      end
    },
    { "basic codec test (perf)",
      fun() ->

        % simple tests for perf events
        EP = new ({127,0,0,1}, 2000, 5, ?MD_PERF_EVENT,
                  mondemand_perfmsg:new (<<"id">>,<<"caller_label">>,
                                         [{<<"label">>,1,5}],
                                         [{<<"foo">>,<<"bar">>}])),
        UP = to_udp (EP),
        ?assertEqual (EP, from_udp (UP)),
        ?assertEqual (EP, from_udp (EP)),
        ?assertEqual (perf_msg, peek_type_from_udp (EP)),
        ?assertEqual (perf_msg, peek_type_from_udp (UP)),
        ?assertEqual (?MD_PERF_EVENT, peek_name_from_udp (EP)),
        ?assertEqual (?MD_PERF_EVENT, peek_name_from_udp (UP))
      end
    },
    { "basic codec test (log)",
      fun() ->

        % simple tests for log events
        EL = new ({127,0,0,1}, 2000, 5, ?MD_LOG_EVENT,
                  mondemand_logmsg:new (<<"prog_id">>, <<"host">>,
                                        [{<<"foo">>,<<"bar">>}],
                                        [mondemand_logmsg:new_line (
                                          <<"foo.txt">>, 10, <<"info">>,
                                          <<"stuff">>, 1)
                                        ],
                                        6
                                       )
                 ),
        UL = to_udp (EL),
        ?assertEqual (EL, from_udp (EL)),
        ?assertEqual (EL, from_udp (UL)),
        ?assertEqual (log_msg, peek_type_from_udp (EL)),
        ?assertEqual (log_msg, peek_type_from_udp (UL)),
        ?assertEqual (?MD_LOG_EVENT, peek_name_from_udp (EL)),
        ?assertEqual (?MD_LOG_EVENT, peek_name_from_udp (UL))
      end
    }
  ].
-endif.
