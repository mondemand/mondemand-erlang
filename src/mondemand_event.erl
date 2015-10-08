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
                    name = Name,
                    msg = Msg }) ->
  case Name of
    ?MD_TRACE_EVENT -> ok;
    ?MD_STATS_EVENT -> {udp,
                        ok,
                        mondemand_util:normalize_ip (SenderIP),
                        SenderPort,
                        lwes_event:to_binary (mondemand_statsmsg:to_lwes (Msg))
                       };
    ?MD_LOG_EVENT -> {udp,
                      ok,
                      mondemand_util:normalize_ip (SenderIP),
                      SenderPort,
                      lwes_event:to_binary (mondemand_logmsg:to_lwes (Msg))
                     };
    ?MD_PERF_EVENT -> {udp,
                       ok,
                       mondemand_util:normalize_ip (SenderIP),
                       SenderPort,
                       lwes_event:to_binary (mondemand_perfmsg:to_lwes (Msg))
                      }
  end.

from_udp (Event = #md_event {}) ->
  Event;
from_udp ({udp, _, _, _, Event = #md_event {}}) ->
  Event;
from_udp (Packet = {udp, _, SenderIp, SenderPort, _}) ->
  case lwes_event:peek_name_from_udp (Packet) of
    { error, _ } ->
      error_logger:error_msg ("Bad Event ~p",[Packet]),
      undefined;
    ?MD_TRACE_EVENT ->
      % TODO get receipt time from json_eep18 decoded lib.
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  name = ?MD_TRACE_EVENT,
                  msg = lwes_event:from_udp_packet (Packet, json_eep18) };
    Name when Name =:= ?MD_STATS_EVENT; Name =:= ?MD_LOG_EVENT ->
      % deserialize the event as a dictionary
      Event = #lwes_event { attrs = Data }
          = lwes_event:from_udp_packet (Packet, dict),
      ReceiptTime = dict:fetch (?MD_RECEIPT_TIME, Data),
      Msg =
        case Name of
          ?MD_STATS_EVENT -> mondemand_statsmsg:from_lwes (Event);
          ?MD_LOG_EVENT -> mondemand_logmsg:from_lwes (Event);
          ?MD_PERF_EVENT -> mondemand_perfmsg:from_lwes (Event)
        end,
      #md_event { sender_ip = SenderIp,
                  sender_port = SenderPort,
                  receipt_time = ReceiptTime,
                  name = Name,
                  msg = Msg }
  end.

to_lwes (#md_event { name = Name, msg = Msg }) ->
  case Name of
    ?MD_TRACE_EVENT -> ok;
    ?MD_STATS_EVENT -> mondemand_statsmsg:to_lwes (Msg);
    ?MD_LOG_EVENT -> mondemand_logmsg:to_lwes (Msg);
    ?MD_PERF_EVENT -> mondemand_perfmsg:to_lwes (Msg)
  end.

name_to_type (?MD_STATS_EVENT) -> stats_msg;
name_to_type (?MD_LOG_EVENT) -> log_msg;
name_to_type (?MD_TRACE_EVENT) -> trace_msg;
name_to_type (?MD_PERF_EVENT) -> perf_msg;
name_to_type (_) -> undefined.

