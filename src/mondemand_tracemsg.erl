% NOTE
%   this module is currently unused, I need to do some heavy lifting to use
%   it as tracing is currently slightly different than stats and log messages
-module (mondemand_tracemsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ to_lwes/1,
           from_lwes/1
         ]).

to_lwes (#md_trace_msg { send_time = SendTimeIn,
                         prog_id = ProgId,
                         host = Host,
                         trace_owner = TraceOwner,
                         trace_id = TraceId,
                         trace_message = TraceMessage,
                         trace_data = TraceData }) ->
  SendTime =
    case SendTimeIn of
      undefined -> mondemand_util:millis_since_epoch();
      T -> T
    end,
  #lwes_event {
    name = ?MD_TRACE_EVENT,
    attrs = [ { ?LWES_INT_64, ?MD_SEND_TIME, SendTime },
              { ?LWES_STRING, ?MD_TRACE_PROG_ID_KEY, ProgId },
              { ?LWES_STRING, ?MD_TRACE_SRC_HOST_KEY, Host },
              { ?LWES_STRING, ?MD_TRACE_OWNER_KEY_BIN, TraceOwner },
              { ?LWES_STRING, ?MD_TRACE_ID_KEY_BIN, TraceId },
              { ?LWES_STRING, ?MD_TRACE_MESSAGE_KEY, TraceMessage }
              | lists:map (fun ({K, V}) ->
                            { ?LWES_STRING,
                              mondemand_util:binaryify (K),
                              mondemand_util:binaryify (V)
                            }
                           end,
                           TraceData)
             ]
  }.

from_lwes (#lwes_event { attrs = Data}) ->
  ProgId = dict:fetch (?MD_TRACE_PROG_ID_KEY, Data),
  Host = dict:fetch (?MD_TRACE_SRC_HOST_KEY, Data),
  SendTime = mondemand_util:find_in_dict (?MD_SEND_TIME, Data),
  TraceId =
    mondemand_util:find_in_dict (?MD_TRACE_ID_KEY_BIN, Data, <<"unknown">>),
  TraceOwner =
    mondemand_util:find_in_dict (?MD_TRACE_OWNER_KEY_BIN, Data, <<"unknown">>),
  TraceMessage =
    mondemand_util:find_in_dict (?MD_TRACE_MESSAGE_KEY, Data, <<"unknown">>),

  #md_trace_msg {
    send_time = SendTime,
    prog_id = ProgId,
    host = Host,
    trace_owner = TraceOwner,
    trace_id = TraceId,
    trace_message = TraceMessage
    % TODO: need to add trace_data here
  }.
