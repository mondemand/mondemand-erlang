-module (mondemand_logmsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ to_lwes/1,
           from_lwes/1
         ]).

-export ([ log_file_key/1,
           log_line_key/1,
           log_priority_key/1,
           log_message_key/1,
           log_repeat_key/1,
           priority_string/1
         ]).

to_lwes (L) when is_list (L) ->
    lists:map (fun to_lwes/1, L);

to_lwes (#md_log_msg { send_time = SendTimeIn,
                       prog_id = ProgId,
                       host = Host,
                       num_context = NumContexts,
                       context = Context,
                       num_lines = NumLines,
                       lines = Lines
                     }) ->
  SendTime =
    case SendTimeIn of
      undefined -> mondemand_util:millis_since_epoch();
      T -> T
    end,
  #lwes_event {
    name = ?MD_LOG_EVENT,
    attrs = lists:flatten (
              [ { ?LWES_STRING, ?MD_PROG_ID, ProgId },
                { ?LWES_INT_64, ?MD_SEND_TIME, SendTime },
                { ?LWES_U_INT_16, ?MD_NUM, NumLines },
                lists:zipwith (fun line_to_lwes/2,
                               lists:seq (1, NumLines),
                               Lines),
                { ?LWES_U_INT_16, ?MD_CTXT_NUM, NumContexts + 1},
                lists:zipwith (fun mondemand_util:context_to_lwes/2,
                               lists:seq (1, NumContexts),
                               Context),
                mondemand_util:context_to_lwes (NumContexts+1,
                                                { ?MD_HOST, Host })
              ]
            )
  }.

line_to_lwes (LineIndex,
              #md_log_line { file = File,
                             line = Line,
                             priority = Priority,
                             message = Message,
                             repeat_count = RepeatCount }) ->
  [ { ?LWES_STRING, log_file_key(LineIndex), File },
    { ?LWES_U_INT_32, log_line_key(LineIndex), Line },
    { ?LWES_U_INT_32, log_priority_key(LineIndex), Priority },
    { ?LWES_STRING, log_message_key(LineIndex), Message },
    { ?LWES_U_INT_16, log_repeat_key(LineIndex), RepeatCount }
  ].

from_lwes (#lwes_event { attrs = Data}) ->
  ProgId = dict:fetch (?MD_PROG_ID, Data),
  SenderIP = dict:fetch (?MD_SENDER_IP, Data),
  SenderPort = dict:fetch (?MD_SENDER_PORT, Data),
  ReceiptTime = dict:fetch (?MD_RECEIPT_TIME, Data),
  SendTime =
    case dict:find (?MD_SEND_TIME, Data) of
      error -> undefined;
      {ok, T} -> T
    end,
  {Host, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  {NumLines, Lines} = line_from_lwes (Data),
  #md_log_msg {
    send_time = SendTime,
    receipt_time = ReceiptTime,
    sender_ip = SenderIP,
    sender_port = SenderPort,
    prog_id = ProgId,
    host = Host,
    num_context = NumContexts,
    context = Context,
    num_lines = NumLines,
    lines = Lines
  }.

line_from_lwes (Data) ->
  Num =
    case dict:find (?MD_LOG_NUM, Data) of
      error -> 0;
      {ok, C} -> C
    end,
  { Num,
    lists:map (
      fun (N) ->
        F = dict:fetch (log_file_key (N), Data),
        L = dict:fetch (log_line_key (N), Data),
        P = priority_string (dict:fetch (log_priority_key (N), Data)),
        M = dict:fetch (log_message_key (N), Data),
        R = case dict:find (log_repeat_key (N), Data) of
              error -> 1;
              {ok, V} -> V
            end,
        #md_log_line {file = F,
                      line = L,
                      priority = P,
                      message = M,
                      repeat_count = R}
      end,
      lists:seq (1, Num)
    )
  }.

log_file_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_LOG_FILE_PREFIX).
log_line_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_LOG_LINE_PREFIX).
log_priority_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_LOG_PRIORITY_PREFIX).
log_message_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_LOG_MESSAGE_PREFIX).
log_repeat_key (N) ->
    ?ELEMENT_OF_TUPLE_LIST (N, ?MD_LOG_REPEAT_PREFIX).

priority_string (0) -> <<"emerg">>;
priority_string (1) -> <<"alert">>;
priority_string (2) -> <<"crit">>;
priority_string (3) -> <<"error">>;
priority_string (4) -> <<"warning">>;
priority_string (5) -> <<"notice">>;
priority_string (6) -> <<"info">>;
priority_string (7) -> <<"debug">>;
priority_string (8) -> <<"all">>.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
