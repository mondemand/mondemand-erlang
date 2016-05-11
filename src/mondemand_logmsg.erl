-module (mondemand_logmsg).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export ([ new/4,  % (ProgId, Host, Context, Lines)
           new/5,  % (ProgId, Host, Context, Lines, SendTime)
           new_line/5 % (File, Line, Priority, Message, RepeatCount)
         ]).

-export ([ to_lwes/1,
           from_lwes/1
         ]).

new (ProgId, Host, Context, Lines) ->
  new (ProgId, Host, Context, Lines, undefined).
new (ProgId, Host, Context, Lines = [#md_log_line{}|_], SendTime) ->
  #md_log_msg { send_time = SendTime,
                prog_id = ProgId,
                host = Host,
                num_context = length (Context),
                context = Context,
                num_lines = length (Lines),
                lines = Lines }.

new_line (File, Line, Priority, Message, RepeatCount) ->
  #md_log_line { file = File,
                 line = Line,
                 priority = Priority,
                 message = Message,
                 repeat_count = RepeatCount }.

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
                mondemand_util:context_to_lwes (Host, NumContexts, Context)
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
    { ?LWES_U_INT_32,log_priority_key(LineIndex),string_to_priority (Priority)},
    { ?LWES_STRING, log_message_key(LineIndex), Message },
    { ?LWES_U_INT_16, log_repeat_key(LineIndex), RepeatCount }
  ].

from_lwes (#lwes_event { attrs = Data}) ->
  ProgId = dict:fetch (?MD_PROG_ID, Data),
  ReceiptTime = dict:fetch (?MD_RECEIPT_TIME, Data),
  SendTime =
    case dict:find (?MD_SEND_TIME, Data) of
      error -> undefined;
      {ok, T} -> T
    end,
  {Host, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  {NumLines, Lines} = line_from_lwes (Data),
  { ReceiptTime,
    #md_log_msg {
      send_time = SendTime,
      prog_id = ProgId,
      host = Host,
      num_context = NumContexts,
      context = Context,
      num_lines = NumLines,
      lines = Lines
    }
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
        P = priority_to_string (dict:fetch (log_priority_key (N), Data)),
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

priority_to_string (0) -> <<"emerg">>;
priority_to_string (1) -> <<"alert">>;
priority_to_string (2) -> <<"crit">>;
priority_to_string (3) -> <<"error">>;
priority_to_string (4) -> <<"warning">>;
priority_to_string (5) -> <<"notice">>;
priority_to_string (6) -> <<"info">>;
priority_to_string (7) -> <<"debug">>;
priority_to_string (8) -> <<"all">>.

string_to_priority (L) when is_list (L) ->
  string_to_priority (list_to_binary (L));
string_to_priority (<<"emerg">>) -> 0;
string_to_priority (<<"alert">>) -> 1;
string_to_priority (<<"crit">>) -> 2;
string_to_priority (<<"error">>) -> 3;
string_to_priority (<<"warning">>) -> 4;
string_to_priority (<<"notice">>) -> 5;
string_to_priority (<<"info">>) -> 6;
string_to_priority (<<"debug">>) -> 7;
string_to_priority (<<"all">>) -> 8.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
