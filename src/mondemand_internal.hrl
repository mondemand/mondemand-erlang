-ifndef(_mondemand_internal_included).
-define(_mondemand_internal_included, yup).

-include("mondemand.hrl").

% global defaults
-define (MD_DEFAULT_SEND_INTERVAL, 60).

% these are common fields in several lwes events
-define (MD_RECEIPT_TIME,  <<"ReceiptTime">>).
-define (MD_SENDER_IP,     <<"SenderIP">>).
-define (MD_SENDER_PORT,   <<"SenderPort">>).
-define (MD_PROG_ID,       <<"prog_id">>).
-define (MD_SEND_TIME,     <<"send_time">>).
-define (MD_CTXT_NUM,      <<"ctxt_num">>).
-define (MD_CTXT_K,        <<"ctxt_k">>).
-define (MD_CTXT_V,        <<"ctxt_v">>).
-define (MD_NUM,           <<"num">>).
-define (MD_HOST,          <<"host">>).

% tokens in Mondemand::StatsMsg
-define (MD_STATS_K,             <<"k">>).
-define (MD_STATS_V,             <<"v">>).
-define (MD_STATS_T,             <<"t">>).

% LWES uses a signed int 64, so use that as the max metric value, so we
% reset at the max and min values where appropriate
-define (MD_STATS_MAX_METRIC_VALUE, 9223372036854775807).
-define (MD_STATS_MIN_METRIC_VALUE, -9223372036854775808).

% tokens in Mondemand::TraceMsg
-define (MD_TRACE_ID_KEY_BIN, <<"mondemand.trace_id">>).
-define (MD_TRACE_ID_KEY_LIST, "mondemand.trace_id").
-define (MD_TRACE_ID_KEY_ATOM, 'mondemand.trace_id').
-define (MD_TRACE_OWNER_KEY_BIN, <<"mondemand.owner">>).
-define (MD_TRACE_OWNER_KEY_LIST, "mondemand.owner").
-define (MD_TRACE_OWNER_KEY_ATOM, 'mondemand.owner').
-define (MD_TRACE_PROG_ID_KEY,  <<"mondemand.prog_id">>).
-define (MD_TRACE_SRC_HOST_KEY, <<"mondemand.src_host">>).
-define (MD_TRACE_MESSAGE_KEY,  <<"mondemand.message">>).

% tokens in Mondemand::LogMsg
-define (MD_LOG_NUM, <<"num">>).
-define (MD_LOG_FILE_PREFIX, <<"f">>).
-define (MD_LOG_LINE_PREFIX, <<"l">>).
-define (MD_LOG_PRIORITY_PREFIX, <<"p">>).
-define (MD_LOG_MESSAGE_PREFIX, <<"m">>).
-define (MD_LOG_REPEAT_PREFIX, <<"r">>).

% tokens used for log levels
-define (MD_LOG_EMERG_LEVEL, <<"emerg">>).
-define (MD_LOG_ALERT_LEVEL, <<"alert">>).
-define (MD_LOG_CRIT_LEVEL, <<"crit">>).
-define (MD_LOG_ERROR_LEVEL, <<"error">>).
-define (MD_LOG_WARNING_LEVEL, <<"warning">>).
-define (MD_LOG_NOTICE_LEVEL, <<"notice">>).
-define (MD_LOG_INFO_LEVEL, <<"info">>).
-define (MD_LOG_DEBUG_LEVEL, <<"debug">>).
-define (MD_LOG_ALL_LEVEL,  <<"all">>).

-compile({parse_transform, ct_expand}).

% generate lookup tables for lwes keys so save some time in production
-define (ELEMENT_OF_TUPLE_LIST(N,Prefix),
         element (N,
                  ct_expand:term (
                    begin
                      list_to_tuple (
                        [
                          list_to_binary ([Prefix, integer_to_list(E-1)])
                          || E
                          <- lists:seq(1,1024)
                        ]
                      )
                    end))).

-endif.
