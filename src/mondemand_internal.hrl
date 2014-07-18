-ifndef(_mondemand_internal_included).
-define(_mondemand_internal_included, yup).

-define (STATS_EVENT, <<"MonDemand::StatsMsg">>).
-define (TRACE_EVENT, <<"MonDemand::TraceMsg">>).

% tokens in Stats message
-define (PROG_ID,    <<"prog_id">>).
-define (STATS_NUM,  <<"num">>).
-define (STATS_K,    "k").
-define (STATS_V,    "v").
-define (STATS_T,    "t").
-define (CTXT_NUM,   <<"ctxt_num">>).
-define (CTXT_K,     "ctxt_k").
-define (CTXT_V,     "ctxt_v").
-define (STATS_HOST, <<"host">>).

% tokens in trace message
-define (TRACE_ID_KEY, "mondemand.trace_id").
-define (OWNER_ID_KEY, "mondemand.owner").
-define (PROG_ID_KEY,  "mondemand.prog_id").
-define (SRC_HOST_KEY, "mondemand.src_host").
-define (MESSAGE_KEY,  "mondemand.message").

-endif.
