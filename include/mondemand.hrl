-ifndef(_mondemand_included).
-define(_mondemand_included, yup).

% this is the 'event' used internally and passed to all the downstream
% handlers
-record (md_event, { receipt_time,
                     sender_ip,
                     sender_port,
                     name,
                     msg
                   }).

% related to stats messages
-define (MD_STATS_EVENT, <<"MonDemand::StatsMsg">>).

% internal records for Mondemand::StatsMsg
-record (md_stats_msg, { send_time,
                         collect_time,
                         prog_id,
                         host,
                         num_context = 0,
                         context = [],
                         num_metrics = 0,
                         metrics = []
                       }).
-record (md_metric, { type,
                      key,
                      value
                    }).
-record (md_statset, { count,
                       sum,
                       min,
                       max,
                       avg,
                       median,
                       pctl_75,
                       pctl_90,
                       pctl_95,
                       pctl_98,
                       pctl_99
                     }).

% related to trace messages
-define (MD_TRACE_EVENT, <<"MonDemand::TraceMsg">>).

% record used internally for traced messages
-record (md_trace_msg, { send_time,
                         receipt_time,
                         sender_ip,
                         sender_port,
                         prog_id,
                         host,
                         trace_owner,
                         trace_id,
                         trace_message,
                         trace_data = []
                       }).

% related to log messages
-define (MD_LOG_EVENT,   <<"MonDemand::LogMsg">>).

-record (md_log_msg, { send_time,
                       receipt_time,
                       sender_ip,
                       sender_port,
                       prog_id,
                       host,
                       num_context = 0,
                       context = [],
                       num_lines = 0,
                       lines = []
                     }).
-record (md_log_line, { file,
                        line,
                        priority,
                        message,
                        repeat_count
                      }).

-endif.
