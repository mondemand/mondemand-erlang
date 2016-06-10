%% @author Anthony Molinaro <anthonym@alumni.caltech.edu>
%%
%% @doc Mondemand Stats functions
%%
%% This module wraps various records used by mondemand and provide
%% the serialization/deserialization to lwes.
%%
%% It also provide an API which should allow easier process of metrics
%% by the mondemand-server, for instance I'd want to do something like
%%
%%  Stats = mondemand_stats:from_lwes (Event),
%%  Host = mondemand_stats:host (Stats),
%%  Context = mondemand_stats:context (Stats),
%%  Metrics = mondemand_stats:metrics (Stats),
%%  lists:foldl (fun (Metric, A) ->
%%                 MetricType = mondemand_stats:metric_type (Metric),
%%                 MetricName = mondemand_stats:metric_name (Metric),
%%                 MetricValue= mondemand_stats:metric_value(Metric)
%%                 % do stuff here with everything above
%%               end,
%%               [],
%%               Metrics)
%%

-module (mondemand_statsmsg).
-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-define(STATSET_SEP, <<":">>).

-export ([new/3,
          new/4,
          new/5,
          new/6,
          new_statset/0,
          set_statset/3,
          get_statset/2,
          prog_id/1,
          host/1,
          collect_time/1,
          send_time/1,
          context/1,
          context_value/2,
          add_contexts/2,
          add_context/3,
          new_metric/3,
          num_metrics/1,
          metrics/1,
          metric/1,
          metric_type/1,
          metric_name/1,
          metric_value/1,
          to_lwes/1,
          from_lwes/1,
          statset_from_string/1,
          statset_to_list/1,
          statset_to_string/1
        ]).

% Context is of the form
%   [ {Key, Value} ]
% Metrics are of the form
%   [ {Type, Key, Value } ]
% for counters and gauges and
%   [ {Type, Key, #statset{} } ]
% for statsets
new (ProgId, Context, Metrics) ->
  Host = mondemand_config:host (),
  new (ProgId, Context, Metrics, Host).
new (ProgId, Context, Metrics, Host) ->
  new (ProgId, Context, Metrics, Host, undefined).
new (ProgId, Context, Metrics, Host, CollectTime) ->
  new (ProgId, Context, Metrics, Host, CollectTime, undefined).
new (ProgId, Context, Metrics = [{_,_,_}|_], Host, CollectTime, SendTime) ->
  ValidatedMetrics = [ #md_metric { type = T, key = K, value = V }
                       || { T, K, V }
                       <- Metrics ],
  new (ProgId, Context, ValidatedMetrics, Host, CollectTime, SendTime);
new (ProgId, Context, Metrics = [#md_metric{}|_],
     Host, CollectTime, SendTime) ->
  #md_stats_msg { collect_time = CollectTime,
                  send_time = SendTime,
                  prog_id = ProgId,
                  host = Host,
                  num_context = length (Context),
                  context = Context,
                  num_metrics = length (Metrics),
                  metrics = Metrics
                }.

new_statset () -> #md_statset {}.

set_statset (count, Count, S = #md_statset{}) -> S#md_statset {count = Count};
set_statset (sum, Sum, S = #md_statset{}) -> S#md_statset {sum = Sum};
set_statset (min, Min, S = #md_statset{}) -> S#md_statset {min = Min};
set_statset (max, Max, S = #md_statset{}) -> S#md_statset {max = Max};
set_statset (avg, Avg, S = #md_statset{}) -> S#md_statset {avg = Avg};
set_statset (median, Median, S = #md_statset{}) -> S#md_statset {median = Median};
set_statset (pctl_75, Pctl75, S = #md_statset{}) -> S#md_statset {pctl_75 = Pctl75};
set_statset (pctl_90, Pctl90, S = #md_statset{}) -> S#md_statset {pctl_90 = Pctl90};
set_statset (pctl_95, Pctl95, S = #md_statset{}) -> S#md_statset {pctl_95 = Pctl95};
set_statset (pctl_98, Pctl98, S = #md_statset{}) -> S#md_statset {pctl_98 = Pctl98};
set_statset (pctl_99, Pctl99, S = #md_statset{}) -> S#md_statset {pctl_99 = Pctl99}.

get_statset (count, S = #md_statset{}) -> S#md_statset.count;
get_statset (sum, S = #md_statset{}) -> S#md_statset.sum;
get_statset (min, S = #md_statset{}) -> S#md_statset.min;
get_statset (max, S = #md_statset{}) -> S#md_statset.max;
get_statset (avg, S = #md_statset{}) -> S#md_statset.avg;
get_statset (median, S = #md_statset{}) -> S#md_statset.median;
get_statset (pctl_75, S = #md_statset{}) -> S#md_statset.pctl_75;
get_statset (pctl_90, S = #md_statset{}) -> S#md_statset.pctl_90;
get_statset (pctl_95, S = #md_statset{}) -> S#md_statset.pctl_95;
get_statset (pctl_98, S = #md_statset{}) -> S#md_statset.pctl_98;
get_statset (pctl_99, S = #md_statset{}) -> S#md_statset.pctl_99.

-record(accum, { receipt_time,
                 send_time,
                 collect_time,
                 prog_id,
                 context_num = 0,
                 context_keys = [],
                 context_vals = [],
                 metric_num = 0,
                 metric_types = [],
                 metric_keys = [],
                 metric_vals = []
               }).

% the ESF for mondemand stats messages is as follows
% MonDemand::StatsMsg
% {
%   string prog_id;    # program identifier
%   int64  send_time;  # send time for stats in milliseconds since epoch
%   int64  collect_time; # collect time for stats in milliseconds since epoch
%   uint16 num;        # number of stats messages in this event
%   string k0;         # name of the 0th counter
%   string t0;         # type of the 0th counter
%                      # (valid values are 'counter', 'gauge', or 'statset')
%   int64  v0;         # value of the 0th counter if its a counter or gauge
%   string v0;         # value of the 0th counter if it's a statset, the format
%                      # of the string is
%                      #  count:sum:min:max:avg:median:pctl_75:pctl_90:pctl_95:pctl_98:pctl_99
%                      # if any are not calculated they can be left unset
%
%   # repeated for num entries
%
%   uint16 ctxt_num;   # number of contextual key/value dimensions
%   string ctxt_k0;    # name of contextual metadata
%   string ctxt_v0;    # value of contextual metadata
%   # repeated for the number of contextual key/value pairs
% }

from_lwes (#lwes_event { attrs = Data}) ->
  #accum { receipt_time = ReceiptTime,
           send_time = SendTime,
           collect_time = CollectTime,
           prog_id = ProgramId,
           context_num = _ContextNum,
           context_keys = ContextKeys,
           context_vals = ContextVals,
           metric_num = MetricNum,
           metric_types = MetricTypes,
           metric_keys = MetricKeys,
           metric_vals = MetricValues
   } = process (Data,
                undefined, % ReceiptTime
                undefined, % SendTime
                undefined, % CollectTime
                undefined, % ProgramId
                0,         % ContextNum
                [],        % ContextKeys
                [],        % ContextVals
                0,         % MetricNum
                [],        % MetricTypes
                [],        % MetricKeys
                []),        % MetricVals
  MetricsTTuple = erlang:make_tuple (MetricNum, [], MetricTypes),
  MetricsKTuple = erlang:make_tuple (MetricNum, [], MetricKeys),
  MetricsVTuple = erlang:make_tuple (MetricNum, [], MetricValues),
  Metrics =
    [
      begin
        Type = element (N, MetricsTTuple),
        Key = element (N, MetricsKTuple),
        Value = element (N, MetricsVTuple),
        #md_metric { type = Type,
                     key = Key,
                     value = case Type of
                               statset -> statset_from_string (Value);
                               _ -> Value
                             end
                   }
      end
      || N
      <- lists:seq (1, MetricNum)
    ],
  {Host, Context} = zip_and_find_host (lists:sort(ContextKeys),
                                       lists:sort(ContextVals),
                                       <<"unknown">>,
                                       []),
  ContextNumOut = length (Context),

  { case ReceiptTime =:= undefined of
      true -> 0;
      false -> ReceiptTime
    end,
    #md_stats_msg { send_time = SendTime,
                    collect_time = CollectTime,
                    prog_id = ProgramId,
                    host = Host,
                    num_context = ContextNumOut,
                    context = Context,
                    num_metrics = MetricNum,
                    metrics = Metrics
                  }
  }.

to_lwes (L) when is_list (L) ->
  MaxMetrics = mondemand_config:max_metrics (),
  lists:flatten (lists:map (fun (X) -> to_lwes (X, MaxMetrics) end, L) );

to_lwes (MondemandStatsMsg = #md_stats_msg {}) ->
  MaxMetrics = mondemand_config:max_metrics (),
  to_lwes (MondemandStatsMsg, MaxMetrics).

% split events into chunks of max_metrics
to_lwes (MondemandStatsMsg = #md_stats_msg { num_metrics = NumMetrics }, MaxMetrics)
  when NumMetrics > MaxMetrics  ->
  lists:reverse (to_lwes( MondemandStatsMsg, MaxMetrics, []));

to_lwes (#md_stats_msg { send_time = SendTimeIn,
                         collect_time = CollectTimeIn,
                         prog_id = ProgId,
                         host = Host,
                         num_context = NumContexts,
                         context = Context,
                         num_metrics = NumMetrics,
                         metrics = Metrics
                       }, _) ->
  NowMillis = mondemand_util:millis_since_epoch(),
  SendTime =
    case SendTimeIn of
      undefined -> NowMillis;
      T -> T
    end,
  CollectTime =
    case CollectTimeIn of
      undefined -> NowMillis;
      CT -> CT
    end,
  #lwes_event {
    name  = ?MD_STATS_EVENT,
    attrs = lists:flatten (
              [ { ?LWES_STRING, ?MD_PROG_ID, ProgId },
                { ?LWES_INT_64, ?MD_SEND_TIME, SendTime },
                { ?LWES_INT_64, ?MD_COLLECT_TIME, CollectTime },
                { ?LWES_U_INT_16, ?MD_NUM, NumMetrics },
                lists:zipwith (fun metric_to_lwes/2,
                               lists:seq (1, NumMetrics),
                               Metrics),
                mondemand_util:context_to_lwes (Host, NumContexts, Context)
              ]
            )
  }.

to_lwes (MondemandStatsMsg = #md_stats_msg {
                         num_metrics = NumMetrics,
                         metrics = Metrics
                       }, MaxMetrics, EventList)
  when NumMetrics > MaxMetrics ->
  { HeadMetrics, TailMetrics } = lists:split (MaxMetrics, Metrics),
  to_lwes (MondemandStatsMsg#md_stats_msg {
                              num_metrics = length (TailMetrics),
                              metrics = TailMetrics },
           MaxMetrics,
           [ to_lwes (MondemandStatsMsg#md_stats_msg {
                                         num_metrics = MaxMetrics,
                                         metrics = HeadMetrics})
              | EventList ]);

to_lwes (MondemandStatsMsg = #md_stats_msg {}, _, EventList) ->
  [ to_lwes (MondemandStatsMsg) | EventList ].

metric_to_lwes (MetricIndex,
                #md_metric { key = Name, type = statset, value = Value }) ->
  [ { ?LWES_STRING,
      metric_name_key (MetricIndex),
      mondemand_util:stringify (Name) },
    { ?LWES_STRING,
      metric_type_key (MetricIndex),
      type_to_string (statset)
    },
    { ?LWES_STRING,
      metric_value_key (MetricIndex),
      statset_to_string (Value)
    }
  ];
metric_to_lwes (MetricIndex,
                #md_metric { key = Name, type = Type, value = Value }) ->
  [ { ?LWES_STRING,
      metric_name_key (MetricIndex),
      mondemand_util:stringify (Name)
    },
    { ?LWES_STRING,
      metric_type_key (MetricIndex),
      type_to_string (Type)
    },
    { ?LWES_INT_64,
      metric_value_key (MetricIndex),
      Value
    }
  ].

prog_id (#md_stats_msg { prog_id = ProgId }) -> ProgId.
host (#md_stats_msg { host = Host }) -> Host.

collect_time (#md_stats_msg { collect_time = CollectTime }) -> CollectTime.
send_time (#md_stats_msg { send_time = SendTime }) -> SendTime.

context (#md_stats_msg { context = Context }) -> Context.
context_value (#md_stats_msg { context = Context }, ContextKey) ->
  context_find (ContextKey, Context, undefined).

context_find (Key, Context, Default) ->
  case lists:keyfind (Key, 1, Context) of
    false -> Default;
    {_, H} -> H
  end.

add_contexts (S = #md_stats_msg { num_context = ContextNum,
                                  context = Context},
              L) when is_list (L) ->
  S#md_stats_msg { num_context = ContextNum + length (L),
                   context = L ++ Context }.

add_context (S = #md_stats_msg { num_context = ContextNum,
                                context = Context},
             ContextKey, ContextValue) ->
  S#md_stats_msg { num_context = ContextNum + 1,
                   context = [ {ContextKey, ContextValue} | Context ] }.

metrics (#md_stats_msg { metrics = Metrics }) -> Metrics.
num_metrics (#md_stats_msg { num_metrics = NumMetrics }) -> NumMetrics.

metric_name (#md_metric { key = Name }) -> Name.
metric_type (#md_metric { type = Type }) -> Type.
metric_value (#md_metric { value = Value }) -> Value.
new_metric (Type, Name, Value) ->
  #md_metric { key = Name, type = Type, value = Value }.
metric (#md_metric { key = Name, type = Type, value = Value }) ->
  { Type, Name, Value }.

statset_from_string (L) when is_list(L) ->
  statset_from_string (list_to_binary (L));
statset_from_string (B) when is_binary(B) ->
  case re:split (B, ?STATSET_SEP) of
    [ Count, Sum, Min, Max, Avg, Median,
      Pctl75, Pctl90, Pctl95, Pctl98, Pctl99] ->
      #md_statset {
        count = mondemand_util:integerify (Count),
        sum = mondemand_util:integerify (Sum),
        min = mondemand_util:integerify (Min),
        max = mondemand_util:integerify (Max),
        avg = mondemand_util:integerify (Avg),
        median = mondemand_util:integerify (Median),
        pctl_75 = mondemand_util:integerify (Pctl75),
        pctl_90 = mondemand_util:integerify (Pctl90),
        pctl_95 = mondemand_util:integerify (Pctl95),
        pctl_98 = mondemand_util:integerify (Pctl98),
        pctl_99 = mondemand_util:integerify (Pctl99)
      };
    _ ->
      undefined
  end.

statset_to_list (#md_statset {
                   count = Count,
                   sum = Sum,
                   min = Min,
                   max = Max,
                   avg = Avg,
                   median = Median,
                   pctl_75 = Pctl75,
                   pctl_90 = Pctl90,
                   pctl_95 = Pctl95,
                   pctl_98 = Pctl98,
                   pctl_99 = Pctl99
                 }) ->
  add_if_defined (count, Count,
    add_if_defined (sum, Sum,
      add_if_defined (min, Min,
        add_if_defined (max, Max,
          add_if_defined (avg, Avg,
            add_if_defined (median, Median,
              add_if_defined (pctl_75, Pctl75,
                add_if_defined (pctl_90, Pctl90,
                  add_if_defined (pctl_95, Pctl95,
                    add_if_defined (pctl_98, Pctl98,
                      add_if_defined (pctl_99, Pctl99, [])
                                   )))))))))).

add_if_defined (_, undefined, A) -> A;
add_if_defined (K, V, A) -> [{K,V}|A].

statset_to_string (StatSet = #md_statset {}) ->
  % somewhat dense, but basically take the record, turn it into a list
  % strip off the tag, then turn entries into strings or empty string
  % and join
  mondemand_util:join (
    lists:map (fun num_or_empty/1, tl (tuple_to_list (StatSet))),
    ?STATSET_SEP).

num_or_empty (I) when is_integer (I) ->
  integer_to_list (I);
num_or_empty (_) ->
  "".

metric_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_STATS_K).

metric_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_STATS_V).

metric_type_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?MD_STATS_T).

string_to_type (L) when is_list(L) ->
  string_to_type (list_to_binary (L));
string_to_type (<<"gauge">>)   -> gauge;
string_to_type (<<"counter">>) -> counter;
string_to_type (<<"statset">>) -> statset.

type_to_string (gauge)   -> <<"gauge">>;
type_to_string (counter) -> <<"counter">>;
type_to_string (statset) -> <<"statset">>.

zip_and_find_host ([], [], Host, Context) ->
  { Host, Context };
zip_and_find_host ([{K, <<"host">>} | Keys], [{K, Host} | Vals], _, Context) ->
  zip_and_find_host (Keys, Vals, Host, Context);
zip_and_find_host ([{K, Key} | Keys ], [{K, Val} | Vals ], Host, Context) ->
  zip_and_find_host (Keys, Vals, Host, [{Key, Val} | Context]).

process ([],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  #accum { receipt_time = ReceiptTime,
           send_time = SendTime,
           collect_time = CollectTime,
           prog_id = ProgramId,
           context_num = ContextNum,
           context_keys = ContextKeys,
           context_vals = ContextVals,
           metric_num = MetricNum,
           metric_types = MetricTypes,
           metric_keys = MetricKeys,
           metric_vals = MetricVals
   };
process ([{<<"ReceiptTime">>, R} | Rest],
         _,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           R,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"send_time">>, S} | Rest],
         ReceiptTime,
         _,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           S,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"collect_time">>, C} | Rest],
         ReceiptTime,
         SendTime,
         _,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           C,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"prog_id">>, P} | Rest],
         ReceiptTime,
         SendTime,
         CollectTime,
         _,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           P,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"ctxt_num">>, C} | Rest],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         _,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           C,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"ctxt_k",N/binary>>,Key} | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           [ {index (N), Key} | ContextKeys],
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"ctxt_v",N/binary>>,Val} | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           [ {index (N), Val} | ContextVals],
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"num">>, N} | Rest],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         _,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           N,
           MetricTypes,
           MetricKeys,
           MetricVals);
process ([{<<"t",N/binary>>,Type} | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           [ { index (N), string_to_type (Type) } | MetricTypes],
           MetricKeys,
           MetricVals);
process ([{<<"k",N/binary>>,Key} | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           [ {index (N), Key} | MetricKeys],
           MetricVals);
process ([{<<"v",N/binary>>, Val} | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           [ {index (N), Val} | MetricVals]);
process ([_ | Rest ],
         ReceiptTime,
         SendTime,
         CollectTime,
         ProgramId,
         ContextNum,
         ContextKeys,
         ContextVals,
         MetricNum,
         MetricTypes,
         MetricKeys,
         MetricVals) ->
  process (Rest,
           ReceiptTime,
           SendTime,
           CollectTime,
           ProgramId,
           ContextNum,
           ContextKeys,
           ContextVals,
           MetricNum,
           MetricTypes,
           MetricKeys,
           MetricVals).

index (<<"0">>) -> 1;
index (<<"1">>) -> 2;
index (<<"2">>) -> 3;
index (<<"3">>) -> 4;
index (<<"4">>) -> 5;
index (<<"5">>) -> 6;
index (<<"6">>) -> 7;
index (<<"7">>) -> 8;
index (<<"8">>) -> 9;
index (<<"9">>) -> 10;
index (<<"10">>) -> 11;
index (<<"11">>) -> 12;
index (<<"12">>) -> 13;
index (<<"13">>) -> 14;
index (<<"14">>) -> 15;
index (<<"15">>) -> 16;
index (<<"16">>) -> 17;
index (<<"17">>) -> 18;
index (<<"18">>) -> 19;
index (<<"19">>) -> 20;
index (<<"20">>) -> 21;
index (<<"21">>) -> 22;
index (<<"22">>) -> 23;
index (<<"23">>) -> 24;
index (<<"24">>) -> 25;
index (<<"25">>) -> 26;
index (<<"26">>) -> 27;
index (<<"27">>) -> 28;
index (<<"28">>) -> 29;
index (<<"29">>) -> 30;
index (<<"30">>) -> 31;
index (<<"31">>) -> 32;
index (<<"32">>) -> 33;
index (<<"33">>) -> 34;
index (<<"34">>) -> 35;
index (<<"35">>) -> 36;
index (<<"36">>) -> 37;
index (<<"37">>) -> 38;
index (<<"38">>) -> 39;
index (<<"39">>) -> 40;
index (<<"40">>) -> 41;
index (<<"41">>) -> 42;
index (<<"42">>) -> 43;
index (<<"43">>) -> 44;
index (<<"44">>) -> 45;
index (<<"45">>) -> 46;
index (<<"46">>) -> 47;
index (<<"47">>) -> 48;
index (<<"48">>) -> 49;
index (<<"49">>) -> 50;
index (<<"50">>) -> 51;
index (<<"51">>) -> 52;
index (<<"52">>) -> 53;
index (<<"53">>) -> 54;
index (<<"54">>) -> 55;
index (<<"55">>) -> 56;
index (<<"56">>) -> 57;
index (<<"57">>) -> 58;
index (<<"58">>) -> 59;
index (<<"59">>) -> 60;
index (<<"60">>) -> 61;
index (<<"61">>) -> 62;
index (<<"62">>) -> 63;
index (<<"63">>) -> 64;
index (<<"64">>) -> 65;
index (<<"65">>) -> 66;
index (<<"66">>) -> 67;
index (<<"67">>) -> 68;
index (<<"68">>) -> 69;
index (<<"69">>) -> 70;
index (<<"70">>) -> 71;
index (<<"71">>) -> 72;
index (<<"72">>) -> 73;
index (<<"73">>) -> 74;
index (<<"74">>) -> 75;
index (<<"75">>) -> 76;
index (<<"76">>) -> 77;
index (<<"77">>) -> 78;
index (<<"78">>) -> 79;
index (<<"79">>) -> 80;
index (<<"80">>) -> 81;
index (<<"81">>) -> 82;
index (<<"82">>) -> 83;
index (<<"83">>) -> 84;
index (<<"84">>) -> 85;
index (<<"85">>) -> 86;
index (<<"86">>) -> 87;
index (<<"87">>) -> 88;
index (<<"88">>) -> 89;
index (<<"89">>) -> 90;
index (<<"90">>) -> 91;
index (<<"91">>) -> 92;
index (<<"92">>) -> 93;
index (<<"93">>) -> 94;
index (<<"94">>) -> 95;
index (<<"95">>) -> 96;
index (<<"96">>) -> 97;
index (<<"97">>) -> 98;
index (<<"98">>) -> 99;
index (<<"99">>) -> 100;
index (<<"100">>) -> 101;
index (<<"101">>) -> 102;
index (<<"102">>) -> 103;
index (<<"103">>) -> 104;
index (<<"104">>) -> 105;
index (<<"105">>) -> 106;
index (<<"106">>) -> 107;
index (<<"107">>) -> 108;
index (<<"108">>) -> 109;
index (<<"109">>) -> 110;
index (<<"110">>) -> 111;
index (<<"111">>) -> 112;
index (<<"112">>) -> 113;
index (<<"113">>) -> 114;
index (<<"114">>) -> 115;
index (<<"115">>) -> 116;
index (<<"116">>) -> 117;
index (<<"117">>) -> 118;
index (<<"118">>) -> 119;
index (<<"119">>) -> 120;
index (<<"120">>) -> 121;
index (<<"121">>) -> 122;
index (<<"122">>) -> 123;
index (<<"123">>) -> 124;
index (<<"124">>) -> 125;
index (<<"125">>) -> 126;
index (<<"126">>) -> 127;
index (<<"127">>) -> 128;
index (<<"128">>) -> 129;
index (<<"129">>) -> 130;
index (<<"130">>) -> 131;
index (<<"131">>) -> 132;
index (<<"132">>) -> 133;
index (<<"133">>) -> 134;
index (<<"134">>) -> 135;
index (<<"135">>) -> 136;
index (<<"136">>) -> 137;
index (<<"137">>) -> 138;
index (<<"138">>) -> 139;
index (<<"139">>) -> 140;
index (<<"140">>) -> 141;
index (<<"141">>) -> 142;
index (<<"142">>) -> 143;
index (<<"143">>) -> 144;
index (<<"144">>) -> 145;
index (<<"145">>) -> 146;
index (<<"146">>) -> 147;
index (<<"147">>) -> 148;
index (<<"148">>) -> 149;
index (<<"149">>) -> 150;
index (<<"150">>) -> 151;
index (<<"151">>) -> 152;
index (<<"152">>) -> 153;
index (<<"153">>) -> 154;
index (<<"154">>) -> 155;
index (<<"155">>) -> 156;
index (<<"156">>) -> 157;
index (<<"157">>) -> 158;
index (<<"158">>) -> 159;
index (<<"159">>) -> 160;
index (<<"160">>) -> 161;
index (<<"161">>) -> 162;
index (<<"162">>) -> 163;
index (<<"163">>) -> 164;
index (<<"164">>) -> 165;
index (<<"165">>) -> 166;
index (<<"166">>) -> 167;
index (<<"167">>) -> 168;
index (<<"168">>) -> 169;
index (<<"169">>) -> 170;
index (<<"170">>) -> 171;
index (<<"171">>) -> 172;
index (<<"172">>) -> 173;
index (<<"173">>) -> 174;
index (<<"174">>) -> 175;
index (<<"175">>) -> 176;
index (<<"176">>) -> 177;
index (<<"177">>) -> 178;
index (<<"178">>) -> 179;
index (<<"179">>) -> 180;
index (<<"180">>) -> 181;
index (<<"181">>) -> 182;
index (<<"182">>) -> 183;
index (<<"183">>) -> 184;
index (<<"184">>) -> 185;
index (<<"185">>) -> 186;
index (<<"186">>) -> 187;
index (<<"187">>) -> 188;
index (<<"188">>) -> 189;
index (<<"189">>) -> 190;
index (<<"190">>) -> 191;
index (<<"191">>) -> 192;
index (<<"192">>) -> 193;
index (<<"193">>) -> 194;
index (<<"194">>) -> 195;
index (<<"195">>) -> 196;
index (<<"196">>) -> 197;
index (<<"197">>) -> 198;
index (<<"198">>) -> 199;
index (<<"199">>) -> 200;
index (<<"200">>) -> 201;
index (<<"201">>) -> 202;
index (<<"202">>) -> 203;
index (<<"203">>) -> 204;
index (<<"204">>) -> 205;
index (<<"205">>) -> 206;
index (<<"206">>) -> 207;
index (<<"207">>) -> 208;
index (<<"208">>) -> 209;
index (<<"209">>) -> 210;
index (<<"210">>) -> 211;
index (<<"211">>) -> 212;
index (<<"212">>) -> 213;
index (<<"213">>) -> 214;
index (<<"214">>) -> 215;
index (<<"215">>) -> 216;
index (<<"216">>) -> 217;
index (<<"217">>) -> 218;
index (<<"218">>) -> 219;
index (<<"219">>) -> 220;
index (<<"220">>) -> 221;
index (<<"221">>) -> 222;
index (<<"222">>) -> 223;
index (<<"223">>) -> 224;
index (<<"224">>) -> 225;
index (<<"225">>) -> 226;
index (<<"226">>) -> 227;
index (<<"227">>) -> 228;
index (<<"228">>) -> 229;
index (<<"229">>) -> 230;
index (<<"230">>) -> 231;
index (<<"231">>) -> 232;
index (<<"232">>) -> 233;
index (<<"233">>) -> 234;
index (<<"234">>) -> 235;
index (<<"235">>) -> 236;
index (<<"236">>) -> 237;
index (<<"237">>) -> 238;
index (<<"238">>) -> 239;
index (<<"239">>) -> 240;
index (<<"240">>) -> 241;
index (<<"241">>) -> 242;
index (<<"242">>) -> 243;
index (<<"243">>) -> 244;
index (<<"244">>) -> 245;
index (<<"245">>) -> 246;
index (<<"246">>) -> 247;
index (<<"247">>) -> 248;
index (<<"248">>) -> 249;
index (<<"249">>) -> 250;
index (<<"250">>) -> 251;
index (<<"251">>) -> 252;
index (<<"252">>) -> 253;
index (<<"253">>) -> 254;
index (<<"254">>) -> 255;
index (<<"255">>) -> 256;
index (<<"256">>) -> 257;
index (<<"257">>) -> 258;
index (<<"258">>) -> 259;
index (<<"259">>) -> 260;
index (<<"260">>) -> 261;
index (<<"261">>) -> 262;
index (<<"262">>) -> 263;
index (<<"263">>) -> 264;
index (<<"264">>) -> 265;
index (<<"265">>) -> 266;
index (<<"266">>) -> 267;
index (<<"267">>) -> 268;
index (<<"268">>) -> 269;
index (<<"269">>) -> 270;
index (<<"270">>) -> 271;
index (<<"271">>) -> 272;
index (<<"272">>) -> 273;
index (<<"273">>) -> 274;
index (<<"274">>) -> 275;
index (<<"275">>) -> 276;
index (<<"276">>) -> 277;
index (<<"277">>) -> 278;
index (<<"278">>) -> 279;
index (<<"279">>) -> 280;
index (<<"280">>) -> 281;
index (<<"281">>) -> 282;
index (<<"282">>) -> 283;
index (<<"283">>) -> 284;
index (<<"284">>) -> 285;
index (<<"285">>) -> 286;
index (<<"286">>) -> 287;
index (<<"287">>) -> 288;
index (<<"288">>) -> 289;
index (<<"289">>) -> 290;
index (<<"290">>) -> 291;
index (<<"291">>) -> 292;
index (<<"292">>) -> 293;
index (<<"293">>) -> 294;
index (<<"294">>) -> 295;
index (<<"295">>) -> 296;
index (<<"296">>) -> 297;
index (<<"297">>) -> 298;
index (<<"298">>) -> 299;
index (<<"299">>) -> 300;
index (<<"300">>) -> 301;
index (<<"301">>) -> 302;
index (<<"302">>) -> 303;
index (<<"303">>) -> 304;
index (<<"304">>) -> 305;
index (<<"305">>) -> 306;
index (<<"306">>) -> 307;
index (<<"307">>) -> 308;
index (<<"308">>) -> 309;
index (<<"309">>) -> 310;
index (<<"310">>) -> 311;
index (<<"311">>) -> 312;
index (<<"312">>) -> 313;
index (<<"313">>) -> 314;
index (<<"314">>) -> 315;
index (<<"315">>) -> 316;
index (<<"316">>) -> 317;
index (<<"317">>) -> 318;
index (<<"318">>) -> 319;
index (<<"319">>) -> 320;
index (<<"320">>) -> 321;
index (<<"321">>) -> 322;
index (<<"322">>) -> 323;
index (<<"323">>) -> 324;
index (<<"324">>) -> 325;
index (<<"325">>) -> 326;
index (<<"326">>) -> 327;
index (<<"327">>) -> 328;
index (<<"328">>) -> 329;
index (<<"329">>) -> 330;
index (<<"330">>) -> 331;
index (<<"331">>) -> 332;
index (<<"332">>) -> 333;
index (<<"333">>) -> 334;
index (<<"334">>) -> 335;
index (<<"335">>) -> 336;
index (<<"336">>) -> 337;
index (<<"337">>) -> 338;
index (<<"338">>) -> 339;
index (<<"339">>) -> 340;
index (<<"340">>) -> 341;
index (<<"341">>) -> 342;
index (<<"342">>) -> 343;
index (<<"343">>) -> 344;
index (<<"344">>) -> 345;
index (<<"345">>) -> 346;
index (<<"346">>) -> 347;
index (<<"347">>) -> 348;
index (<<"348">>) -> 349;
index (<<"349">>) -> 350;
index (<<"350">>) -> 351;
index (<<"351">>) -> 352;
index (<<"352">>) -> 353;
index (<<"353">>) -> 354;
index (<<"354">>) -> 355;
index (<<"355">>) -> 356;
index (<<"356">>) -> 357;
index (<<"357">>) -> 358;
index (<<"358">>) -> 359;
index (<<"359">>) -> 360;
index (<<"360">>) -> 361;
index (<<"361">>) -> 362;
index (<<"362">>) -> 363;
index (<<"363">>) -> 364;
index (<<"364">>) -> 365;
index (<<"365">>) -> 366;
index (<<"366">>) -> 367;
index (<<"367">>) -> 368;
index (<<"368">>) -> 369;
index (<<"369">>) -> 370;
index (<<"370">>) -> 371;
index (<<"371">>) -> 372;
index (<<"372">>) -> 373;
index (<<"373">>) -> 374;
index (<<"374">>) -> 375;
index (<<"375">>) -> 376;
index (<<"376">>) -> 377;
index (<<"377">>) -> 378;
index (<<"378">>) -> 379;
index (<<"379">>) -> 380;
index (<<"380">>) -> 381;
index (<<"381">>) -> 382;
index (<<"382">>) -> 383;
index (<<"383">>) -> 384;
index (<<"384">>) -> 385;
index (<<"385">>) -> 386;
index (<<"386">>) -> 387;
index (<<"387">>) -> 388;
index (<<"388">>) -> 389;
index (<<"389">>) -> 390;
index (<<"390">>) -> 391;
index (<<"391">>) -> 392;
index (<<"392">>) -> 393;
index (<<"393">>) -> 394;
index (<<"394">>) -> 395;
index (<<"395">>) -> 396;
index (<<"396">>) -> 397;
index (<<"397">>) -> 398;
index (<<"398">>) -> 399;
index (<<"399">>) -> 400;
index (<<"400">>) -> 401;
index (<<"401">>) -> 402;
index (<<"402">>) -> 403;
index (<<"403">>) -> 404;
index (<<"404">>) -> 405;
index (<<"405">>) -> 406;
index (<<"406">>) -> 407;
index (<<"407">>) -> 408;
index (<<"408">>) -> 409;
index (<<"409">>) -> 410;
index (<<"410">>) -> 411;
index (<<"411">>) -> 412;
index (<<"412">>) -> 413;
index (<<"413">>) -> 414;
index (<<"414">>) -> 415;
index (<<"415">>) -> 416;
index (<<"416">>) -> 417;
index (<<"417">>) -> 418;
index (<<"418">>) -> 419;
index (<<"419">>) -> 420;
index (<<"420">>) -> 421;
index (<<"421">>) -> 422;
index (<<"422">>) -> 423;
index (<<"423">>) -> 424;
index (<<"424">>) -> 425;
index (<<"425">>) -> 426;
index (<<"426">>) -> 427;
index (<<"427">>) -> 428;
index (<<"428">>) -> 429;
index (<<"429">>) -> 430;
index (<<"430">>) -> 431;
index (<<"431">>) -> 432;
index (<<"432">>) -> 433;
index (<<"433">>) -> 434;
index (<<"434">>) -> 435;
index (<<"435">>) -> 436;
index (<<"436">>) -> 437;
index (<<"437">>) -> 438;
index (<<"438">>) -> 439;
index (<<"439">>) -> 440;
index (<<"440">>) -> 441;
index (<<"441">>) -> 442;
index (<<"442">>) -> 443;
index (<<"443">>) -> 444;
index (<<"444">>) -> 445;
index (<<"445">>) -> 446;
index (<<"446">>) -> 447;
index (<<"447">>) -> 448;
index (<<"448">>) -> 449;
index (<<"449">>) -> 450;
index (<<"450">>) -> 451;
index (<<"451">>) -> 452;
index (<<"452">>) -> 453;
index (<<"453">>) -> 454;
index (<<"454">>) -> 455;
index (<<"455">>) -> 456;
index (<<"456">>) -> 457;
index (<<"457">>) -> 458;
index (<<"458">>) -> 459;
index (<<"459">>) -> 460;
index (<<"460">>) -> 461;
index (<<"461">>) -> 462;
index (<<"462">>) -> 463;
index (<<"463">>) -> 464;
index (<<"464">>) -> 465;
index (<<"465">>) -> 466;
index (<<"466">>) -> 467;
index (<<"467">>) -> 468;
index (<<"468">>) -> 469;
index (<<"469">>) -> 470;
index (<<"470">>) -> 471;
index (<<"471">>) -> 472;
index (<<"472">>) -> 473;
index (<<"473">>) -> 474;
index (<<"474">>) -> 475;
index (<<"475">>) -> 476;
index (<<"476">>) -> 477;
index (<<"477">>) -> 478;
index (<<"478">>) -> 479;
index (<<"479">>) -> 480;
index (<<"480">>) -> 481;
index (<<"481">>) -> 482;
index (<<"482">>) -> 483;
index (<<"483">>) -> 484;
index (<<"484">>) -> 485;
index (<<"485">>) -> 486;
index (<<"486">>) -> 487;
index (<<"487">>) -> 488;
index (<<"488">>) -> 489;
index (<<"489">>) -> 490;
index (<<"490">>) -> 491;
index (<<"491">>) -> 492;
index (<<"492">>) -> 493;
index (<<"493">>) -> 494;
index (<<"494">>) -> 495;
index (<<"495">>) -> 496;
index (<<"496">>) -> 497;
index (<<"497">>) -> 498;
index (<<"498">>) -> 499;
index (<<"499">>) -> 500;
index (<<"500">>) -> 501;
index (<<"501">>) -> 502;
index (<<"502">>) -> 503;
index (<<"503">>) -> 504;
index (<<"504">>) -> 505;
index (<<"505">>) -> 506;
index (<<"506">>) -> 507;
index (<<"507">>) -> 508;
index (<<"508">>) -> 509;
index (<<"509">>) -> 510;
index (<<"510">>) -> 511;
index (<<"511">>) -> 512;
index (<<"512">>) -> 513;
index (<<"513">>) -> 514;
index (<<"514">>) -> 515;
index (<<"515">>) -> 516;
index (<<"516">>) -> 517;
index (<<"517">>) -> 518;
index (<<"518">>) -> 519;
index (<<"519">>) -> 520;
index (<<"520">>) -> 521;
index (<<"521">>) -> 522;
index (<<"522">>) -> 523;
index (<<"523">>) -> 524;
index (<<"524">>) -> 525;
index (<<"525">>) -> 526;
index (<<"526">>) -> 527;
index (<<"527">>) -> 528;
index (<<"528">>) -> 529;
index (<<"529">>) -> 530;
index (<<"530">>) -> 531;
index (<<"531">>) -> 532;
index (<<"532">>) -> 533;
index (<<"533">>) -> 534;
index (<<"534">>) -> 535;
index (<<"535">>) -> 536;
index (<<"536">>) -> 537;
index (<<"537">>) -> 538;
index (<<"538">>) -> 539;
index (<<"539">>) -> 540;
index (<<"540">>) -> 541;
index (<<"541">>) -> 542;
index (<<"542">>) -> 543;
index (<<"543">>) -> 544;
index (<<"544">>) -> 545;
index (<<"545">>) -> 546;
index (<<"546">>) -> 547;
index (<<"547">>) -> 548;
index (<<"548">>) -> 549;
index (<<"549">>) -> 550;
index (<<"550">>) -> 551;
index (<<"551">>) -> 552;
index (<<"552">>) -> 553;
index (<<"553">>) -> 554;
index (<<"554">>) -> 555;
index (<<"555">>) -> 556;
index (<<"556">>) -> 557;
index (<<"557">>) -> 558;
index (<<"558">>) -> 559;
index (<<"559">>) -> 560;
index (<<"560">>) -> 561;
index (<<"561">>) -> 562;
index (<<"562">>) -> 563;
index (<<"563">>) -> 564;
index (<<"564">>) -> 565;
index (<<"565">>) -> 566;
index (<<"566">>) -> 567;
index (<<"567">>) -> 568;
index (<<"568">>) -> 569;
index (<<"569">>) -> 570;
index (<<"570">>) -> 571;
index (<<"571">>) -> 572;
index (<<"572">>) -> 573;
index (<<"573">>) -> 574;
index (<<"574">>) -> 575;
index (<<"575">>) -> 576;
index (<<"576">>) -> 577;
index (<<"577">>) -> 578;
index (<<"578">>) -> 579;
index (<<"579">>) -> 580;
index (<<"580">>) -> 581;
index (<<"581">>) -> 582;
index (<<"582">>) -> 583;
index (<<"583">>) -> 584;
index (<<"584">>) -> 585;
index (<<"585">>) -> 586;
index (<<"586">>) -> 587;
index (<<"587">>) -> 588;
index (<<"588">>) -> 589;
index (<<"589">>) -> 590;
index (<<"590">>) -> 591;
index (<<"591">>) -> 592;
index (<<"592">>) -> 593;
index (<<"593">>) -> 594;
index (<<"594">>) -> 595;
index (<<"595">>) -> 596;
index (<<"596">>) -> 597;
index (<<"597">>) -> 598;
index (<<"598">>) -> 599;
index (<<"599">>) -> 600;
index (<<"600">>) -> 601;
index (<<"601">>) -> 602;
index (<<"602">>) -> 603;
index (<<"603">>) -> 604;
index (<<"604">>) -> 605;
index (<<"605">>) -> 606;
index (<<"606">>) -> 607;
index (<<"607">>) -> 608;
index (<<"608">>) -> 609;
index (<<"609">>) -> 610;
index (<<"610">>) -> 611;
index (<<"611">>) -> 612;
index (<<"612">>) -> 613;
index (<<"613">>) -> 614;
index (<<"614">>) -> 615;
index (<<"615">>) -> 616;
index (<<"616">>) -> 617;
index (<<"617">>) -> 618;
index (<<"618">>) -> 619;
index (<<"619">>) -> 620;
index (<<"620">>) -> 621;
index (<<"621">>) -> 622;
index (<<"622">>) -> 623;
index (<<"623">>) -> 624;
index (<<"624">>) -> 625;
index (<<"625">>) -> 626;
index (<<"626">>) -> 627;
index (<<"627">>) -> 628;
index (<<"628">>) -> 629;
index (<<"629">>) -> 630;
index (<<"630">>) -> 631;
index (<<"631">>) -> 632;
index (<<"632">>) -> 633;
index (<<"633">>) -> 634;
index (<<"634">>) -> 635;
index (<<"635">>) -> 636;
index (<<"636">>) -> 637;
index (<<"637">>) -> 638;
index (<<"638">>) -> 639;
index (<<"639">>) -> 640;
index (<<"640">>) -> 641;
index (<<"641">>) -> 642;
index (<<"642">>) -> 643;
index (<<"643">>) -> 644;
index (<<"644">>) -> 645;
index (<<"645">>) -> 646;
index (<<"646">>) -> 647;
index (<<"647">>) -> 648;
index (<<"648">>) -> 649;
index (<<"649">>) -> 650;
index (<<"650">>) -> 651;
index (<<"651">>) -> 652;
index (<<"652">>) -> 653;
index (<<"653">>) -> 654;
index (<<"654">>) -> 655;
index (<<"655">>) -> 656;
index (<<"656">>) -> 657;
index (<<"657">>) -> 658;
index (<<"658">>) -> 659;
index (<<"659">>) -> 660;
index (<<"660">>) -> 661;
index (<<"661">>) -> 662;
index (<<"662">>) -> 663;
index (<<"663">>) -> 664;
index (<<"664">>) -> 665;
index (<<"665">>) -> 666;
index (<<"666">>) -> 667;
index (<<"667">>) -> 668;
index (<<"668">>) -> 669;
index (<<"669">>) -> 670;
index (<<"670">>) -> 671;
index (<<"671">>) -> 672;
index (<<"672">>) -> 673;
index (<<"673">>) -> 674;
index (<<"674">>) -> 675;
index (<<"675">>) -> 676;
index (<<"676">>) -> 677;
index (<<"677">>) -> 678;
index (<<"678">>) -> 679;
index (<<"679">>) -> 680;
index (<<"680">>) -> 681;
index (<<"681">>) -> 682;
index (<<"682">>) -> 683;
index (<<"683">>) -> 684;
index (<<"684">>) -> 685;
index (<<"685">>) -> 686;
index (<<"686">>) -> 687;
index (<<"687">>) -> 688;
index (<<"688">>) -> 689;
index (<<"689">>) -> 690;
index (<<"690">>) -> 691;
index (<<"691">>) -> 692;
index (<<"692">>) -> 693;
index (<<"693">>) -> 694;
index (<<"694">>) -> 695;
index (<<"695">>) -> 696;
index (<<"696">>) -> 697;
index (<<"697">>) -> 698;
index (<<"698">>) -> 699;
index (<<"699">>) -> 700;
index (<<"700">>) -> 701;
index (<<"701">>) -> 702;
index (<<"702">>) -> 703;
index (<<"703">>) -> 704;
index (<<"704">>) -> 705;
index (<<"705">>) -> 706;
index (<<"706">>) -> 707;
index (<<"707">>) -> 708;
index (<<"708">>) -> 709;
index (<<"709">>) -> 710;
index (<<"710">>) -> 711;
index (<<"711">>) -> 712;
index (<<"712">>) -> 713;
index (<<"713">>) -> 714;
index (<<"714">>) -> 715;
index (<<"715">>) -> 716;
index (<<"716">>) -> 717;
index (<<"717">>) -> 718;
index (<<"718">>) -> 719;
index (<<"719">>) -> 720;
index (<<"720">>) -> 721;
index (<<"721">>) -> 722;
index (<<"722">>) -> 723;
index (<<"723">>) -> 724;
index (<<"724">>) -> 725;
index (<<"725">>) -> 726;
index (<<"726">>) -> 727;
index (<<"727">>) -> 728;
index (<<"728">>) -> 729;
index (<<"729">>) -> 730;
index (<<"730">>) -> 731;
index (<<"731">>) -> 732;
index (<<"732">>) -> 733;
index (<<"733">>) -> 734;
index (<<"734">>) -> 735;
index (<<"735">>) -> 736;
index (<<"736">>) -> 737;
index (<<"737">>) -> 738;
index (<<"738">>) -> 739;
index (<<"739">>) -> 740;
index (<<"740">>) -> 741;
index (<<"741">>) -> 742;
index (<<"742">>) -> 743;
index (<<"743">>) -> 744;
index (<<"744">>) -> 745;
index (<<"745">>) -> 746;
index (<<"746">>) -> 747;
index (<<"747">>) -> 748;
index (<<"748">>) -> 749;
index (<<"749">>) -> 750;
index (<<"750">>) -> 751;
index (<<"751">>) -> 752;
index (<<"752">>) -> 753;
index (<<"753">>) -> 754;
index (<<"754">>) -> 755;
index (<<"755">>) -> 756;
index (<<"756">>) -> 757;
index (<<"757">>) -> 758;
index (<<"758">>) -> 759;
index (<<"759">>) -> 760;
index (<<"760">>) -> 761;
index (<<"761">>) -> 762;
index (<<"762">>) -> 763;
index (<<"763">>) -> 764;
index (<<"764">>) -> 765;
index (<<"765">>) -> 766;
index (<<"766">>) -> 767;
index (<<"767">>) -> 768;
index (<<"768">>) -> 769;
index (<<"769">>) -> 770;
index (<<"770">>) -> 771;
index (<<"771">>) -> 772;
index (<<"772">>) -> 773;
index (<<"773">>) -> 774;
index (<<"774">>) -> 775;
index (<<"775">>) -> 776;
index (<<"776">>) -> 777;
index (<<"777">>) -> 778;
index (<<"778">>) -> 779;
index (<<"779">>) -> 780;
index (<<"780">>) -> 781;
index (<<"781">>) -> 782;
index (<<"782">>) -> 783;
index (<<"783">>) -> 784;
index (<<"784">>) -> 785;
index (<<"785">>) -> 786;
index (<<"786">>) -> 787;
index (<<"787">>) -> 788;
index (<<"788">>) -> 789;
index (<<"789">>) -> 790;
index (<<"790">>) -> 791;
index (<<"791">>) -> 792;
index (<<"792">>) -> 793;
index (<<"793">>) -> 794;
index (<<"794">>) -> 795;
index (<<"795">>) -> 796;
index (<<"796">>) -> 797;
index (<<"797">>) -> 798;
index (<<"798">>) -> 799;
index (<<"799">>) -> 800;
index (<<"800">>) -> 801;
index (<<"801">>) -> 802;
index (<<"802">>) -> 803;
index (<<"803">>) -> 804;
index (<<"804">>) -> 805;
index (<<"805">>) -> 806;
index (<<"806">>) -> 807;
index (<<"807">>) -> 808;
index (<<"808">>) -> 809;
index (<<"809">>) -> 810;
index (<<"810">>) -> 811;
index (<<"811">>) -> 812;
index (<<"812">>) -> 813;
index (<<"813">>) -> 814;
index (<<"814">>) -> 815;
index (<<"815">>) -> 816;
index (<<"816">>) -> 817;
index (<<"817">>) -> 818;
index (<<"818">>) -> 819;
index (<<"819">>) -> 820;
index (<<"820">>) -> 821;
index (<<"821">>) -> 822;
index (<<"822">>) -> 823;
index (<<"823">>) -> 824;
index (<<"824">>) -> 825;
index (<<"825">>) -> 826;
index (<<"826">>) -> 827;
index (<<"827">>) -> 828;
index (<<"828">>) -> 829;
index (<<"829">>) -> 830;
index (<<"830">>) -> 831;
index (<<"831">>) -> 832;
index (<<"832">>) -> 833;
index (<<"833">>) -> 834;
index (<<"834">>) -> 835;
index (<<"835">>) -> 836;
index (<<"836">>) -> 837;
index (<<"837">>) -> 838;
index (<<"838">>) -> 839;
index (<<"839">>) -> 840;
index (<<"840">>) -> 841;
index (<<"841">>) -> 842;
index (<<"842">>) -> 843;
index (<<"843">>) -> 844;
index (<<"844">>) -> 845;
index (<<"845">>) -> 846;
index (<<"846">>) -> 847;
index (<<"847">>) -> 848;
index (<<"848">>) -> 849;
index (<<"849">>) -> 850;
index (<<"850">>) -> 851;
index (<<"851">>) -> 852;
index (<<"852">>) -> 853;
index (<<"853">>) -> 854;
index (<<"854">>) -> 855;
index (<<"855">>) -> 856;
index (<<"856">>) -> 857;
index (<<"857">>) -> 858;
index (<<"858">>) -> 859;
index (<<"859">>) -> 860;
index (<<"860">>) -> 861;
index (<<"861">>) -> 862;
index (<<"862">>) -> 863;
index (<<"863">>) -> 864;
index (<<"864">>) -> 865;
index (<<"865">>) -> 866;
index (<<"866">>) -> 867;
index (<<"867">>) -> 868;
index (<<"868">>) -> 869;
index (<<"869">>) -> 870;
index (<<"870">>) -> 871;
index (<<"871">>) -> 872;
index (<<"872">>) -> 873;
index (<<"873">>) -> 874;
index (<<"874">>) -> 875;
index (<<"875">>) -> 876;
index (<<"876">>) -> 877;
index (<<"877">>) -> 878;
index (<<"878">>) -> 879;
index (<<"879">>) -> 880;
index (<<"880">>) -> 881;
index (<<"881">>) -> 882;
index (<<"882">>) -> 883;
index (<<"883">>) -> 884;
index (<<"884">>) -> 885;
index (<<"885">>) -> 886;
index (<<"886">>) -> 887;
index (<<"887">>) -> 888;
index (<<"888">>) -> 889;
index (<<"889">>) -> 890;
index (<<"890">>) -> 891;
index (<<"891">>) -> 892;
index (<<"892">>) -> 893;
index (<<"893">>) -> 894;
index (<<"894">>) -> 895;
index (<<"895">>) -> 896;
index (<<"896">>) -> 897;
index (<<"897">>) -> 898;
index (<<"898">>) -> 899;
index (<<"899">>) -> 900;
index (<<"900">>) -> 901;
index (<<"901">>) -> 902;
index (<<"902">>) -> 903;
index (<<"903">>) -> 904;
index (<<"904">>) -> 905;
index (<<"905">>) -> 906;
index (<<"906">>) -> 907;
index (<<"907">>) -> 908;
index (<<"908">>) -> 909;
index (<<"909">>) -> 910;
index (<<"910">>) -> 911;
index (<<"911">>) -> 912;
index (<<"912">>) -> 913;
index (<<"913">>) -> 914;
index (<<"914">>) -> 915;
index (<<"915">>) -> 916;
index (<<"916">>) -> 917;
index (<<"917">>) -> 918;
index (<<"918">>) -> 919;
index (<<"919">>) -> 920;
index (<<"920">>) -> 921;
index (<<"921">>) -> 922;
index (<<"922">>) -> 923;
index (<<"923">>) -> 924;
index (<<"924">>) -> 925;
index (<<"925">>) -> 926;
index (<<"926">>) -> 927;
index (<<"927">>) -> 928;
index (<<"928">>) -> 929;
index (<<"929">>) -> 930;
index (<<"930">>) -> 931;
index (<<"931">>) -> 932;
index (<<"932">>) -> 933;
index (<<"933">>) -> 934;
index (<<"934">>) -> 935;
index (<<"935">>) -> 936;
index (<<"936">>) -> 937;
index (<<"937">>) -> 938;
index (<<"938">>) -> 939;
index (<<"939">>) -> 940;
index (<<"940">>) -> 941;
index (<<"941">>) -> 942;
index (<<"942">>) -> 943;
index (<<"943">>) -> 944;
index (<<"944">>) -> 945;
index (<<"945">>) -> 946;
index (<<"946">>) -> 947;
index (<<"947">>) -> 948;
index (<<"948">>) -> 949;
index (<<"949">>) -> 950;
index (<<"950">>) -> 951;
index (<<"951">>) -> 952;
index (<<"952">>) -> 953;
index (<<"953">>) -> 954;
index (<<"954">>) -> 955;
index (<<"955">>) -> 956;
index (<<"956">>) -> 957;
index (<<"957">>) -> 958;
index (<<"958">>) -> 959;
index (<<"959">>) -> 960;
index (<<"960">>) -> 961;
index (<<"961">>) -> 962;
index (<<"962">>) -> 963;
index (<<"963">>) -> 964;
index (<<"964">>) -> 965;
index (<<"965">>) -> 966;
index (<<"966">>) -> 967;
index (<<"967">>) -> 968;
index (<<"968">>) -> 969;
index (<<"969">>) -> 970;
index (<<"970">>) -> 971;
index (<<"971">>) -> 972;
index (<<"972">>) -> 973;
index (<<"973">>) -> 974;
index (<<"974">>) -> 975;
index (<<"975">>) -> 976;
index (<<"976">>) -> 977;
index (<<"977">>) -> 978;
index (<<"978">>) -> 979;
index (<<"979">>) -> 980;
index (<<"980">>) -> 981;
index (<<"981">>) -> 982;
index (<<"982">>) -> 983;
index (<<"983">>) -> 984;
index (<<"984">>) -> 985;
index (<<"985">>) -> 986;
index (<<"986">>) -> 987;
index (<<"987">>) -> 988;
index (<<"988">>) -> 989;
index (<<"989">>) -> 990;
index (<<"990">>) -> 991;
index (<<"991">>) -> 992;
index (<<"992">>) -> 993;
index (<<"993">>) -> 994;
index (<<"994">>) -> 995;
index (<<"995">>) -> 996;
index (<<"996">>) -> 997;
index (<<"997">>) -> 998;
index (<<"998">>) -> 999;
index (<<"999">>) -> 1000;
index (<<"1000">>) -> 1001;
index (<<"1001">>) -> 1002;
index (<<"1002">>) -> 1003;
index (<<"1003">>) -> 1004;
index (<<"1004">>) -> 1005;
index (<<"1005">>) -> 1006;
index (<<"1006">>) -> 1007;
index (<<"1007">>) -> 1008;
index (<<"1008">>) -> 1009;
index (<<"1009">>) -> 1010;
index (<<"1010">>) -> 1011;
index (<<"1011">>) -> 1012;
index (<<"1012">>) -> 1013;
index (<<"1013">>) -> 1014;
index (<<"1014">>) -> 1015;
index (<<"1015">>) -> 1016;
index (<<"1016">>) -> 1017;
index (<<"1017">>) -> 1018;
index (<<"1018">>) -> 1019;
index (<<"1019">>) -> 1020;
index (<<"1020">>) -> 1021;
index (<<"1021">>) -> 1022;
index (<<"1022">>) -> 1023;
index (<<"1023">>) -> 1024;
index (<<"1024">>) -> 1025.

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

statsmsg_test_ () ->
  [
    { "basic constructor test",
      fun() ->
        C = [{<<"foo">>,<<"bar">>}],
        MIn ={gauge, <<"baz">>, 5},
        S = new (<<"program_id">>, C, [MIn]),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (<<"bar">>, context_value (S, <<"foo">>)),
        ?assertEqual (undefined, context_value (S, <<"bar">>)),
        ?assertEqual (1, num_metrics (S)),
        [ M = #md_metric {} ] = metrics (S),
        ?assertEqual (MIn, metric (M)),
        ?assertEqual (<<"baz">>, metric_name (M)),
        ?assertEqual (gauge, metric_type (M)),
        ?assertEqual (5, metric_value (M)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut)))
      end
    },
    { "basic constructor metric test",
      fun() ->
        C = [{<<"foo">>,<<"bar">>}],
        MIn = new_metric (gauge, <<"baz">>, 5),
        S = new (<<"program_id">>, C, [MIn]),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (<<"bar">>, context_value (S, <<"foo">>)),
        ?assertEqual (undefined, context_value (S, <<"bar">>)),
        ?assertEqual (1, num_metrics (S)),
        [ M = #md_metric {} ] = metrics (S),
        ?assertEqual ({gauge, <<"baz">>, 5}, metric (M)),
        ?assertEqual (<<"baz">>, metric_name (M)),
        ?assertEqual (gauge, metric_type (M)),
        ?assertEqual (5, metric_value (M)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut)))
      end
    },
    { "basic constructor multi-test",
      fun() ->
        C = [],
        S = new (<<"program_id">>,
                 [],
                 [{counter, <<"bob">>, 10}, {gauge, <<"baz">>, 5}]),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (2, num_metrics (S)),
        [ M1 = #md_metric {}, M2 = #md_metric {} ] = metrics (S),
        ?assertEqual (<<"baz">>, metric_name (M2)),
        ?assertEqual (gauge, metric_type (M2)),
        ?assertEqual (5, metric_value (M2)),
        ?assertEqual (<<"bob">>, metric_name (M1)),
        ?assertEqual (counter, metric_type (M1)),
        ?assertEqual (10, metric_value (M1)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut))),
        ?assertEqual (<<"unknown">>, host (SOut))
      end
    },
    { "basic constructor host override",
      fun() ->
        C = [],
        S = new (<<"program_id">>,
                 [],
                 [{counter, <<"bob">>, 10}, {gauge, <<"baz">>, 5}],
                  <<"known">>),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (2, num_metrics (S)),
        [ M1 = #md_metric {}, M2 = #md_metric {} ] = metrics (S),
        ?assertEqual (<<"baz">>, metric_name (M2)),
        ?assertEqual (gauge, metric_type (M2)),
        ?assertEqual (5, metric_value (M2)),
        ?assertEqual (<<"bob">>, metric_name (M1)),
        ?assertEqual (counter, metric_type (M1)),
        ?assertEqual (10, metric_value (M1)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut))),
        ?assertEqual (<<"known">>, host (SOut))
      end
    },
    { "basic constructor host override/timestamp override",
      fun() ->
        C = [],
        S = new (<<"program_id">>,
                 C,
                 [{counter, <<"bob">>, 10}, {gauge, <<"baz">>, 5}],
                  <<"known">>,
                 1234567
                ),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (2, num_metrics (S)),
        [ M1 = #md_metric {}, M2 = #md_metric {} ] = metrics (S),
        ?assertEqual (<<"baz">>, metric_name (M2)),
        ?assertEqual (gauge, metric_type (M2)),
        ?assertEqual (5, metric_value (M2)),
        ?assertEqual (<<"bob">>, metric_name (M1)),
        ?assertEqual (counter, metric_type (M1)),
        ?assertEqual (10, metric_value (M1)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut))),
        ?assertEqual (<<"known">>, host (SOut)),
        ?assertEqual (1234567, collect_time (SOut))
      end
    },
    { "statsets",
      fun() ->
        C = [],
        SS = set_statset(count,5,
               set_statset (sum, 10,
                 set_statset (min, 1,
                   set_statset (max, 3,
                     set_statset (avg, 4,
                       set_statset (median, 6,
                         set_statset (pctl_75, 7,
                           set_statset (pctl_90, 8,
                             set_statset (pctl_95, 9,
                               set_statset (pctl_98, 11,
                                 set_statset (pctl_99, 12,
                                              new_statset()))))))))))),
        S = new (<<"program_id">>, C, [{statset, <<"foo">>, SS}]),
        ?assertEqual (<<"program_id">>, prog_id(S)),
        ?assertEqual (C, context(S)),
        ?assertEqual (1, num_metrics (S)),
        [ M ] = metrics (S),
        ?assertEqual (<<"foo">>, metric_name (M)),
        ?assertEqual (statset, metric_type (M)),
        SO = metric_value (M),
        ?assertEqual (SS, SO),
        ?assertEqual (5,  get_statset (count,SO)),
        ?assertEqual (10, get_statset (sum,SO)),
        ?assertEqual (1,  get_statset (min,SO)),
        ?assertEqual (3,  get_statset (max,SO)),
        ?assertEqual (4,  get_statset (avg,SO)),
        ?assertEqual (6,  get_statset (median,SO)),
        ?assertEqual (7,  get_statset (pctl_75,SO)),
        ?assertEqual (8,  get_statset (pctl_90,SO)),
        ?assertEqual (9,  get_statset (pctl_95,SO)),
        ?assertEqual (11, get_statset (pctl_98,SO)),
        ?assertEqual (12, get_statset (pctl_99,SO)),
        SL = statset_to_list (SO),
        ?assertEqual ({count, 5}, lists:keyfind (count, 1, SL)),
        ?assertEqual ({sum, 10}, lists:keyfind (sum, 1, SL)),
        ?assertEqual ({min, 1}, lists:keyfind (min, 1, SL)),
        ?assertEqual ({max, 3}, lists:keyfind (max, 1, SL)),
        ?assertEqual ({avg, 4}, lists:keyfind (avg, 1, SL)),
        ?assertEqual ({median, 6}, lists:keyfind (median, 1, SL)),
        ?assertEqual ({pctl_75, 7}, lists:keyfind (pctl_75, 1, SL)),
        ?assertEqual ({pctl_90, 8}, lists:keyfind (pctl_90, 1, SL)),
        ?assertEqual ({pctl_95, 9}, lists:keyfind (pctl_95, 1, SL)),
        ?assertEqual ({pctl_98, 11}, lists:keyfind (pctl_98, 1, SL)),
        ?assertEqual ({pctl_99, 12}, lists:keyfind (pctl_99, 1, SL)),
        % for coverage, use an incomplete statset
        SL2 = statset_to_list (set_statset(count,5,new_statset())),
        ?assertEqual ([{count, 5}], SL2),
        % for coverage, check some boundary cases
        % odd, that I allow a float
        SS2S = list_to_binary (
                 statset_to_string (set_statset(count,5,new_statset()))
               ),
        SS3 = statset_from_string (SS2S),
        ?assertEqual ([{count, 5}], statset_to_list (SS3)),
        % can't just assertEqual as things like timestamps are added
        {0, SOut} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(to_lwes(S)),list)),
        ?assertEqual (prog_id (S), prog_id (SOut)),
        ?assertEqual (lists:sort(context(S)), lists:sort(context (SOut))),
        ?assertEqual (lists:sort(metrics(S)), lists:sort(metrics (SOut)))
      end
    },
    { "code coverage",
      fun() ->
        C = [{<<"foo">>,<<"bar">>}],
        MIn ={gauge, <<"baz">>, 5},
        S = new (<<"program_id">>, C, [MIn], <<"known">>, 5, 5),
        ?assertEqual (5, send_time (S)),
        % check adding extra contexts
        SwC = add_context (S, <<"blah">>, <<"boo">>),
        ?assertEqual (<<"boo">>, context_value (SwC, <<"blah">>)),
        SwCL = add_contexts (S, [{<<"blah">>,<<"boo">>},{<<"aa">>,<<"zz">>}]),
        ?assertEqual (<<"boo">>, context_value (SwCL, <<"blah">>)),
        ?assertEqual (<<"zz">>, context_value (SwCL, <<"aa">>)),
        % test that to_lwes of a list returns several events that are correct
        [E1, E2] = to_lwes([S,S]),
        {0, SOut1} =
          from_lwes (lwes_event:from_binary(lwes_event:to_binary(E1),list)),
        {0, SOut2} =
          from_lwes (lwes_event:from_binary(lwes_event:to_binary(E2),list)),
        ?assertEqual (SOut1, SOut2),
        % test bad stat sets, as well as list to binary bit
        ?assertEqual (undefined, statset_from_string ("::")),
        ?assertEqual (gauge, string_to_type ("gauge")),
        ?assertEqual (gauge, string_to_type (<<"gauge">>)),
        NewEvent = E1#lwes_event {
                     attrs = [{?LWES_INT_64, ?MD_RECEIPT_TIME, 5}
                              | E1#lwes_event.attrs ]},
        ?assertEqual ({5, S},
          from_lwes(
              lwes_event:from_binary(lwes_event:to_binary(NewEvent),list))),
        % hackery to test the cases where we don't have send_time
        % or collect_time
        S2 = new (<<"program_id">>, C, [MIn], <<"known">>),
        E3 = to_lwes (S2),
        NewEventMinusTimes = E3#lwes_event {
                               attrs = lists:keydelete (?MD_SEND_TIME, 2,
                                       lists:keydelete (?MD_COLLECT_TIME, 2,
                                                        E3#lwes_event.attrs))
                             },
        ?assertEqual ({0, S2},
                      from_lwes (
                        lwes_event:from_binary(
                          lwes_event:to_binary(NewEventMinusTimes),
                          list))),
        R =
        lists:foldl(fun(N,A) ->
                      (N+1) =:= index (list_to_binary(integer_to_list(N)))
                      andalso A
                    end,
                    true,
                    lists:seq (0,1024)),
        ?assertEqual (true, R)
      end
    },
    { "many metrics",
      fun() ->
        mondemand_config:init(),
        C = [{<<"foo">>,<<"bar">>}],
        MaxMetrics = mondemand_config:max_metrics (),
        MetricsCount = MaxMetrics + 1,
        E2K0Name = list_to_binary(string:concat("baz", integer_to_list(MetricsCount))),
        MIn = lists:map(fun(X) ->
                          {
                            gauge,
                            list_to_binary(string:concat("baz", integer_to_list(X))),
                            X
                          } end,
                        lists:seq(1, MetricsCount)),
        S = new (<<"program_id">>, C, MIn, <<"known">>, 5, 5),
        ?assertEqual (5, send_time (S)),
        % check adding extra contexts
        SwC = add_context (S, <<"blah">>, <<"boo">>),
        ?assertEqual (<<"boo">>, context_value (SwC, <<"blah">>)),
        SwCL = add_contexts (S, [{<<"blah">>,<<"boo">>},{<<"aa">>,<<"zz">>}]),
        ?assertEqual (<<"boo">>, context_value (SwCL, <<"blah">>)),
        ?assertEqual (<<"zz">>, context_value (SwCL, <<"aa">>)),
        % test that to_lwes of long statsmsg  returns several events that are correct
        [E1, E2] = to_lwes(S),
        % test that the metric counts and first metric in events match expected values
        ?assert (lists:member({?LWES_U_INT_16, <<"num">>, MaxMetrics}, E1#lwes_event.attrs)),
        ?assert (lists:member({?LWES_STRING, <<"k0">>, <<"baz1">>}, E1#lwes_event.attrs)),
        ?assert (lists:member({?LWES_STRING, <<"t0">>, <<"gauge">>}, E1#lwes_event.attrs)),
        ?assert (lists:member({?LWES_INT_64, <<"v0">>, 1}, E1#lwes_event.attrs)),
        ?assert (lists:member({?LWES_U_INT_16, <<"num">>, 1}, E2#lwes_event.attrs)),
        ?assert (lists:member({?LWES_STRING, <<"k0">>, E2K0Name}, E2#lwes_event.attrs)),
        ?assert (lists:member({?LWES_STRING, <<"t0">>, <<"gauge">>}, E2#lwes_event.attrs)),
        ?assert (lists:member({?LWES_INT_64, <<"v0">>, MetricsCount}, E2#lwes_event.attrs)),
        % test that the prog_id and contexts match in the two lwes events
        {0, S1} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(E1),list)),
        {0, S2} =
          from_lwes (
              lwes_event:from_binary(lwes_event:to_binary(E2),list)),
        ?assertEqual (prog_id (S1), prog_id (S2)),
        ?assertEqual (lists:sort(context(S1)), lists:sort(context (S2)))
      end
    }
  ].

-endif.
