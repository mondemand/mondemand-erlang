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
          new_statset/0,
          set_statset/3,
          get_statset/2,
          prog_id/1,
          host/1,
          timestamp/1,
          context/1,
          context_value/2,
          add_contexts/2,
          add_context/3,
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
  Host = net_adm:localhost (),
  new (ProgId, Context, Metrics, Host).
new (ProgId, Context, Metrics = [{_,_,_}|_], Host) ->
  ValidatedMetrics = [ #md_metric { type = T, key = K, value = V }
                       || { T, K, V }
                       <- Metrics ],
  new (ProgId, Context, ValidatedMetrics, Host);
new (ProgId, Context, Metrics, Host) ->
  new (ProgId, Context, Metrics, Host, undefined).

new (ProgId, Context, Metrics, Host, Timestamp) ->
  #md_stats_msg { send_time = Timestamp,
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

% the ESF for mondemand stats messages is as follows
% MonDemand::StatsMsg
% {
%   string prog_id;    # program identifier
%   int64  send_time;  # time for stats in milliseconds since epoch
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
  % here's the name of the program which originated the metric
  ProgId = dict:fetch (?MD_PROG_ID, Data),
  SendTime =
    case dict:find (?MD_SEND_TIME, Data) of
      error -> undefined;
      {ok, T} -> T
    end,
  {Host, NumContexts, Context} = mondemand_util:context_from_lwes (Data),
  {NumMetrics, Metrics} = metrics_from_lwes (Data),

  #md_stats_msg {
    send_time = SendTime,
    prog_id = ProgId,
    host = Host,
    num_context = NumContexts,
    context = Context,
    num_metrics = NumMetrics,
    metrics = Metrics
  }.

metrics_from_lwes (Data) ->
  Num =
    case dict:find (?MD_NUM, Data) of
      error -> 0;
      {ok, C} -> C
    end,
  { Num,
    lists:map (
      fun (N) ->
          K = dict:fetch (metric_name_key (N), Data),
          V = dict:fetch (metric_value_key (N), Data),
          T = string_to_type (dict:fetch (metric_type_key (N), Data)),
          #md_metric { key = K,
                       type = T,
                       value = case T of
                                 statset -> statset_from_string (V);
                                 _ -> V
                               end
                  }
      end,
      lists:seq (1,Num)
    )
  }.

to_lwes (L) when is_list (L) ->
  lists:map (fun to_lwes/1, L);

to_lwes (#md_stats_msg { send_time = SendTimeIn,
                         prog_id = ProgId,
                         host = Host,
                         num_context = NumContexts,
                         context = Context,
                         num_metrics = NumMetrics,
                         metrics = Metrics
                       }) ->
  SendTime =
    case SendTimeIn of
      undefined -> mondemand_util:millis_since_epoch();
      T -> T
    end,
  #lwes_event {
    name  = ?MD_STATS_EVENT,
    attrs = lists:flatten (
              [ { ?LWES_STRING, ?MD_PROG_ID, ProgId },
                { ?LWES_INT_64, ?MD_SEND_TIME, SendTime },
                { ?LWES_U_INT_16, ?MD_NUM, NumMetrics },
                lists:zipwith (fun metric_to_lwes/2,
                               lists:seq (1, NumMetrics),
                               Metrics),
                mondemand_util:context_to_lwes (Host, NumContexts, Context)
              ]
            )
  }.

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

timestamp (#md_stats_msg { send_time = SendTime }) -> SendTime.

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
num_or_empty (F) when is_float (F) ->
  float_to_list (F);
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


%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").


-endif.
