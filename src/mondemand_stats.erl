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

-module (mondemand_stats).
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
          context/1,
          context_val/2,
          metrics/1,
          metric/1,
          metric_type/1,
          metric_name/1,
          metric_value/1,
          to_lwes/1,
          from_lwes/1,
          statset_from_string/1,
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
  ValidatedMetrics = [ #metric { type = T, key = K, value = V }
                       || { T, K, V }
                       <- Metrics ],
  new (ProgId, Context, ValidatedMetrics, Host);
new (ProgId, Context, Metrics, Host) ->
  new (ProgId, Context, Metrics, Host, undefined).

new (ProgId, Context, Metrics, Host, Timestamp) ->
  #stats_msg { send_time = Timestamp,
               prog_id = ProgId,
               host = Host,
               num_context = length (Context),
               context = Context,
               num_metrics = length (Metrics),
               metrics = Metrics
             }.

new_statset () -> #statset {}.

set_statset (count, Count, S = #statset{}) -> S#statset {count = Count};
set_statset (sum, Sum, S = #statset{}) -> S#statset {sum = Sum};
set_statset (min, Min, S = #statset{}) -> S#statset {min = Min};
set_statset (max, Max, S = #statset{}) -> S#statset {max = Max};
set_statset (avg, Avg, S = #statset{}) -> S#statset {avg = Avg};
set_statset (median, Median, S = #statset{}) -> S#statset {median = Median};
set_statset (pctl_75, Pctl75, S = #statset{}) -> S#statset {pctl_75 = Pctl75};
set_statset (pctl_90, Pctl90, S = #statset{}) -> S#statset {pctl_90 = Pctl90};
set_statset (pctl_95, Pctl95, S = #statset{}) -> S#statset {pctl_95 = Pctl95};
set_statset (pctl_98, Pctl98, S = #statset{}) -> S#statset {pctl_98 = Pctl98};
set_statset (pctl_99, Pctl99, S = #statset{}) -> S#statset {pctl_99 = Pctl99}.

get_statset (count, S = #statset{}) -> S#statset.count;
get_statset (sum, S = #statset{}) -> S#statset.sum;
get_statset (min, S = #statset{}) -> S#statset.min;
get_statset (max, S = #statset{}) -> S#statset.max;
get_statset (avg, S = #statset{}) -> S#statset.avg;
get_statset (median, S = #statset{}) -> S#statset.median;
get_statset (pctl_75, S = #statset{}) -> S#statset.pctl_75;
get_statset (pctl_90, S = #statset{}) -> S#statset.pctl_90;
get_statset (pctl_95, S = #statset{}) -> S#statset.pctl_95;
get_statset (pctl_98, S = #statset{}) -> S#statset.pctl_98;
get_statset (pctl_99, S = #statset{}) -> S#statset.pctl_99.

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
  ProgId = dict:fetch (?PROG_ID, Data),
  SenderIP = dict:fetch (?SENDER_IP, Data),
  SenderPort = dict:fetch (?SENDER_PORT, Data),
  ReceiptTime = dict:fetch (?RECEIPT_TIME, Data),
  SendTime =
    case dict:find (?SEND_TIME, Data) of
      error -> undefined;
      {ok, T} -> T
    end,
  {Host, NumContexts, Context} = construct_context (Data),
  {NumMetrics, Metrics} = construct_metrics (Data),

  #stats_msg {
    send_time = SendTime,
    receipt_time = ReceiptTime,
    sender_ip = SenderIP,
    sender_port = SenderPort,
    prog_id = ProgId,
    host = Host,
    num_context = NumContexts,
    context = Context,
    num_metrics = NumMetrics,
    metrics = Metrics
  }.

construct_metrics (Data) ->
  Num =
    case dict:find (?STATS_NUM, Data) of
      error -> 0;
      {ok, C} -> C
    end,
  { Num,
    lists:map (
      fun (N) ->
          K = dict:fetch (mondemand_util:metric_name_key (N), Data),
          V = dict:fetch (mondemand_util:metric_value_key (N), Data),
          T = string_to_type (
                dict:fetch (
                  mondemand_util:metric_type_key (N),
                  Data
                )
              ),
          #metric { key = K,
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

construct_context (Data) ->
  Num =
    case dict:find (?CTXT_NUM, Data) of
      error -> 0;
      {ok, C} -> C
    end,
  { Host, Context } =
    lists:foldl ( fun (N, {H, A}) ->
                    K = dict:fetch (mondemand_util:context_name_key (N), Data),
                    V = dict:fetch (mondemand_util:context_value_key (N), Data),
                    case K of
                      ?STATS_HOST -> { V, A };
                      _ -> { H, [ {K, V} | A ] }
                    end
                  end,
                  { <<"unknown">>, [] },
                  lists:seq (1,Num)
                ),
  { Host, length (Context), lists:keysort (1, Context) }.

to_lwes (L) when is_list (L) ->
  lists:map (fun to_lwes/1, L);

to_lwes (#stats_msg { send_time = SendTimeIn,
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
    name  = ?STATS_EVENT,
    attrs = lists:flatten (
              [ { ?LWES_STRING, ?PROG_ID, ProgId },
                { ?LWES_INT_64, ?SEND_TIME, SendTime },
                { ?LWES_U_INT_16, ?STATS_NUM, NumMetrics },
                lists:zipwith (fun metric_to_lwes/2,
                               lists:seq (1, NumMetrics),
                               Metrics),
                { ?LWES_U_INT_16, ?CTXT_NUM, NumContexts + 1},
                lists:zipwith (fun context_to_lwes/2,
                               lists:seq (1, NumContexts),
                               Context),
                context_to_lwes (NumContexts+1, { ?STATS_HOST, Host })
              ]
            )
  }.

context_to_lwes (ContextIndex, {ContextKey, ContextValue}) ->
  [ { ?LWES_STRING,
      mondemand_util:context_name_key (ContextIndex),
      mondemand_util:stringify (ContextKey)
    },
    { ?LWES_STRING,
      mondemand_util:context_value_key (ContextIndex),
      mondemand_util:stringify (ContextValue)
    }
  ].

metric_to_lwes (MetricIndex,
                #metric { key = Name, type = statset, value = Value }) ->
  [ { ?LWES_STRING,
      mondemand_util:metric_name_key (MetricIndex),
      mondemand_util:stringify (Name) },
    { ?LWES_STRING,
      mondemand_util:metric_type_key (MetricIndex),
      type_to_string (statset)
    },
    { ?LWES_STRING,
      mondemand_util:metric_value_key (MetricIndex),
      statset_to_string (Value)
    }
  ];
metric_to_lwes (MetricIndex,
                #metric { key = Name, type = Type, value = Value }) ->
  [ { ?LWES_STRING,
      mondemand_util:metric_name_key (MetricIndex),
      mondemand_util:stringify (Name)
    },
    { ?LWES_STRING,
      mondemand_util:metric_type_key (MetricIndex),
      type_to_string (Type)
    },
    { ?LWES_INT_64,
      mondemand_util:metric_value_key (MetricIndex),
      Value
    }
  ].

prog_id (#stats_msg { prog_id = ProgId }) -> ProgId.
host (#stats_msg { host = Host }) -> Host.
context (#stats_msg { context = Context }) -> Context.
context_val (#stats_msg { context = Context }, ContextKey) ->
  context_find (ContextKey, Context, undefined).

context_find (Key, Context, Default) ->
  case lists:keyfind (Key, 1, Context) of
    false -> Default;
    {_, H} -> H
  end.

metrics (#stats_msg { metrics = Metrics }) ->
  Metrics.

metric_name (#metric { key = Name }) -> Name.
metric_type (#metric { type = Type }) -> Type.
metric_value (#metric { value = Value }) -> Value.
metric (#metric { key = Name, type = Type, value = Value }) ->
  { Type, Name, Value }.

statset_from_string (L) when is_list(L) ->
  statset_from_string (list_to_binary (L));
statset_from_string (B) when is_binary(B) ->
  case re:split (B, ?STATSET_SEP) of
    [ Count, Sum, Min, Max, Avg, Median,
      Pctl75, Pctl90, Pctl95, Pctl98, Pctl99] ->
      #statset {
        count = integerify (Count),
        sum = integerify (Sum),
        min = integerify (Min),
        max = integerify (Max),
        avg = integerify (Avg),
        median = integerify (Median),
        pctl_75 = integerify (Pctl75),
        pctl_90 = integerify (Pctl90),
        pctl_95 = integerify (Pctl95),
        pctl_98 = integerify (Pctl98),
        pctl_99 = integerify (Pctl99)
      };
    _ ->
      undefined
  end.

statset_to_string (StatSet = #statset {}) ->
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

integerify ("") -> undefined;
integerify (<<>>) -> undefined;
integerify (I) when is_integer (I) ->
  I;
integerify (F) when is_float (F) ->
  trunc (F);
integerify (B) when is_binary (B) ->
  integerify (binary_to_list (B));
integerify (L) when is_list (L) ->
  try list_to_integer (L) of
    I -> I
  catch
    _:_ -> undefined
  end.

floatify ("") -> undefined;
floatify (<<>>) -> undefined;
floatify (I) when is_integer (I) ->
  I + 0.0;
floatify (F) when is_float (F) ->
  F;
floatify (B) when is_binary (B) ->
  floatify (binary_to_list (B));
floatify (L) when is_list (L) ->
  try list_to_float (L) of
    F -> F
  catch
    _:_ ->
      case integerify (L) of
        undefined -> undefined;
        I -> I + 0.0
      end
  end.

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
