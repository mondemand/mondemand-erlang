-module (mondemand_statdb).

-include ("mondemand_internal.hrl").

% The mondemand client stat db is meant to store stats as they are being
% collected.  The types of stats stored are
%
% counters - these can be incremented and decremented up to the limit accepted
% by lwes (INT 64)
% gauges - these can be set to a value in the INT 64 range and will be
% capped and the top and bottom of the range
% stat sets - these are sampled collections of samples for the given interval
% from which various statistics can be gathered at emit time.
%
% There are 4 ets tables in the DB
%
% 1. a config table which keeps track of configuration for a metric
% 2. a counter|gauge table which keeps all metrics set by
%    increment/decrement/set
% 3. a current stat set table which keeps track of current stat sets
% 4. a previous stat set table which contains the previous interval of
%    stat sets
%
% In order to make stat set lookup easier, the following scheme will be
% used.
%
% At startup
%   1. create 60 ets tables, called 'md_min_00' through 'md_min_59'
%
% When a stat is updated
%   1. update in table for current minute
%
% At a timer which fires every 60 seconds does the following
%   1. looks at current minute
%      walks backward finding oldest minute with data
%        if it's the previous minute, then
%           flush it
%        else
%           empty it
%           look at next oldest minute with data
%

%% API
-export([ start_link/0,

          create_counter/2,
          create_counter/3,
          increment/2,
          increment/3,
          increment/4,

          create_gauge/2,
          create_gauge/3,
          set/3,
          set/4,

          create_sample_set/3,
          create_sample_set/4,
          create_sample_set/6,
          add_sample/3,
          add_sample/4,
          all_sample_set_stats/0,

          map/1,
          statset/6,
          flush_stats_sets/0,
          config/0,
          metrics/0,
          to_stats/1,
          all/0,
          reset_stats/0
        ]).

%% gen_server callbacks
-export ( [ init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3
          ]).

-record (state,  {timer}).
-record (config, {key,
                  type,
                  description,
                  table,
                  max_sample_size,
                  statistics
                 }).

-define (STATS_TABLE,  md_stats).
-define (CONFIG_TABLE, md_config).

-define (DEFAULT_MAX_SAMPLE_SIZE, 10).
-define (DEFAULT_STATS, [min, max, sum, count]).
-define (ALL_STATS, [min, max, sum, count, avg, median,
                     pctl_75, pctl_90, pctl_95, pctl_98, pctl_99]).

-define (CONFIG_KEY_INDEX, #config.key).
-define (CONFIG_TYPE_INDEX, #config.type).
-define (CONFIG_DESCRIPTION_INDEX, #config.description).

-define (METRIC_KEY_INDEX, #metric.key).
-define (METRIC_TYPE_INDEX, #metric.type).
-define (METRIC_VALUE_INDEX, #metric.value).

-define (STATSET_KEY_INDEX,   1).
-define (STATSET_MAX_INDEX,   2).
-define (STATSET_COUNT_INDEX, 3).
-define (STATSET_SUM_INDEX,   4).


% LWES uses a signed int 64, so use that as the max metric value, so we
% reset at the same rate
-define (MAX_METRIC_VALUE, 9223372036854775807).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

create_counter (Key, Description) ->
  create_counter (Key, Description, 0).

create_counter (Key, Description, Amount) ->
  add_new_config (Key, counter, Description),
  ets:insert_new (?STATS_TABLE, #metric {key = Key, value = Amount}).

increment (ProgId, Key) ->
  increment (ProgId, Key, 1, []).

increment (ProgId, Key, Amount) when is_integer (Amount) ->
  increment (ProgId, Key, Amount, []).

increment (ProgId, Key, Amount, Context)
  when is_integer (Amount), is_list (Context) ->
  InternalKey = calculate_key (ProgId, Context, counter, Key),
  try_update_counter (InternalKey, Amount).

try_update_counter (Key, Amount) ->
  % LWES is sending int64 values, so wrap at the max int64 integer back to
  % zero
  try ets:update_counter (?STATS_TABLE, Key,
                          {?METRIC_VALUE_INDEX, Amount,
                           ?MAX_METRIC_VALUE, 0}) of
    _ -> ok
  catch
    error:badarg ->
      % the key probably doesn't exist, so create with an empty description
      case create_counter (Key, "", Amount) of
        true -> ok;
        false ->
          % create failed, so someone else probably created it, so just
          % try again this time without the catch
          ets:update_counter (?STATS_TABLE, Key,
                              {?METRIC_VALUE_INDEX, Amount,
                               ?MAX_METRIC_VALUE, 0}),
          ok
      end
  end.

create_gauge (Key, Description) ->
  create_gauge (Key, Description, 0).

create_gauge (Key, Description, Amount) ->
  add_new_config (Key, gauge, Description),
  ets:insert (?STATS_TABLE, #metric {key = Key, value = Amount}).

set (ProgId, Key, Amount) ->
  set (ProgId, Key, Amount, []).

set (ProgId, Key, Amount, Context) ->
  InternalKey = calculate_key (ProgId, Context, gauge, Key),

  % if we would overflow a gauge, instead of going negative just leave it
  % at the max value and return false
  {Overflowed, RealAmount} =
    case Amount =< ?MAX_METRIC_VALUE of
      true -> {false, Amount};
      false -> {true, ?MAX_METRIC_VALUE}
    end,

  case try_update_gauge (InternalKey, RealAmount) of
    false -> {error, internal_db};
    true ->
      case Overflowed of
        true -> {error, overflow};
        false -> ok
      end
  end.

try_update_gauge (Key, Amount) ->
  % use update_element for gauges as we only want the last value
  case
    ets:update_element (?STATS_TABLE, Key, [{?METRIC_VALUE_INDEX, Amount}])
  of
    true -> true;
    false ->
      % the key probably doesn't exist, so create with an empty description
      case create_gauge (Key, "", Amount) of
        true -> true;
        false ->
          % create failed, so someone else probably created it, so just
          % try again this time without the case
          ets:update_element (?STATS_TABLE, Key, [{?METRIC_VALUE_INDEX, Amount}])
      end
  end.

create_sample_set (ProgId, Key, Description) ->
  create_sample_set (ProgId, Key, [], Description).

create_sample_set (ProgId, Key, Context, Description) ->
  create_sample_set (ProgId, Key, Context, Description,
                     ?DEFAULT_MAX_SAMPLE_SIZE, ?DEFAULT_STATS).

create_sample_set (ProgId, Key, Context, Description, Max, Stats) ->
  InternalKey = calculate_key (ProgId, Context, sampleset, Key),
  create_sample_set_internal (InternalKey, Description, Max, Stats).

create_sample_set_internal (InternalKey = {_,_,_,_}) ->
  #config { max_sample_size = Max, statistics = Stats } =
    lookup_config (InternalKey),
  create_sample_set_internal (InternalKey, "", Max, Stats).

create_sample_set_internal (InternalKey, Description, Max, Stats) ->
  add_new_config (InternalKey, sampleset, Description, Max, Stats),
  % Creates a new entry of the form
  % { Key, Count, Sum, Sample1 ... SampleMax }
  ets:insert_new (minute_tab (mondemand_util:current_minute()),
    list_to_tuple (
      [ InternalKey, Max, 0, 0
        | [ 0 || _ <- lists:seq (1, Max) ]
      ])
  ).

lookup_config (Key) ->
  case ets:lookup (?CONFIG_TABLE, Key) of
    [] -> lookup_default_config ();
    [C = #config { }] -> C
  end.

lookup_default_config () ->
  case ets:lookup (?CONFIG_TABLE, '$default_config') of
    [C = #config {}] -> C;
    [] -> undefined
  end.

add_new_config (Key, Type, Description) ->
  C = lookup_default_config (),
  NewConfig = C#config { key = Key,
                         type = Type,
                         description = Description },
  ets:insert_new (?CONFIG_TABLE, NewConfig).

add_new_config (Key, Type, Description, Max, Stats) ->
  C = lookup_default_config (),
  NewConfig = C#config { key = Key,
                         type = Type,
                         description = Description,
                         max_sample_size = Max,
                         statistics = normalize_stats (Stats) },
  ets:insert_new (?CONFIG_TABLE, NewConfig),
  C#config.table.

update_sampleset (Table, Key, Value) ->
  ets:update_counter (Table, Key,
                    [{?STATSET_MAX_INDEX,0},    % fetch the index
                     {?STATSET_COUNT_INDEX,1},  % increment the count by 1
                     {?STATSET_SUM_INDEX,Value} % increment the sum by Value
                    ]).

try_update_sampleset (Table, InternalKey, Value) ->
  % attempt an update, this will fail the first time unless someone
  % has created the entry
  try update_sampleset (Table, InternalKey, Value) of
    [Max, UpdateCount, _] -> [Max, UpdateCount]
  catch
    error:badarg ->
      % catch the failure, create the entry, then try the update again,
      % if it crashes a second time we'll just let it go
      create_sample_set_internal (InternalKey),
      [M, UC,_] = update_sampleset (Table, InternalKey, Value),
      [M, UC]
  end.

add_sample (ProgId, Key, Value) ->
  add_sample (ProgId, Key, Value, []).

add_sample (ProgId, Key, Value, Context) ->
  InternalKey = calculate_key (ProgId, Context, sampleset, Key),

  Tid = minute_tab (mondemand_util:current_minute()),

  % First we'll update the count and sum, and we care about the count
  % as it gives us an index into the list of samples
  [Max, UpdateCount] = try_update_sampleset (Tid, InternalKey, Value),

  % If we've already collected the max samples we'll generate a random
  % index to possibly replace
  IndexToUpdate =
    case UpdateCount =< Max of
      true ->
        UpdateCount;
      false ->
        % we'll replace with a probability of SampleCount / UpdateCount
        % so for instance if we have a max of 100 samples and we've collected
        % 105 samples, we'll want to replace with a probability of 100 / 105,
        % so if we generate a random number between 1 and 105, then if it
        % is less than 100 use that as the new index.
        %
        % But say if you have collected 500 updates for 100 slots it will
        % be replaced 1/5 percent of the time.
        IndexToReplace = crypto:rand_uniform (1,UpdateCount),
        case IndexToReplace =< Max of
          true -> IndexToReplace;
          false -> skip
        end
    end,

  % finally we'll update the value
  case IndexToUpdate of
    skip -> true;
    I -> ets:update_element (Tid, InternalKey, {I+?STATSET_SUM_INDEX,Value})
  end.

all_sample_set_stats () ->
  ?ALL_STATS.

config () ->
  ets:tab2list (?CONFIG_TABLE).

% I want to iterate over the config table, collapsing all metrics for a
% particular program id and context into a group so they can all be processed
% together.
%
% I want to use ets:first/1 and ets:next/2 so I can eventually set some rules
% about how they are processed in terms of time spent overall
%
map (Function) ->
  % there a couple of things we'd like to not recalculate but are probably
  % used over and over, so get them here and pass them through
  Host = net_adm:localhost (),
  PreviousMinute = prev_min (mondemand_util:current_minute()),
  StatsSetTable = minute_tab (PreviousMinute),

  case ets:first (?CONFIG_TABLE) of
    '$end_of_table' -> [];
    FirstKey ->
      % put the first into the current list to collapse
      map (Function, {Host, PreviousMinute, StatsSetTable}, [FirstKey])
  end.

% need to skip the config as that's not what we want to map over
map (Function, State, [Key = '$default_config']) ->
  case ets:next (?CONFIG_TABLE, Key) of
    '$end_of_table' -> [];
    NextKey ->
      map (Function, State, [NextKey])
  end;
map (Function, State,
     % match out the ProgId and Context from the current collapsed list
     AllKeys = [LastKey = {ProgId,Context,_,_}|_]) ->

  case ets:next (?CONFIG_TABLE,LastKey) of
    '$end_of_table' ->
      % we hit the end of the table, so just call the function with the
      % current set of matched keys
      Function (construct_stats_msg (AllKeys, State));
    Key = {ProgId, Context, _, _} ->
      % this particular entry has the same ProgId and Context, so add it
      % to the list of keys which are grouped together
      map (Function, State, [Key|AllKeys]);
    NonMatchingKey ->
      % the key didn't match, so call the function with the current set
      Function (construct_stats_msg (AllKeys, State)),
      % then use this key for the next iteration
      map (Function, State, [NonMatchingKey])
  end.

construct_stats_msg (AllKeys = [{ProgId, Context,_,_}|_],
                     {Host, _, Table}) ->
  Metrics = [ lookup_metric (I, Table) || I <- AllKeys ],
  mondemand_stats:new (ProgId, Context, Metrics, Host).

% this function looks up metrics from the different internal DB's and
% unboxes them
lookup_metric (InternalKey = {_,_,Type,Key}, Table) ->
  case Type of
    I when I =:= counter; I =:= gauge ->
      case ets:lookup (?STATS_TABLE, InternalKey) of
        [] ->
          #metric { key = Key, type = I, value = 0 };
        [#metric {value = V}] ->
          #metric { key = Key, type = I, value = V }
      end;
    I when I =:= sampleset ->
      #config { statistics = Stats } = lookup_config (InternalKey),
      case ets:lookup (Table, InternalKey) of
        [] ->
          % special case, for filling out an empty statset
          #metric { key = Key, type = I, value = statset (0, 0, 0, 0, [], Stats) };
        [Entry] ->
          #metric { key = Key, type = I, value = ets_to_statset (Entry, Stats) }
      end
  end.

metrics () ->
  io:format ("~1s ~-21s ~-35s ~-20s~n",["t", "prog_id", "key", "value"]),
  io:format ("  ~-78s~n",["contexts"]),
  io:format ("~1s ~-21s ~-35s ~-20s~n",["-","---------------------",
                                    "-----------------------------------",
                                    "--------------------"]),

  [
    begin
      io:format ("~1s ~-21s ~-35s ~-20b~n",
                 [
                   case Type of
                     counter -> "c";
                     gauge -> "g"
                   end,
                   case ProgId of
                     undefined -> "unknown";
                     P ->  mondemand_util:stringify (P)
                   end,
                   mondemand_util:stringify (Key),
                   Value
                 ]),
      lists:foreach (fun ({CKey, CValue}) ->
                           io:format ("  {~s, ~s}~n", [
                               mondemand_util:stringify (CKey),
                               mondemand_util:stringify (CValue)
                             ])
                     end, Context)
    end
    || #metric { key = {ProgId, Context, Type, Key}, value = Value }
    <- lists:sort (ets:tab2list (?STATS_TABLE))
  ],
  ok.

to_stats (Func) ->
  AllEts = ets:tab2list (?STATS_TABLE),
  Host = net_adm:localhost (),

  % struct in ets is
  %  { { ProgId, Context, Type, Key }, Value }
  % but I want to send this in the fewest number of mondemand-tool calls
  % so I need to get all {ProgId, Context} pairs, then unique sort them,
  % after that send_stats for all stats which match ProgId/Context pair
  [ begin
      Metrics =
        [ { T, K, V }
          || #metric { key = { P2, C2, T, K }, value = V }
          <- AllEts,
             P2 =:= ProgId,
             C2 =:= Context ],
      Func (mondemand_stats:new (ProgId, Context, Metrics, Host))
    end
    || { ProgId, Context }
    <- lists:usort ( [ {EtsProgId, EtsContext}
                       || #metric { key = {EtsProgId, EtsContext, _, _} }
                       <- AllEts
                     ])
  ].

%statset_tid () ->
%  gen_server:call (?MODULE, {statsset_tid}).
%statsets () ->
%  Tid = statset_tid (),
%  AllEts = get_stats_sets (Tid),
%  io:format ("~p",[AllEts]),
%  ok.

%-=====================================================================-
%-                        gen_server callbacks                         -
%-=====================================================================-
init([]) ->
  % Create a stats set table for each minute.  Only 2 tables should actually
  % ever have data in them.
  lists:foreach (fun (Min) ->
                   ets:new (minute_tab(Min), [ set,
                                               public,
                                               named_table,
                                               {write_concurrency, true},
                                               {read_concurrency, false},
                                               {keypos, ?STATSET_KEY_INDEX}
                                             ])
                 end,
                 lists:seq (0,59)
                ),

  % keep track of custom config for stats sets
  ets:new (?CONFIG_TABLE, [ ordered_set,
                            public,
                            named_table,
                            {keypos, ?CONFIG_KEY_INDEX},
                            {read_concurrency, true},
                            {write_concurrency, false}
                          ]),

  % make sure there's a default config
  ets:insert_new (?CONFIG_TABLE,
                  #config { key = '$default_config',
                            type = config,
                            max_sample_size = ?DEFAULT_MAX_SAMPLE_SIZE,
                            statistics = ?DEFAULT_STATS }),

  % this table is for counters and gauges
  ets:new (?STATS_TABLE, [ set,
                           public,
                           named_table,
                           {write_concurrency, true},
                           {read_concurrency, false},
                           {keypos, ?METRIC_KEY_INDEX}
                         ]),
  % shoot for starting flushes a second after the next minute starts
  {ok, _} = timer:send_after (
              mondemand_util:millis_to_next_round_minute() + 1000,
              ?MODULE,
              flush_stats_sets),
  {ok, #state {}}.


handle_call (_Request, _From, State) ->
  {reply, ok, State}.

handle_cast (_Request, State) ->
  {noreply, State}.

handle_info (flush_stats_sets, State = #state {timer = undefined}) ->
  {ok, TRef} = timer:send_interval (60000, ?MODULE, flush_stats_sets),
  flush_stats_sets (),
  {noreply, State#state {timer = TRef}};
handle_info (flush_stats_sets, State = #state {}) ->
  flush_stats_sets (),
  {noreply, State#state {}};
handle_info (_Info, State) ->
  {noreply, State}.

terminate (_Reason, _State) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.


%-=====================================================================-
%-                        Internal Functions                           -
%-=====================================================================-
%update_all (Tid) ->
%  update_all (ets:first (?CONFIG_TABLE), Tid).
%
%update_all ('$end_of_table', _) -> ok;
%update_all (Key, Tid) ->
%  ets:update_element (?CONFIG_TABLE, Key, [{?CONFIG_TABLE_INDEX, Tid}]),
%  update_all (ets:next (?CONFIG_TABLE, Key), Tid).
%
%update_default (Tid) ->
%  ets:update_element (?CONFIG_TABLE, '$default_config',
%                      [{?CONFIG_TABLE_INDEX, Tid}]).

normalize_stats (undefined) -> ?DEFAULT_STATS;
normalize_stats ("") -> ?DEFAULT_STATS;
normalize_stats (L) when is_list (L) ->
  lists:flatten (lists:map (fun normalize/1, L)).

normalize (L) when is_list (L) ->
  normalize (list_to_existing_atom (L));
normalize (B) when is_binary (B) ->
  normalize (binary_to_existing_atom (B, utf8));
normalize (min) -> min;
normalize (max) -> max;
normalize (avg) -> avg;
normalize (median) -> median;
normalize (pctl_75) -> pctl_75;
normalize (pctl_90) -> pctl_90;
normalize (pctl_95) -> pctl_95;
normalize (pctl_98) -> pctl_98;
normalize (pctl_99) -> pctl_99;
normalize (sum) -> sum;
normalize (count) -> count;
normalize (_) -> "".

prev_min (Min) ->
  case Min - 1 of
    -1 -> 59;
    Previous -> Previous
  end.

flush_stats_sets () ->
  MinuteToFlush = minute_tab (prev_min (mondemand_util:current_minute())),
%  error_logger:info_msg ("flushing ~p",[MinuteToFlush]),
  process_stats_sets (MinuteToFlush,
                      fun (S) ->
                        io:format ("process ~p~n",[S])
                      end
  ),
  ok.
%  TabData = get_stats_sets (Table),
%  ets:delete (Table),
%  TabData.


ets_to_statset (Data, Stats) ->
  % this needs to match the create side
  [ _, MaxSize, Count, Sum | RawSamples ] = tuple_to_list (Data),

  % if we have less than the MaxSize, we shorten the list
  { Samples, SamplesCount } =
    case Count =< MaxSize of
      false -> { RawSamples, MaxSize };
      true ->
        {RS, _ } = lists:split (Count, RawSamples),
        {RS, Count}
    end,

  % now we sort and turn into a tuple so we can use indices
  Sorted = list_to_tuple (lists:sort (Samples)),
  ScaledCount = case SamplesCount == 0 of
                  true -> 0;
                  false ->
                    % we'll be truncating so need to round up
                    SamplesCount+0.5
                end,

  % we fold over the list of stats we are calculating and
  % construct a #statset{} record
  statset (Count, Sum, SamplesCount, ScaledCount, Sorted, Stats).

statset (Count, Sum, SamplesCount, ScaledCount, Sorted, Stats) ->
  {_,_,_,_,_,StatSet} =
    lists:foldl (
      fun stats_to_statset/2,
      { Count, Sum, SamplesCount, ScaledCount, Sorted,
        mondemand_stats:new_statset ()},
      Stats),
  StatSet.

stats_to_statset (count,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (count, Count, StatSet)};
stats_to_statset (sum,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (sum, Sum, StatSet) };
stats_to_statset (min,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      min,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (1, Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (max,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      max,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (SamplesCount, Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (avg,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      avg,
      case ScaledCount > 0 of  % avoid divide by zero
        true -> trunc (Sum / Count);
        false -> 0
      end,
      StatSet) };
stats_to_statset (median,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      median,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.50), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_75,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      pctl_75,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.75), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_90,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      pctl_90,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.90), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_95,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      pctl_95,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.95), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_98,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      pctl_98,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.98), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_99,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_stats:set_statset (
      pctl_99,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.99), Sorted);
        false -> 0
      end,
      StatSet) }.

process_stats_sets (Table, _Function) ->
%  Host = net_adm:localhost (),
%  ets:foldl (fun (Data, Accum) ->
%               % invoke the function for the data
%               Function (ets_to_statset (Data, Host)),
%               Accum
%             end,
%             ok,
%             Table),
  ets:delete_all_objects (Table).

all () ->
  ets:tab2list (?STATS_TABLE).

reset_stats () ->
  ets:foldl (fun ({K, _}, Prev) ->
               ets:update_element (?STATS_TABLE, K, [{2,0}]) andalso Prev
             end,
             true,
             ?STATS_TABLE).

calculate_key (ProgId, Context, Type, Key) ->
  {ProgId, lists:keysort (1, Context), Type, Key}.


minute_tab (0)  -> md_min_00;
minute_tab (1)  -> md_min_01;
minute_tab (2)  -> md_min_02;
minute_tab (3)  -> md_min_03;
minute_tab (4)  -> md_min_04;
minute_tab (5)  -> md_min_05;
minute_tab (6)  -> md_min_06;
minute_tab (7)  -> md_min_07;
minute_tab (8)  -> md_min_08;
minute_tab (9)  -> md_min_09;
minute_tab (10) -> md_min_10;
minute_tab (11) -> md_min_11;
minute_tab (12) -> md_min_12;
minute_tab (13) -> md_min_13;
minute_tab (14) -> md_min_14;
minute_tab (15) -> md_min_15;
minute_tab (16) -> md_min_16;
minute_tab (17) -> md_min_17;
minute_tab (18) -> md_min_18;
minute_tab (19) -> md_min_19;
minute_tab (20) -> md_min_20;
minute_tab (21) -> md_min_21;
minute_tab (22) -> md_min_22;
minute_tab (23) -> md_min_23;
minute_tab (24) -> md_min_24;
minute_tab (25) -> md_min_25;
minute_tab (26) -> md_min_26;
minute_tab (27) -> md_min_27;
minute_tab (28) -> md_min_28;
minute_tab (29) -> md_min_29;
minute_tab (30) -> md_min_30;
minute_tab (31) -> md_min_31;
minute_tab (32) -> md_min_32;
minute_tab (33) -> md_min_33;
minute_tab (34) -> md_min_34;
minute_tab (35) -> md_min_35;
minute_tab (36) -> md_min_36;
minute_tab (37) -> md_min_37;
minute_tab (38) -> md_min_38;
minute_tab (39) -> md_min_39;
minute_tab (40) -> md_min_40;
minute_tab (41) -> md_min_41;
minute_tab (42) -> md_min_42;
minute_tab (43) -> md_min_43;
minute_tab (44) -> md_min_44;
minute_tab (45) -> md_min_45;
minute_tab (46) -> md_min_46;
minute_tab (47) -> md_min_47;
minute_tab (48) -> md_min_48;
minute_tab (49) -> md_min_49;
minute_tab (50) -> md_min_50;
minute_tab (51) -> md_min_51;
minute_tab (52) -> md_min_52;
minute_tab (53) -> md_min_53;
minute_tab (54) -> md_min_54;
minute_tab (55) -> md_min_55;
minute_tab (56) -> md_min_56;
minute_tab (57) -> md_min_57;
minute_tab (58) -> md_min_58;
minute_tab (59) -> md_min_59.

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

setup () ->
  case start_link() of
    {ok, Pid} -> Pid;
    {error, {already_started, _}} -> already_started
  end.

cleanup (already_started) -> ok;
cleanup (Pid) -> exit (Pid, normal).

% need to randomly create config keys and test looking them up in a sorted
% fashion  (basically test set versus ordered_set for a bunch of metrics)
%random_atom
 

config_perf_test_ () ->
  { setup,
    fun setup/0,
    fun cleanup/1,
    [
    ]
  }.


-endif.
