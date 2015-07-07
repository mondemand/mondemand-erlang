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
          get_state/0,

          % counter functions
          create_counter/2,
          create_counter/3,
          create_counter/4,
          create_counter/5,
          increment/2,
          increment/3,
          increment/4,
          fetch_counter/2,
          fetch_counter/3,
          remove_counter/2,
          remove_counter/3,

          % gauge functions
          create_gauge/2,
          create_gauge/3,
          create_gauge/4,
          create_gauge/5,
          set/3,
          set/4,
          fetch_gauge/2,
          fetch_gauge/3,
          remove_gauge/2,
          remove_gauge/3,

          % sample set functions
          create_sample_set/2,
          create_sample_set/3,
          create_sample_set/4,
          create_sample_set/5,
          create_sample_set/6,
          add_sample/3,
          add_sample/4,
          fetch_sample_set/2,
          fetch_sample_set/3,
          remove_sample_set/2,
          remove_sample_set/3,

          all_sample_set_stats/0,

          map_now/1,
          map_then/2,
          map/2,

          flush/2,
          config/0,
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

-record (state,  {}).
-record (mdkey,  {type, prog_id, context, key}).
-record (config, {key,
                  description,
                  max_sample_size,
                  statistics
                 }).

-define (STATS_TABLE,  md_stats).
-define (CONFIG_TABLE, md_config).

-define (ALL_STATS, [min, max, sum, count, avg, median,
                     pctl_75, pctl_90, pctl_95, pctl_98, pctl_99]).

-define (CONFIG_KEY_INDEX, #config.key).

-define (METRIC_KEY_INDEX, #md_metric.key).
-define (METRIC_TYPE_INDEX, #md_metric.type).
-define (METRIC_VALUE_INDEX, #md_metric.value).

-define (STATSET_KEY_INDEX,   1).
-define (STATSET_MAX_INDEX,   2).
-define (STATSET_COUNT_INDEX, 3).
-define (STATSET_SUM_INDEX,   4).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

get_state() ->
  gen_server:call (?MODULE, get_state).

create_counter (ProgId, Key) ->
  create_counter (ProgId, Key, [], "", 0).
create_counter (ProgId, Key, Description) ->
  create_counter (ProgId, Key, [], Description, 0).
create_counter (ProgId, Key, Context, Description) ->
  create_counter (ProgId, Key, Context, Description, 0).
create_counter (ProgId, Key, Context, Description, Amount)
  when is_integer (Amount), is_list (Context) ->
  InternalKey = calculate_key (ProgId, Context, counter, Key),
  add_new_config (InternalKey, Description),
  case ets:insert_new (?STATS_TABLE,
                       #md_metric {key = InternalKey, value = Amount}) of
    true -> ok;
    false -> {error, already_created}
  end.

increment (ProgId, Key) ->
  increment (ProgId, Key, [], 1).
increment (ProgId, Key, Amount)
  when is_integer (Amount) ->
  increment (ProgId, Key, [], Amount);
increment (ProgId, Key, Context)
  when is_list (Context) ->
  increment (ProgId, Key, Context, 1).
increment (ProgId, Key, Context, Amount)
  when is_integer (Amount), is_list (Context) ->
  InternalKey = calculate_key (ProgId, Context, counter, Key),
  try_update_counter (InternalKey, Amount).

fetch_counter (ProgId, Key) ->
  fetch_counter (ProgId, Key, []).
fetch_counter (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, counter, Key),
  return_if_exists (InternalKey, ?STATS_TABLE).

remove_counter (ProgId, Key) ->
  remove_counter (ProgId, Key, []).
remove_counter (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, counter, Key),
  remove_metric (InternalKey, ?STATS_TABLE).

update_counter (InternalKey, Amount) when Amount >= 0 ->
  ets:update_counter (?STATS_TABLE, InternalKey,
                      {?METRIC_VALUE_INDEX, Amount,
                       ?MD_STATS_MAX_METRIC_VALUE, 0});
update_counter (InternalKey, Amount) when Amount < 0 ->
  ets:update_counter (?STATS_TABLE, InternalKey,
                      {?METRIC_VALUE_INDEX, Amount,
                       ?MD_STATS_MIN_METRIC_VALUE, 0}).

try_update_counter (InternalKey =
                      #mdkey { prog_id = ProgId,
                               context = Context,
                               key = Key
                             },
                    Amount) ->
  % LWES is sending int64 values, so wrap at the max int64 integer back to
  % zero
  try update_counter (InternalKey, Amount) of
    V -> {ok,V}
  catch
    error:badarg ->
      % the key probably doesn't exist, so create with an empty description
      case create_counter (ProgId, Key, Context, "", Amount) of
        ok -> {ok, Amount}; % may not always be true if simultaneous updates
                            % are happening, but probably mostly true
        { error, already_created }->
          % create failed, so someone else probably created it, so just
          % try again this time without the catch
          V = update_counter (InternalKey, Amount),
          {ok, V}
      end
  end.

create_gauge (ProgId, Key) ->
  create_gauge (ProgId, Key, [], "", 0).
create_gauge (ProgId, Key, Description) ->
  create_gauge (ProgId, Key, [], Description, 0).
create_gauge (ProgId, Key, Context, Description) ->
  create_gauge (ProgId, Key, Context, Description, 0).
create_gauge (ProgId, Key, Context, Description, Amount) ->
  InternalKey = calculate_key (ProgId, Context, gauge, Key),
  add_new_config (InternalKey, Description),
  case ets:insert_new (?STATS_TABLE,
                       #md_metric {key = InternalKey, value = Amount}) of
    true -> ok;
    false -> {error, already_created}
  end.

set (ProgId, Key, Amount) ->
  set (ProgId, Key, [], Amount).
set (ProgId, Key, Context, Amount) ->
  InternalKey = calculate_key (ProgId, Context, gauge, Key),

  % if we would overflow a gauge, instead of going negative just leave it
  % at the max value and return false
  {Overflowed, RealAmount} =
    case Amount >= 0 of
      true ->
        case Amount =< ?MD_STATS_MAX_METRIC_VALUE of
          true -> {false, Amount};
          false -> {true, ?MD_STATS_MAX_METRIC_VALUE}
        end;
      false ->
        case Amount >= ?MD_STATS_MIN_METRIC_VALUE of
          true -> {false, Amount};
          false -> {true, ?MD_STATS_MIN_METRIC_VALUE}
        end
    end,

  case try_update_gauge (InternalKey, RealAmount) of
    false -> {error, internal_db};
    true ->
      case Overflowed of
        true -> {error, overflow};
        false -> ok
      end
  end.

fetch_gauge (ProgId, Key) ->
  fetch_gauge (ProgId, Key, []).
fetch_gauge (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, gauge, Key),
  return_if_exists (InternalKey, ?STATS_TABLE).

remove_gauge (ProgId, Key) ->
  remove_gauge (ProgId, Key, []).
remove_gauge (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, gauge, Key),
  remove_metric (InternalKey, ?STATS_TABLE).

try_update_gauge (InternalKey =
                      #mdkey { prog_id = ProgId,
                               context = Context,
                               key = Key
                             },
                  Amount) ->
  % use update_element for gauges as we only want the last value
  case
    ets:update_element (?STATS_TABLE, InternalKey,
                        [{?METRIC_VALUE_INDEX, Amount}])
  of
    true -> true;
    false ->
      % the key probably doesn't exist, so create with an empty description
      case create_gauge (ProgId, Key, Context, "", Amount) of
        ok -> true;
        {error, already_created} ->
          % create failed, so someone else probably created it, so just
          % try again this time without the case
          ets:update_element (?STATS_TABLE, InternalKey,
                              [{?METRIC_VALUE_INDEX, Amount}])
      end
  end.

create_sample_set (ProgId, Key) ->
  create_sample_set (ProgId, Key, [], "",
                     mondemand_config:default_max_sample_size(),
                     mondemand_config:default_stats()).
create_sample_set (ProgId, Key, Description) ->
  create_sample_set (ProgId, Key, [], Description,
                     mondemand_config:default_max_sample_size(),
                     mondemand_config:default_stats()).
create_sample_set (ProgId, Key, Context, Description) ->
  create_sample_set (ProgId, Key, Context, Description,
                     mondemand_config:default_max_sample_size(),
                     mondemand_config:default_stats()).
create_sample_set (ProgId, Key, Context, Description, Max) ->
  create_sample_set (ProgId, Key, Context, Description,
                     Max,
                     mondemand_config:default_stats()).
create_sample_set (ProgId, Key, Context, Description, Max, Stats) ->
  InternalKey = calculate_key (ProgId, Context, statset, Key),
  create_sample_set_internal (InternalKey, Description, Max, Stats).

create_sample_set_internal (InternalKey = #mdkey{}) ->
  #config { max_sample_size = Max, statistics = Stats } =
    lookup_config (InternalKey),
  create_sample_set_internal (InternalKey, "", Max, Stats).

create_sample_set_internal (InternalKey, Description, Max, Stats) ->
  add_new_config (InternalKey, Description, Max, Stats),
  % Creates a new entry of the form
  % { Key, Count, Sum, Sample1 ... SampleMax }
  case ets:insert_new (minute_tab (mondemand_util:current_minute()),
                        list_to_tuple (
                          [ InternalKey, Max, 0, 0
                            | [ 0 || _ <- lists:seq (1, Max) ]
                          ])
                      ) of
    true -> ok;
    false -> {error, already_created}
  end.

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
  add_sample (ProgId, Key, [], Value).

% this implements reservoir sampling of values
%   http://en.wikipedia.org/wiki/Reservoir_sampling
% in an ets table
add_sample (ProgId, Key, Context, Value) ->
  InternalKey = calculate_key (ProgId, Context, statset, Key),

  Tid = minute_tab (mondemand_util:current_minute()),

  % First we'll update the count and sum, and we care about the count
  % as it gives us an index into the list of samples, also we'll get
  % back the max size, saving us a lookup in the config table
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
    I -> ets:update_element (Tid, InternalKey, {?STATSET_SUM_INDEX+I,Value})
  end.

fetch_sample_set (ProgId, Key) ->
  fetch_sample_set (ProgId, Key, []).

fetch_sample_set (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, statset, Key),
  Table = minute_tab (mondemand_util:current_minute()),
  return_if_exists (InternalKey, Table).

remove_sample_set (ProgId, Key) ->
  remove_sample_set (ProgId, Key, []).
remove_sample_set (ProgId, Key, Context) ->
  InternalKey = calculate_key (ProgId, Context, statset, Key),
  Table = minute_tab (mondemand_util:current_minute()),
  remove_metric (InternalKey, Table).

config_exists (Key) ->
  case ets:lookup (?CONFIG_TABLE, Key) of
    [] -> false;
    [#config{}] -> true
  end.

return_if_exists (Key, Table) ->
  case config_exists (Key) of
    true ->
      #md_metric {value = V} = lookup_metric (Key, Table),
      V;
    false ->
      undefined
  end.

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

add_new_config (Key, Description) ->
  C = lookup_default_config (),
  NewConfig = C#config { key = Key,
                         description = Description },
  ets:insert_new (?CONFIG_TABLE, NewConfig).

add_new_config (Key, Description, Max, Stats) ->
  C = lookup_default_config (),
  NewConfig = C#config { key = Key,
                         description = Description,
                         max_sample_size = Max,
                         statistics = normalize_stats (Stats) },
  ets:insert_new (?CONFIG_TABLE, NewConfig).

all_sample_set_stats () ->
  ?ALL_STATS.

config () ->
  io:format ("~1s ~-21s ~-35s ~-20s~n",["t", "prog_id", "key", "value"]),
  ets:foldl (fun
               (#config {key = '$default_config'}, A) ->
                 A;
               (#config {
                  key = #mdkey { type = _Type, prog_id = _ProgId,
                                 context = _Context, key = _Key }
                }, A) ->
                 A
             end,
             ok,
             ?CONFIG_TABLE).

map_now (Function) ->
  StatsSetTable = minute_tab (mondemand_util:current_minute()),
  map (Function, StatsSetTable).

map_then (Function, Ago) ->
  PreviousMinute = minutes_ago (mondemand_util:current_minute(), Ago),
  StatsSetTable = minute_tab (PreviousMinute),
  map (Function, StatsSetTable).

% I want to iterate over the config table, collapsing all metrics for a
% particular program id and context into a group so they can all be processed
% together.
%
% I want to use ets:first/1 and ets:next/2 so I can eventually set some rules
% about how they are processed in terms of time spent overall
%
map (Function, StatsSetTable) ->
  % there a couple of things we'd like to not recalculate but are probably
  % used over and over, so get them here and pass them through
  Host = mondemand_util:host (),

  case ets:first (?CONFIG_TABLE) of
    '$end_of_table' -> [];
    FirstKey ->
      % put the first into the current list to collapse
      map (Function, {Host, StatsSetTable}, [FirstKey])
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
     AllKeys = [LastKey = #mdkey {prog_id = ProgId, context = Context}|_]) ->

  case ets:next (?CONFIG_TABLE,LastKey) of
    '$end_of_table' ->
      % we hit the end of the table, so just call the function with the
      % current set of matched keys
      Function (construct_stats_msg (AllKeys, State));
    Key = #mdkey {prog_id = ProgId, context = Context} ->
      % this particular entry has the same ProgId and Context, so add it
      % to the list of keys which are grouped together
      map (Function, State, [Key|AllKeys]);
    NonMatchingKey ->
      % the key didn't match, so call the function with the current set
      Function (construct_stats_msg (AllKeys, State)),
      % then use this key for the next iteration
      map (Function, State, [NonMatchingKey])
  end.

construct_stats_msg (AllKeys = [#mdkey {prog_id = ProgId, context = Context}|_],
                     {Host, Table}) ->
  Metrics = [ lookup_metric (I, Table) || I <- AllKeys ],
  {FinalHost, FinalContext} =
    mondemand_util:context_from_context (Host, Context),
  mondemand_statsmsg:new (mondemand_util:binaryify (ProgId),
                          mondemand_util:binaryify_context (FinalContext),
                          Metrics,
                          case FinalHost =/= undefined of
                            true -> mondemand_util:binaryify (FinalHost);
                            false -> FinalHost
                          end,
                          mondemand_util:millis_since_epoch()).

% this function looks up metrics from the different internal DB's and
% unboxes them
lookup_metric (InternalKey = #mdkey {type = Type, key = Key}, Table) ->
  case Type of
    I when I =:= counter; I =:= gauge ->
      case ets:lookup (?STATS_TABLE, InternalKey) of
        [] ->
          #md_metric { key = mondemand_util:binaryify (Key),
                       type = I,
                       value = 0 };
        [#md_metric {value = V}] ->
          #md_metric { key = mondemand_util:binaryify (Key),
                       type = I,
                       value = V }
      end;
    I when I =:= statset ->
      #config { statistics = Stats } = lookup_config (InternalKey),
      case ets:lookup (Table, InternalKey) of
        [] ->
          % special case, for filling out an empty statset
          #md_metric { key = mondemand_util:binaryify (Key),
                       type = I,
                       value = statset (0, 0, 0, 0, [], Stats)
                     };
        [Entry] ->
          #md_metric { key = mondemand_util:binaryify (Key),
                       type = I,
                       value = ets_to_statset (Entry, Stats)
                     }
      end
  end.

remove_metric (InternalKey = #mdkey {type = Type}, Table) ->
  ets:delete (?CONFIG_TABLE, InternalKey),
  case Type of
    I when I =:= counter; I =:= gauge ->
      ets:delete (?STATS_TABLE, InternalKey);
    I when I =:= statset ->
      ets:delete (Table, InternalKey)
  end.

graphite_type_string (ProgId, Key, Context, Type) ->
  Context_String =
    mondemand_util:join ( [ io_lib:format ("~s_~s",
                                       [mondemand_util:stringify (K),
                                        mondemand_util:stringify (V)
                                       ])
                            || {K, V} <- Context
                          ], "."
                        ),
  io_lib:format ("~s.~s~s.*.~s",
                 [ProgId, Key,
                  case Context_String of
                    [] -> "";
                    C -> [".",C]
                  end,
                  Type]).

all () ->
  io:format ("~-58s ~-20s~n",["key", "value"]),
  io:format ("~-58s ~-20s~n",[
             "----------------------------------------------------------",
             "--------------------"]),
  map_now (fun (#md_stats_msg {prog_id = ProgId,
                               context = Context,
                               metrics = Metrics}) ->
             [
               case T of
                 IT when IT =:= gauge; IT =:= counter ->
                   io:format ("~-58s ~-20b~n",
                              [graphite_type_string (ProgId, K, Context, T),
                               V]);
                 statset ->
                   [
                     case mondemand_statsmsg:get_statset (S, V) of
                       undefined -> ok;
                       SV ->
                         io:format ("~-58s ~-20b~n",
                              [graphite_type_string (ProgId, K, Context, S),
                               SV])
                     end
                     || S <- all_sample_set_stats ()
                   ]
               end
               || #md_metric { type = T, key = K, value = V } <- Metrics
             ]
           end),
  ok.

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
                            max_sample_size =
                               mondemand_config:default_max_sample_size(),
                            statistics =
                               mondemand_config:default_stats() }),

  % this table is for counters and gauges
  ets:new (?STATS_TABLE, [ set,
                           public,
                           named_table,
                           {write_concurrency, true},
                           {read_concurrency, false},
                           {keypos, ?METRIC_KEY_INDEX}
                         ]),
  {ok, #state {}}.

handle_call (get_state, _From, State) ->
  {reply, State, State};
handle_call (_Request, _From, State) ->
  {reply, ok, State}.

handle_cast (_Request, State) ->
  {noreply, State}.

handle_info (_Info, State) ->
  {noreply, State}.

terminate (_Reason, _State) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.


%-=====================================================================-
%-                        Internal Functions                           -
%-=====================================================================-

normalize_stats (undefined) -> mondemand_config:default_stats();
normalize_stats ("") -> mondemand_config:default_stats();
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

minutes_ago (MinuteNow, Ago) ->
  case MinuteNow - Ago of
    N when N < 0 -> 60 + N;
    N -> N
  end.

flush (MinutesAgo, Function) ->
  PreviousMinute = minutes_ago (mondemand_util:current_minute(), MinutesAgo),
  StatsSetTable = minute_tab (PreviousMinute),
  map (Function, StatsSetTable),
  ets:delete_all_objects (StatsSetTable).

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

  statset (Count, Sum, SamplesCount, ScaledCount, Sorted, Stats).

statset (Count, Sum, SamplesCount, ScaledCount, Sorted, Stats) ->
  % we fold over the list of stats we are calculating and
  % construct a #statset{} record
  {_,_,_,_,_,StatSet} =
    lists:foldl (
      fun stats_to_statset/2,
      { Count, Sum, SamplesCount, ScaledCount, Sorted,
        mondemand_statsmsg:new_statset ()},
      Stats),
  StatSet.

stats_to_statset (count,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (count, Count, StatSet)};
stats_to_statset (sum,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (sum, Sum, StatSet) };
stats_to_statset (min,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      min,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (1, Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (max,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      max,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (SamplesCount, Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (avg,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      avg,
      case ScaledCount > 0 of  % avoid divide by zero
        true -> trunc (Sum / Count);
        false -> 0
      end,
      StatSet) };
stats_to_statset (median,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      median,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.50), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_75,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      pctl_75,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.75), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_90,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      pctl_90,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.90), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_95,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      pctl_95,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.95), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_98,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      pctl_98,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.98), Sorted);
        false -> 0
      end,
      StatSet) };
stats_to_statset (pctl_99,
                  {Count, Sum, SamplesCount, ScaledCount, Sorted, StatSet}) ->
  { Count, Sum, SamplesCount, ScaledCount, Sorted,
    mondemand_statsmsg:set_statset (
      pctl_99,
      case ScaledCount > 0 of  % avoid badarg
        true -> element (trunc (ScaledCount*0.99), Sorted);
        false -> 0
      end,
      StatSet) }.


reset_stats () ->
  ets:foldl (fun ({K, _}, Prev) ->
               ets:update_element (?STATS_TABLE, K, [{2,0}]) andalso Prev
             end,
             true,
             ?STATS_TABLE).

calculate_key (ProgId, Context, Type, Key) ->
  #mdkey {type = Type,
          prog_id = ProgId,
          context = lists:keysort (1, Context),
          key =Key
         }.

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
%-ifdef (TEST).
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
      % tests using create_counter first
      ?_assertEqual (undefined, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual (ok, create_counter (my_prog1, my_metric1)),
      ?_assertEqual ({error, already_created}, create_counter (my_prog1, my_metric1)),
      ?_assertEqual (0, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual ({ok,1}, increment (my_prog1, my_metric1)),
      ?_assertEqual (1, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual ({ok,2}, increment (my_prog1, my_metric1)),
      ?_assertEqual ({ok,3}, increment (my_prog1, my_metric1)),
      ?_assertEqual ({ok,4}, increment (my_prog1, my_metric1)),
      ?_assertEqual (4, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual (true, remove_counter (my_prog1, my_metric1)),
      ?_assertEqual (undefined, fetch_counter (my_prog1, my_metric1)),

      % test using automatic creation of counters
      ?_assertEqual (undefined, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual ({ok,1}, increment (my_prog1, my_metric1)),
      ?_assertEqual (1, fetch_counter (my_prog1, my_metric1)),
      ?_assertEqual (true, remove_counter (my_prog1, my_metric1)),
      ?_assertEqual (undefined, fetch_counter (my_prog1, my_metric1)),

      % tests using create_gauge first
      ?_assertEqual (undefined, fetch_gauge (my_prog1, my_metric1)),
      ?_assertEqual (ok, create_gauge (my_prog1, my_metric1)),
      ?_assertEqual ({error, already_created}, create_gauge (my_prog1, my_metric1)),
      ?_assertEqual (0, fetch_gauge (my_prog1, my_metric1)),
      ?_assertEqual (ok, set (my_prog1, my_metric1, 5)),
      ?_assertEqual (5, fetch_gauge (my_prog1, my_metric1)),
      ?_assertEqual (ok, set (my_prog1, my_metric1, 6)),
      ?_assertEqual (ok, set (my_prog1, my_metric1, 4)),
      ?_assertEqual (4, fetch_gauge (my_prog1, my_metric1)),
      ?_assertEqual (true, remove_gauge (my_prog1, my_metric1)),
      ?_assertEqual (undefined, fetch_gauge (my_prog1, my_metric1)),

      % tests using sample sets
      ?_assertEqual (undefined, fetch_sample_set (my_prog1, my_metric1)),
      % default size is 10
      ?_assertEqual (ok, create_sample_set (my_prog1, my_metric1)),
      % add some
      fun () ->
        [
          ?assertEqual (true, add_sample (my_prog1, my_metric1, N))
          || N <- lists:seq (1, 5)
        ]
      end,
      % check their values
      fun () ->
        SS = fetch_sample_set (my_prog1, my_metric1),
        ?assertEqual (5, mondemand_statsmsg:get_statset (count, SS)),
        ?assertEqual (15, mondemand_statsmsg:get_statset (sum, SS)),
        ?assertEqual (1, mondemand_statsmsg:get_statset (min, SS)),
        ?assertEqual (5, mondemand_statsmsg:get_statset (max, SS))
      end,
      % add a few more
      fun () ->
        [
          ?assertEqual (true, add_sample (my_prog1, my_metric1, N))
          || N <- lists:seq (6, 20)
        ]
      end,
      fun () ->
        SS = fetch_sample_set (my_prog1, my_metric1),
        ?assertEqual (20, mondemand_statsmsg:get_statset (count, SS)),
        ?assertEqual (lists:sum(lists:seq(1,20)),
                      mondemand_statsmsg:get_statset (sum, SS)),
        % for min and max since we've been replacing the samples in the
        % reservoir we can't really assert much other than min will probably
        % not be 20 and max will probably not be 1
        Min = mondemand_statsmsg:get_statset (min, SS),
        ?assertEqual (true, Min < 20),
        Max = mondemand_statsmsg:get_statset (max, SS),
        ?assertEqual (true, Max > 1)
      end,
      ?_assertEqual (true, remove_sample_set (my_prog1, my_metric1))
    ]
  }.

%-endif.
