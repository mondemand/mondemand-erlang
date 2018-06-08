-module (mondemand_vmstats).

% This module is used to collect vmstats from the erlang vm and provide
% them in a form which mondemand can work with.  In addition, I'd like
% to eventually support looking at a finer granularity than mondemand.
% In order to support this, a gen_server keeps a queue of samples.  The
% samples are taken once a second, and a certain number of them are kept.
% About once a minute mondemand will come in and calculate gauges for all
% the sampled values (I use gauges so I don't overflow the counters in
% mondemand).
-include ("mondemand_internal.hrl").

-behaviour (gen_server).

%% API
-export ([start_link/0,
          to_mondemand/0,
          to_list/0,
          first/0,
          first/1,
          last/0,
          last/1,
          collect_sample/2,
          scheduler_wall_time_diff/2]).

%% gen_server callbacks
-export ( [ init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3
          ]).

-record (state, {samples = queue:new(),
                 max_samples = 300,  % 5 minutes of sampled data
                 legacy = false,     % old otp workarounds
                 previous_mondemand = undefined,
                 timer,
                 scheduler_former_flag,% keep track of previous scheduler
                                       % stats flag for shutdown
                 collect_scheduler_stats
                }).

-record (vm_sample, { timestamp,
                      context_switches,
                      gc_count,
                      gc_bytes_reclaimed,
                      io_bytes_in,
                      io_bytes_out,
                      reductions,
                      runtime,
                      wallclock,
                      run_queue,
                      queued_messages,
                      memory_total,
                      memory_process,
                      memory_system,
                      memory_atom,
                      memory_binary,
                      memory_ets,
                      memory_literal,
                      process_count,
                      process_limit,
                      port_count,
                      port_limit,
                      scheduler_wall_time
                    }).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

to_mondemand() ->
  gen_server:call (?MODULE, to_mondemand).

first () ->
  gen_server:call (?MODULE, first).

first (MetricOrMetrics) ->
  metrics_from_sample (MetricOrMetrics, first()).

last () ->
  gen_server:call (?MODULE, last).

last (MetricOrMetrics) ->
  metrics_from_sample (MetricOrMetrics, last()).

metrics_from_sample (A, Sample = #vm_sample {}) when is_atom (A) ->
  {_,Value} = metric_from_sample (A, Sample),
  Value;
metrics_from_sample (L, Sample = #vm_sample {}) when is_list (L) ->
  lists:map (fun (M) -> metric_from_sample (M, Sample) end, L).

metric_from_sample (Metric, Sample) ->
  case Metric of
    timestamp -> {Metric, Sample#vm_sample.timestamp};
    context_switches -> {Metric, Sample#vm_sample.context_switches};
    gc_count -> {Metric, Sample#vm_sample.gc_count};
    gc_bytes_reclaimed -> {Metric, Sample#vm_sample.gc_bytes_reclaimed};
    io_bytes_in -> {Metric, Sample#vm_sample.io_bytes_in};
    io_bytes_out -> {Metric, Sample#vm_sample.io_bytes_out};
    reductions -> {Metric, Sample#vm_sample.reductions};
    runtime -> {Metric, Sample#vm_sample.runtime};
    wallclock -> {Metric, Sample#vm_sample.wallclock};
    run_queue -> {Metric, Sample#vm_sample.run_queue};
    queued_messages -> {Metric, Sample#vm_sample.queued_messages};
    memory_total -> {Metric, Sample#vm_sample.memory_total};
    memory_process -> {Metric, Sample#vm_sample.memory_process};
    memory_system -> {Metric, Sample#vm_sample.memory_system};
    memory_atom -> {Metric, Sample#vm_sample.memory_atom};
    memory_binary -> {Metric, Sample#vm_sample.memory_binary};
    memory_ets -> {Metric, Sample#vm_sample.memory_ets};
    memory_literal -> {Metric, Sample#vm_sample.memory_literal};
    process_count -> {Metric, Sample#vm_sample.process_count};
    process_limit -> {Metric, Sample#vm_sample.process_limit};
    port_count -> {Metric, Sample#vm_sample.port_count};
    port_limit -> {Metric, Sample#vm_sample.port_limit};
    scheduler_wall_time -> {Metric, Sample#vm_sample.scheduler_wall_time }
  end.

to_list() ->
  gen_server:call (?MODULE, to_list).

%-=====================================================================-
%-                        gen_server callbacks                         -
%-=====================================================================-
init([]) ->
  % work around for the fact that R15B didn't have port_count
  Legacy = mondemand_config:vmstats_legacy_workaround(),

  % allow scheduler stats to be turned off (should default to true)
  {Former, CollectSchedulerStats} =
    case mondemand_config:vmstats_disable_scheduler_wall_time() of
      true -> {undefined, false};
      false -> {erlang:system_flag(scheduler_wall_time, true), true}
    end,

  InitialSample = collect_sample (Legacy, CollectSchedulerStats),
  InitialQueue = queue:in (InitialSample, queue:new ()),
  TRef = timer:send_interval (1000, collect), % collect samples every second
  % keep the initial sample as both the previous mondemand value and put
  % it into the queue
  { ok, #state { samples = InitialQueue,
                 timer = TRef,
                 legacy = Legacy,
                 collect_scheduler_stats = CollectSchedulerStats,
                 scheduler_former_flag = Former
                }
  }.

handle_call (first, _From, State = #state { samples = Queue }) ->
  {value, FirstSample} = queue:peek (Queue),
  {reply, FirstSample, State};
handle_call (last, _From, State = #state { samples = Queue }) ->
  {value, LastSample} = queue:peek_r (Queue),
  {reply, LastSample, State};
handle_call (to_list, _From, State = #state { samples = Queue }) ->
  {reply, queue:to_list (Queue), State};
handle_call (to_mondemand, _From,
             State = #state { samples = Queue,
                              previous_mondemand = Prev }) ->
  % queue should always have something in it
  {value, LastSample} = queue:peek_r (Queue),
  Stats =
    case Prev =:= undefined of
      true ->
        % we skip the first send of data to mondemand, as we have no way
        % to really ensure the normal duration between sends to mondemand
        % has elapsed, if it hasn't elapsed we might be emitting to mondemand
        % shortly after restart and would see some spikiness in any counters
        % (as they are turned into gauges with the assumption calls to 
        % to_mondemand/0 are happening on a regular interval).
        [];
      false ->
        to_mondemand (Prev, LastSample)
    end,
  {reply, Stats, State#state { previous_mondemand = LastSample } };
handle_call (_Request, _From, State = #state { }) ->
  {reply, ok, State }.

handle_cast (_Request, State = #state { }) ->
  {noreply, State}.

handle_info (collect,
             State = #state {samples = QueueIn,
                             max_samples = Max,
                             legacy = Legacy,
                             collect_scheduler_stats = CollectSchedulerStats
                            }) ->
  % collect a sample
  CurrentSample = collect_sample (Legacy, CollectSchedulerStats),

  % insert it into the queue
  QueueOut =
    case queue:len (QueueIn) =:= Max of
      true ->
        % when we are at the max entries, we drop one since we are adding one
        {{value, _},Q} = queue:out (QueueIn),
        queue:in (CurrentSample, Q);
      false ->
        % otherwise we aren't full yet, so just add one
        queue:in (CurrentSample, QueueIn)
    end,
  {noreply, State#state { samples = QueueOut }};
handle_info (_Info, State = #state {}) ->
  {noreply, State}.

terminate (_Reason, #state { scheduler_former_flag = Former }) ->
  % revert to the former wall time
  case Former =/= undefined of
    true -> erlang:system_flag(scheduler_wall_time, Former);
    false -> ok
  end,
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% private functions
%%====================================================================

collect_sample (Legacy, CollectSchedulerStats) ->

  Timestamp = mondemand_util:seconds_since_epoch (),

  % Most collected from http://www.erlang.org/doc/man/erlang.html#statistics-1
  %
  % ContextSwitches is the total number of context switches since
  % the system started.
  { ContextSwitches, _ } = erlang:statistics (context_switches),
  % GC info
  { NumberOfGCs, WordsReclaimed, _} = erlang:statistics (garbage_collection),
  % Input is the total number of bytes received through ports,
  % and Output is the total number of bytes output to ports.
  {{input,Input},{output,Output}} = erlang:statistics(io),
  % information about reductions
  { TotalReductions, _ReductionsSinceLast } = erlang:statistics (reductions),
  % Note that the run-time is the sum of the run-time for all threads in
  % the Erlang run-time system and may therefore be greater than
  % the wall-clock time. The time is returned in milliseconds.
  { TotalRuntime, _ } = erlang:statistics (runtime),
  % wall_clock can be used in the same manner as runtime, except that
  % real time is measured as opposed to runtime or CPU time.
  { TotalWallclock, _ } = erlang:statistics (wall_clock),

  % Returns the total length of the run queues, that is, the number of
  % processes that are ready to run on all available run queues.
  RunQueue = erlang:statistics (run_queue),
  % total length of all message queues
  TotalMessages =
    lists:foldl (
      fun (Pid, Acc) ->
        case process_info(Pid, message_queue_len) of
          undefined -> Acc;
          {message_queue_len, Count} -> Count+Acc
        end
      end,
      0,
      processes()
    ),

  Memory = erlang:memory(),
  TotalMemory = proplists:get_value(total, Memory),
  ProcessMemory = proplists:get_value(processes_used, Memory),
  SystemMemory = proplists:get_value(system, Memory),
  AtomUsed = proplists:get_value(atom_used, Memory),
  BinaryMemory = proplists:get_value(binary, Memory),
  EtsMemory = proplists:get_value(ets, Memory),
  LiteralMemory = get_literal_memory_size(),
  ProcessCount = erlang:system_info(process_count),
  ProcessLimit = erlang:system_info(process_limit),

  % R15B didn't have a good way to get these so working around this fact
  {PortCount, PortLimit} =
    case Legacy of
      true -> {length(erlang:ports()), 0};
      false -> {erlang:system_info(port_count), erlang:system_info(port_limit)}
    end,

  SchedWallTime =
    case CollectSchedulerStats of
      true -> erlang:statistics(scheduler_wall_time);
      false -> undefined
    end,

  #vm_sample {
    timestamp = Timestamp,
    context_switches = ContextSwitches,
    gc_count = NumberOfGCs,
    gc_bytes_reclaimed = WordsReclaimed * erlang:system_info(wordsize),
    io_bytes_in = Input,
    io_bytes_out = Output,
    reductions = TotalReductions,
    runtime = TotalRuntime,
    wallclock = TotalWallclock,
    run_queue = RunQueue,
    queued_messages = TotalMessages,
    memory_total = TotalMemory,
    memory_process = ProcessMemory,
    memory_system = SystemMemory,
    memory_atom = AtomUsed,
    memory_binary = BinaryMemory,
    memory_ets = EtsMemory,
    memory_literal = LiteralMemory,
    process_count = ProcessCount,
    process_limit = ProcessLimit,
    port_count = PortCount,
    port_limit = PortLimit,
    scheduler_wall_time = SchedWallTime
  }.

to_mondemand (#vm_sample {
               context_switches = PrevContextSwitches,
               gc_count = PrevNumberOfGCs,
               gc_bytes_reclaimed = PrevWordsReclaimed,
               io_bytes_in = PrevInput,
               io_bytes_out = PrevOutput,
               reductions = PrevReductions,
               runtime = PrevRuntime,
               wallclock = PrevWallclock,
               scheduler_wall_time = PrevSchedWallTime
             },
             #vm_sample {
               timestamp = _Timestamp,
               context_switches = ContextSwitches,
               gc_count = NumberOfGCs,
               gc_bytes_reclaimed = WordsReclaimed,
               io_bytes_in = Input,
               io_bytes_out = Output,
               reductions = Reductions,
               runtime = Runtime,
               wallclock = Wallclock,
               run_queue = RunQueue,
               queued_messages = TotalMessages,
               memory_total = TotalMemory,
               memory_process = ProcessMemory,
               memory_system = SystemMemory,
               memory_atom = AtomUsed,
               memory_binary = BinaryMemory,
               memory_ets = EtsMemory,
               memory_literal = LiteralMemory,
               process_count = ProcessCount,
               process_limit = ProcessLimit,
               port_count = PortCount,
               port_limit = PortLimit,
               scheduler_wall_time = SchedWallTime
             }) ->
    [
      { gauge, context_switches, ContextSwitches - PrevContextSwitches },
      { gauge, gc_count, NumberOfGCs - PrevNumberOfGCs },
      { gauge, gc_bytes_reclaimed, WordsReclaimed - PrevWordsReclaimed },
      { gauge, io_bytes_in, Input - PrevInput },
      { gauge, io_bytes_out, Output - PrevOutput },
      { gauge, reductions, Reductions - PrevReductions },
      { gauge, runtime, Runtime - PrevRuntime },
      { gauge, wallclock, Wallclock - PrevWallclock },
      { gauge, run_queue, RunQueue },
      { gauge, queued_messages, TotalMessages },
      { gauge, memory_total, TotalMemory },
      { gauge, memory_process, ProcessMemory },
      { gauge, memory_system, SystemMemory },
      { gauge, memory_atom, AtomUsed },
      { gauge, memory_binary, BinaryMemory },
      { gauge, memory_ets, EtsMemory },
      { gauge, memory_literal, LiteralMemory },
      { gauge, process_count, ProcessCount },
      { gauge, process_limit, ProcessLimit },
      { gauge, port_count, PortCount },
      { gauge, port_limit, PortLimit }
      | scheduler_wall_time_diff (PrevSchedWallTime, SchedWallTime)
    ].

scheduler_wall_time_diff (undefined,_) -> [];
scheduler_wall_time_diff (_,undefined) -> [];
scheduler_wall_time_diff (PrevSchedWallTime, SchedWallTime) ->
  [ { gauge,
      ["scheduler_",integer_to_list(I),"_utilization"],
      case TotalTime - PrevTotalTime of
        0 -> 0;
        TotalDiff -> trunc (((ActiveTime - PrevActiveTime)/TotalDiff) * 100.0)
      end
    }
    || {{I, PrevActiveTime, PrevTotalTime}, {I, ActiveTime, TotalTime}}
    <- lists:zip(lists:sort(PrevSchedWallTime),lists:sort(SchedWallTime))
  ].

% Sum of current carriersizes(mcbs+scbs) for all instances although
% currently there is only one global instance for literal_alloc.
get_literal_memory_size () ->
  lists:foldl(
    fun
      ({instance, _N, Instance}, AccLiteralSize) when is_list(Instance) ->
        CarrierSizeForInstance =
          lists:map(
            fun(CarrierSizeType) ->
              case lists:keyfind(CarrierSizeType, 1, Instance) of
                {CarrierSizeType, MemDataList} when is_list(MemDataList) ->
                  case lists:keyfind(carriers_size, 1, MemDataList) of
                    {
                     carriers_size,
                     CurrentSize, _LastMaxSize, _MaxSize
                    } -> CurrentSize;
                    _Else -> 0 % Ignore unknown format
                  end;
                _Else -> 0 % Ignore unknown format
              end
            end, [mbcs, sbcs]),
        lists:sum(CarrierSizeForInstance) + AccLiteralSize
     ;(_UnknownFormat, AccLiteralSize) -> AccLiteralSize
    end, 0, erlang:system_info({allocator, literal_alloc})).

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

literal_memory_test () ->
  Size = get_literal_memory_size(),
  ?assertEqual(true, is_integer(Size) andalso Size =/= 0).

-endif.
