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
          collect_sample/0]).

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
                 previous_mondemand,
                 timer
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
                      process_count,
                      process_limit
                    }).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

to_mondemand() ->
  gen_server:call (?MODULE, to_mondemand).

to_list() ->
  gen_server:call (?MODULE, to_list).

%-=====================================================================-
%-                        gen_server callbacks                         -
%-=====================================================================-
init([]) ->
  InitialSample = collect_sample (),
  InitialQueue = queue:in (InitialSample, queue:new ()),
  TRef = timer:send_interval (1000, collect), % collect samples every second
  % keep the initial sample as both the previous mondemand value and put
  % it into the queue
  { ok, #state{ samples = InitialQueue,
                previous_mondemand = InitialSample,
                timer = TRef } }.

handle_call (to_list, _From, State = #state { samples = Queue }) ->
  {reply, queue:to_list (Queue), State};
handle_call (to_mondemand, _From,
             State = #state { samples = Queue,
                              previous_mondemand = Prev }) ->
  % queue should always have something in it
  {value, LastSample} = queue:peek_r (Queue),
  Stats = to_mondemand (Prev, LastSample),
  {reply, Stats, State#state { previous_mondemand = LastSample } };
handle_call (_Request, _From, State = #state { }) ->
  {reply, ok, State }.

handle_cast (_Request, State = #state { }) ->
  {noreply, State}.

handle_info (collect,
             State = #state {samples = QueueIn,
                             max_samples = Max
                            }) ->
  % collect a sample
  CurrentSample = collect_sample (),

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

terminate (_Reason, _State) ->
  ok.
code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% private functions
%%====================================================================

collect_sample () ->

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
  TotalMemory = proplists:get_value (total, Memory),
  ProcessMemory = proplists:get_value (processes_used, Memory),
  SystemMemory = proplists:get_value (system, Memory),
  AtomUsed = proplists:get_value (atom_used, Memory),
  BinaryMemory = proplists:get_value (binary, Memory),
  EtsMemory = proplists:get_value (ets, Memory),
  ProcessCount = erlang:system_info (process_count),
  ProcessLimit = erlang:system_info(process_limit),

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
    process_count = ProcessCount,
    process_limit = ProcessLimit
  }.

to_mondemand (#vm_sample {
               context_switches = PrevContextSwitches,
               gc_count = PrevNumberOfGCs,
               gc_bytes_reclaimed = PrevWordsReclaimed,
               io_bytes_in = PrevInput,
               io_bytes_out = PrevOutput,
               reductions = PrevReductions,
               runtime = PrevRuntime,
               wallclock = PrevWallclock
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
               process_count = ProcessCount,
               process_limit = ProcessLimit
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
      { gauge, process_count, ProcessCount },
      { gauge, process_limit, ProcessLimit }
    ].

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
