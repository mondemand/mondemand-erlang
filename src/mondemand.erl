%% @author Anthony Molinaro <anthonym@alumni.caltech.edu>
%%
%% @doc Mondemand Public Interface
%%
%% This module is the public interface for the mondemand client application
%% for erlang.  In addition it manages the ets table which backs the mondemand
%% stats collection system.
%%
%% Mondemand has the concept of two types of metrics
%%
%% * Counters - values which typically increase until rolling back to 0
%% * Gauges - instantaneous values such as temperature (or more regularly
%% a reseting counter or non-mondemand held counter).
%%
%% Clients will typically add calls like
%%
%% mondemand:increment (ProgramName, CounterName, AmountToIncrementBy).
%%
%% for manipulating counters, and
%%
%% mondemand:set (ProgramName, GaugeName, AmountToSetGaugeTo).
%%
%% In addition both calls above accept an optional 4th parameter of a list
%% of Key/Value pairs representing the context of the call.
%%
%% Mondemand will add the hostname to all metrics sent to the central
%% mondemand-server.

-module (mondemand).

-include_lib ("lwes/include/lwes.hrl").
-include ("mondemand_internal.hrl").

-behaviour (gen_server).

%% API
-export ([
  start_link/0,

  % ------------ stats functions -----------
  %
  % ============ counters =================
  create_counter/2, % (ProgId, Key)
  create_counter/3, % (ProgId, Key, [{CtxtKey,CtxtVal}] | Desc)
  create_counter/4, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc)
  create_counter/5, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc, InitialAmount)
  increment/2,      % (ProgId, Key)
  increment/3,      % (ProgId, Key, Amount | [{CtxtKey,CtxtValue}])
  increment/4,      % (ProgId, Key, Amount | [{CtxtKey,CtxtValue}], [{CtxtKey,CtxtValue}]| Amount )
  fetch_counter/2,  % (ProgId, Key)
  fetch_counter/3,  % (ProgId, Key, [{CtxtKey,CtxtValue}])
  remove_counter/2, % (ProgId, Key)
  remove_counter/3, % (ProgId, Key, [{CtxtKey,CtxtValue}])

  % ============ gauges =================
  create_gauge/2, % (ProgId, Key)
  create_gauge/3, % (ProgId, Key, [{CtxtKey,CtxtVal}] | Desc)
  create_gauge/4, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc)
  create_gauge/5, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc, InitialAmount)
  set/3,          % (ProgId, Key, Value)
  set/4,          % (ProgId, Key, [{ContextKey,ContextValue}], Value)
  fetch_gauge/2,  % (ProgId, Key)
  fetch_gauge/3,  % (ProgId, Key, [{CtxtKey,CtxtValue}])
  remove_gauge/2, % (ProgId, Key)
  remove_gauge/3, % (ProgId, Key, [{CtxtKey,CtxtValue}])

  % ============ statsets =================
  create_sample_set/2, % (ProgId, Key)
  create_sample_set/3, % (ProgId, Key, [{CtxtKey,CtxtVal}] | Desc)
  create_sample_set/4, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc)
  create_sample_set/5, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc, MaxSamples)
  create_sample_set/6, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc, MaxSamples, StatTypeToSend)

  add_sample/3,        % (ProgId, Key, Value)
  add_sample/4,        % (ProgId, Key, [{CtxtKey,CtxtValue}], Value)
  fetch_sample_set/2,  % (ProgId, Key)
  fetch_sample_set/3,  % (ProgId, Key, [{CtxtKey,CtxtValue}])
  remove_sample_set/2,  % (ProgId, Key)
  remove_sample_set/3,  % (ProgId, Key, [{CtxtKey,CtxtValue}])

  % ============ gcounters =================
  % counters that emit metrics as gauges
  create_gcounter/2, % (ProgId, Key)
  create_gcounter/3, % (ProgId, Key, [{CtxtKey,CtxtVal}] | Desc)
  create_gcounter/4, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc)
  create_gcounter/5, % (ProgId, Key, [{CtxtKey,CtxtVal}], Desc, InitialAmount)
  gincrement/2,      % (ProgId, Key)
  gincrement/3,      % (ProgId, Key, Increment)
  gincrement/4,      % (ProgId, Key, [{ContextKey,ContextValue}], Increment)
  fetch_gcounter/2,  % (ProgId, Key)
  fetch_gcounter/3,  % (ProgId, Key, [{CtxtKey,CtxtValue}])
  remove_gcounter/2, % (ProgId, Key)
  remove_gcounter/3, % (ProgId, Key, [{CtxtKey,CtxtValue}])

  % ------------ tracing functions -----------
  send_trace/3,
  send_trace/5,

  % ------------ performance tracing functions -----------
  send_perf_info/3,  % (Id, CallerLabel, Timings)
  send_perf_info/4,  % (Id, CallerLabel, Timings, [{ContextKey,ContextValue}])
  send_perf_info/5,  % (Id, CallerLabel, Label, StartTime, StopTime)
  send_perf_info/6,  % (Id, CallerLabel, Label, StartTime, StopTime, [{ContextKey,ContextValue}])

  % ------------ annotation functions -----------
  send_annotation/4, % (Id, Timestamp, Description, Text)
  send_annotation/5, % (Id, Timestamp, Description, Text, [Tag])
  send_annotation/6, % (Id, Timestamp, Description, Text, [Tag],[{ContextKey,ContextValue}])

  % ------------ other functions -----------
  send_stats/3,
  flush_state_init/0,
  flush_one_stat/2,
  reset_stats/0,
  stats/0,
  export_as_prometheus/0,
  all/0,
  all_event_names_as_binary/0,
  get_lwes_config/0,
  reload_config/0,
  current_config/0,
  restart/0
]).

%% gen_server callbacks
-export ( [ init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3
          ]).

-record (state, { config,
                  interval,
                  jitter,
                  timer,
                  channels = dict:new(),
                  http_config,
                  flush_config
                }).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

create_counter (ProgId, Key) ->
  create_counter (ProgId, Key, [], "", 0).
create_counter (ProgId, Key, Context = [{_,_}|_]) ->
  create_counter (ProgId, Key, Context, "", 0);
create_counter (ProgId, Key, Description)
  when is_list(Description) ; is_binary(Description) ->
  create_counter (ProgId, Key, [], Description, 0).
create_counter (ProgId, Key, Context = [{_,_}|_], Description) ->
  create_counter (ProgId, Key, Context, Description, 0).
create_counter (ProgId, Key, Context, Description, InitialAmount) ->
  mondemand_statdb:create_counter (ProgId, Key, Context, Description, InitialAmount).

increment (ProgId, Key) ->
  increment (ProgId, Key, [], 1).
increment (ProgId, Key, Context) when is_list (Context) ->
  increment (ProgId, Key, Context, 1);
increment (ProgId, Key, Amount) when is_integer (Amount) ->
  increment (ProgId, Key, [], Amount).
% this first clause is just for legacy systems
increment (ProgId, Key, Amount, Context)
  when is_integer (Amount), is_list (Context) ->
  increment (ProgId, Key, Context, Amount);
increment (ProgId, Key, Context, Amount)
  when is_integer (Amount), is_list (Context) ->
  mondemand_statdb:increment_counter (ProgId, Key, Context, Amount).

fetch_counter (ProgId, Key) ->
  fetch_counter (ProgId, Key, []).
fetch_counter (ProgId, Key, Context) ->
  mondemand_statdb:fetch_counter (ProgId, Key, Context).

remove_counter (ProgId, Key) ->
  remove_counter (ProgId, Key, []).
remove_counter (ProgId, Key, Context) ->
  mondemand_statdb:remove_counter (ProgId, Key, Context).

create_gauge (ProgId, Key) ->
  create_gauge (ProgId, Key, [], "", 0).
create_gauge (ProgId, Key, Context = [{_,_}|_]) ->
  create_gauge (ProgId, Key, Context, "", 0);
create_gauge (ProgId, Key, Description)
  when is_list(Description) ; is_binary(Description) ->
  create_gauge (ProgId, Key, [], Description, 0).
create_gauge (ProgId, Key, Context, Description) ->
  create_gauge (ProgId, Key, Context, Description, 0).
create_gauge (ProgId, Key, Context, Description, InitialAmount)
  when is_integer (InitialAmount), is_list (Context) ->
  mondemand_statdb:create_gauge (ProgId, Key, Context, Description, InitialAmount).

set (ProgId, Key, Amount) when is_integer (Amount) ->
  set (ProgId, Key, [], Amount).
set (ProgId, Key, Context, Amount)
  when is_integer (Amount), is_list (Context) ->
  mondemand_statdb:set_gauge (ProgId, Key, Context, Amount).

fetch_gauge (ProgId, Key) ->
  fetch_gauge (ProgId, Key, []).
fetch_gauge (ProgId, Key, Context) ->
  mondemand_statdb:fetch_gauge (ProgId, Key, Context).

remove_gauge (ProgId, Key) ->
  remove_gauge (ProgId, Key, []).
remove_gauge (ProgId, Key, Context) ->
  mondemand_statdb:remove_gauge (ProgId, Key, Context).

create_sample_set (ProgId, Key) ->
  create_sample_set (ProgId, Key, [], "",
                     mondemand_config:default_max_sample_size(),
                     mondemand_config:default_stats()).
create_sample_set (ProgId, Key, Context = [{_,_}|_]) ->
  create_sample_set (ProgId, Key, Context, "",
                     mondemand_config:default_max_sample_size(),
                     mondemand_config:default_stats());
create_sample_set (ProgId, Key, Description)
  when is_list(Description) ; is_binary(Description) ->
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
% Stats can be set to 'all' to include all available summary statistics
create_sample_set (ProgId, Key, Context, Description, Max, Stats) ->
  mondemand_statdb:create_sample_set (ProgId, Key, Context, Description, Max, Stats).

add_sample (ProgId, Key, Value) ->
  add_sample (ProgId, Key, [], Value).
add_sample (ProgId, Key, Context, Value)
  when is_integer (Value), is_list (Context) ->
  mondemand_statdb:add_sample (ProgId, Key, Context, Value).

fetch_sample_set (ProgId, Key) ->
  fetch_sample_set (ProgId, Key, []).
fetch_sample_set (ProgId, Key, Context) ->
  mondemand_statdb:fetch_sample_set (ProgId, Key, Context).

remove_sample_set (ProgId, Key) ->
  remove_sample_set (ProgId, Key, []).
remove_sample_set (ProgId, Key, Context) ->
  mondemand_statdb:remove_sample_set (ProgId, Key, Context).

create_gcounter (ProgId, Key) ->
  create_gcounter (ProgId, Key, [], "", 0).
create_gcounter (ProgId, Key, Context = [{_,_}|_]) ->
  create_gcounter (ProgId, Key, Context, "", 0);
create_gcounter (ProgId, Key, Description)
  when is_list(Description) ; is_binary(Description) ->
  create_gcounter (ProgId, Key, [], Description, 0).
create_gcounter (ProgId, Key, Context = [{_,_}|_], Description) ->
  create_gcounter (ProgId, Key, Context, Description, 0).
create_gcounter (ProgId, Key, Context, Description, InitialAmount)
  when is_list(Context), is_integer(InitialAmount) ->
  mondemand_statdb:create_gcounter (ProgId, Key, Context, Description, InitialAmount).

gincrement (ProgId, Key) ->
  gincrement (ProgId, Key, [], 1).
gincrement (ProgId, Key, Context) when is_list (Context) ->
  gincrement (ProgId, Key, Context, 1);
gincrement (ProgId, Key, Amount) when is_integer (Amount) ->
  gincrement (ProgId, Key, [], Amount).
gincrement (ProgId, Key, Context, Amount)
  when is_list (Context), is_integer (Amount) ->
  mondemand_statdb:increment_gcounter (ProgId, Key, Context, Amount).

fetch_gcounter (ProgId, Key) ->
  fetch_gcounter (ProgId, Key, []).
fetch_gcounter (ProgId, Key, Context) ->
  mondemand_statdb:fetch_gcounter (ProgId, Key, Context).

remove_gcounter (ProgId, Key) ->
  remove_gcounter (ProgId, Key, []).
remove_gcounter (ProgId, Key, Context) ->
  mondemand_statdb:remove_gcounter (ProgId, Key, Context).

all () ->
  mondemand_statdb:all().

all_event_names_as_binary () -> ?ALL_EVENTS_BINARY.

get_lwes_config () ->
  mondemand_config:lwes_config().

stats () ->
  mondemand_statdb:all().

export_as_prometheus () ->
  % we export a 'minute ago' which actually just effects statsets.  This uses
  % the previous minutes statset db which will be complete and will export that
  % time as well
  mondemand_statdb:map_then (
    fun (S,A) ->
      [mondemand_statsmsg:to_prometheus(S) | A]
    end,
    [],
    1).

trace_info_in_dict (Dict) ->
  trace_owner_in_dict (Dict) andalso trace_id_in_dict (Dict).

trace_id_in_dict (Dict) ->
  dict:is_key (?MD_TRACE_ID_KEY_BIN, Dict)
  orelse dict:is_key (?MD_TRACE_ID_KEY_LIST, Dict)
  orelse dict:is_key (?MD_TRACE_ID_KEY_ATOM, Dict).

trace_owner_in_dict (Dict) ->
  dict:is_key (?MD_TRACE_OWNER_KEY_BIN, Dict)
  orelse dict:is_key (?MD_TRACE_OWNER_KEY_LIST, Dict)
  orelse dict:is_key (?MD_TRACE_OWNER_KEY_ATOM, Dict).

trace_id_in_list (List) ->
  proplists:is_defined (?MD_TRACE_ID_KEY_BIN, List)
  orelse proplists:is_defined (?MD_TRACE_ID_KEY_LIST, List)
  orelse proplists:is_defined (?MD_TRACE_ID_KEY_ATOM, List).

trace_owner_in_list (List) ->
  proplists:is_defined (?MD_TRACE_OWNER_KEY_BIN, List)
  orelse proplists:is_defined (?MD_TRACE_OWNER_KEY_LIST, List)
  orelse proplists:is_defined (?MD_TRACE_OWNER_KEY_ATOM, List).

send_trace (ProgId, Message, Context) ->
  case Context of
    List when is_list (Context) ->
      case trace_id_in_list (List) andalso trace_owner_in_list (List) of
          true ->
            send_event (
              #lwes_event {
                name = ?MD_TRACE_EVENT,
                attrs = dict:from_list (
                          [ {?MD_TRACE_SRC_HOST_KEY, mondemand_config:host() },
                            {?MD_TRACE_PROG_ID_KEY, ProgId},
                            {?MD_TRACE_MESSAGE_KEY, Message}
                            | List ]
                        )
              });
          false ->
            {error, required_fields_not_set}
      end;
    Dict ->
      case trace_info_in_dict (Dict) of
        true ->
          send_event (
            #lwes_event {
              name = ?MD_TRACE_EVENT,
              attrs = dict:store (?MD_TRACE_PROG_ID_KEY, ProgId,
                        dict:store (?MD_TRACE_SRC_HOST_KEY, mondemand_config:host(),
                          dict:store (?MD_TRACE_MESSAGE_KEY, Message,
                                      Dict)))
            });
        false -> {error, required_fields_not_set}
      end
  end.

-type trace_key() :: binary() | iolist() | atom().
-type trace_dict() :: term(). %% Could be dict() or dict:dict().
%% @doc Emits a trace event to the mondemand trace server.  These trace events
%% may be viewed in the trace interface of the
%% <a href="https://github.com/mondemand/mondemand-server">mondemand server</a>.
%%
%% The trace event is characterized by five attributes.
%%
%% A trace usually contains multiple related events that are all due to a
%% single request.  The `Owner' and `TraceId' identify the request, and are
%% used by the mondemand server's trace interface to select which messages to
%% view.
%%
%% The `ProgId' identifies the source of the trace message, and is useful when
%% a trace contains messages from several components.
%%
%% The `Message' is a short string that contains the main information of the
%% trace event.
%%
%% The `Context' is either a dict or a list of additional key/value pairs that
%% provide additional information about the message.  Mondemand's trace
%% interface attempts to interpret each context values as JSON if possible and
%% will display them in a structured way if successful.
%%
%% `send_trace' is very liberal about the values it accepts, and accepts both
%% iodata and atom for its arguments.
%%
-spec send_trace(ProgId::trace_key(), Owner::trace_key(), TraceId::trace_key(), Message::trace_key(), Context) -> 'ok'
        when Context :: list({Key::trace_key(), Value::trace_key()}) | trace_dict().

send_trace (ProgId, Owner, TraceId, Message, Context) ->
  case Context of
    List when is_list (Context) ->
      send_event (
        #lwes_event {
          name = ?MD_TRACE_EVENT,
          attrs = dict:from_list (
                    [ { ?MD_TRACE_PROG_ID_KEY, ProgId },
                      { ?MD_TRACE_OWNER_KEY_BIN, Owner },
                      { ?MD_TRACE_ID_KEY_BIN, TraceId },
                      { ?MD_TRACE_SRC_HOST_KEY, mondemand_config:host() },
                      { ?MD_TRACE_MESSAGE_KEY, Message }
                      | List ]
                  )
        });
    Dict ->
      send_event (
        #lwes_event {
          name = ?MD_TRACE_EVENT,
          attrs = dict:store (?MD_TRACE_PROG_ID_KEY, ProgId,
                    dict:store (?MD_TRACE_OWNER_KEY_BIN, Owner,
                      dict:store (?MD_TRACE_ID_KEY_BIN, TraceId,
                        dict:store (?MD_TRACE_SRC_HOST_KEY, mondemand_config:host(),
                          dict:store (?MD_TRACE_MESSAGE_KEY, Message,
                                      Dict)))))
        })
  end.

send_perf_info (Id, CallerLabel, Timings, Context)
  when is_list (Timings), is_list (Context) ->
    Event =
      mondemand_perfmsg:to_lwes (
        mondemand_perfmsg:new (Id, CallerLabel, Timings, Context)
      ),
    send_event (Event).

send_perf_info (Id, CallerLabel, Timings)
  when is_list (Timings) ->
    send_perf_info (Id, CallerLabel, Timings, []).

send_perf_info (Id, CallerLabel, Label, StartTime, StopTime, Context)
  when is_list (Context) ->
    Event =
      mondemand_perfmsg:to_lwes (
        mondemand_perfmsg:new (Id, CallerLabel, Label,
                               StartTime, StopTime, Context)
      ),
    send_event (Event).

send_perf_info (Id, CallerLabel, Label, StartTime, StopTime) ->
  send_perf_info (Id, CallerLabel, Label, StartTime, StopTime, []).

send_annotation (Id, Time, Description, Text) ->
  send_annotation (Id, Time, Description, Text, [], []).

send_annotation (Id, Time, Description, Text, Tags) ->
  send_annotation (Id, Time, Description, Text, Tags, []).
send_annotation (Id, Time, Description, Text, Tags, Context) ->
  Event =
    mondemand_annotationmsg:to_lwes (
      mondemand_annotationmsg:new (Id, Time, Description, Text, Tags, Context)
    ),
  send_event (Event).

send_stats (_, _, []) ->
  ok;
send_stats (ProgId, Context, Stats) ->
  StatsMsg = mondemand_statsmsg:new (ProgId, Context, Stats),
  case not mondemand_config:lwes_stats_disabled() of
    true ->
      Event = mondemand_statsmsg:to_lwes (StatsMsg),
      send_event (Event);
    false ->
      ok
  end.

flush ({FlushModule, FlushStatePrepFunction, FlushFunction}) ->
  case mondemand_config:vmstats_prog_id () of
    undefined -> ok;
    ProgId ->
      Context = mondemand_config:vmstats_context (),
      VmStats = mondemand_vmstats:to_mondemand (),
      send_stats (ProgId, Context, VmStats)
  end,
  % allow some initialization of state to be sent to each invocation
  Start = os:timestamp (),
  Ret = mondemand_statdb:flush (1,
                                fun FlushModule:FlushFunction/2,
                                FlushModule:FlushStatePrepFunction()
                               ),
  Finish = os:timestamp (),
  TotalMillis = mondemand_util:now_diff_milliseconds (Finish,Start),
  increment (mondemand_erlang, flush_count),
  increment (mondemand_erlang, flush_total_millis, TotalMillis),
  Ret.

reset_stats () ->
  mondemand_statdb:reset_stats().

reload_config () ->
  gen_server:call (?MODULE, reload).

restart () ->
  gen_server:call (?MODULE, restart).

current_config () ->
  gen_server:call (?MODULE, current).

%-=====================================================================-
%-                        gen_server callbacks                         -
%-=====================================================================-
init([]) ->

  IntervalSecs = mondemand_config:send_interval(),

  % use milliseconds for interval
  Interval = IntervalSecs * 1000,

  HttpConfig =
    case mondemand_config:get_http_config () of
      {error, _} -> undefined;
      HC -> HC
    end,
  FlushConfig = mondemand_config:flush_config(),

  % initialize a few internal counters
  create_counter(mondemand_erlang, flush_count, [],
                 "The total number of times events have been flushed", 0),
  create_counter(mondemand_erlang, flush_total_millis, [],
                 "The total amount of time in milliseconds taken to flush", 0),

  case mondemand_config:lwes_config () of
    {error, Error} ->
      {stop, {error, Error}};
    Config ->
      case open_all (Config) of
        {ok, ChannelDict} ->
          Jitter =
            case Interval =/= 0 of
              true ->
                % use milliseconds for jitter
                J = rand:uniform (IntervalSecs - 1) * 1000,

                % shoot for starting flushes some time after the next
                % round minute
                timer:send_after (
                  mondemand_util:millis_to_next_round_minute() + J,
                  ?MODULE,
                  flush),
                J;
              false ->
                0
            end,

          { ok,
            #state{
              config = Config,
              jitter = Jitter,
              interval = Interval,
              channels = ChannelDict,
              http_config = HttpConfig,
              flush_config = FlushConfig
            }
          };
        {error, Error} ->
          { stop, {error, lwes, Error} }
      end
  end.

% restart with currently assigned config
handle_call (restart, _From,
             State = #state { config = Config,
                              channels = OldChannelDict }) ->
  case open_all (Config) of
    {ok, NewChannelDict} ->
      close_all (OldChannelDict),
      {reply, ok, State#state {channels = NewChannelDict}};
    {error, E} ->
      {reply, {error, E, Config}, State}
  end;
% reload config, and set in state
handle_call (reload, _From, State) ->
  case mondemand_config:lwes_config () of
    {error, Error} ->
      {replay, {error, Error}, State};
    NewConfig ->
      {reply, ok, State#state {config = NewConfig}}
  end;
% return current config from state
handle_call (current, _From, State = #state { config = Config }) ->
  {reply, Config, State};
% fallback
handle_call (_Request, _From, State) ->
  {reply, ok, State}.

% send an event
handle_cast ({send, Name, Event},
             State = #state { http_config = HttpConfig }) ->
  NewState =
    case Name of
      ?MD_TRACE_EVENT ->
        Bin = lwes_event:to_binary(Event),
        case size(Bin) of
          X when X > 65535 ->
            post_via_http (Bin, HttpConfig),
            State;
          _ -> emit_one (Name, Bin, State)
        end;
      _ ->
        emit_one (Name, Event, State)
    end,
  { noreply, NewState };
handle_cast (_Request, State) ->
  {noreply, State}.

emit_one (Name, Event, State = #state { channels = ChannelDict }) ->
  case dict:find (Name, ChannelDict) of
    {ok, Channels} ->
      NewChannels = lwes:emit (Channels, Event),
      State#state { channels = dict:store (Name, NewChannels, ChannelDict)};
    _ ->
      error_logger:error_msg ("Unrecognized event ~p : ~p",
                              [Name, dict:to_list(ChannelDict)]),
      State
  end.

handle_info (flush, State = #state {timer = undefined,
                                    interval = Interval,
                                    flush_config = FlushConfig
                                   }) ->
  {ok, TRef} = timer:send_interval (Interval, ?MODULE, flush),
  flush (FlushConfig),
  {noreply, State#state {timer = TRef}};
handle_info (flush, State = #state {flush_config = FlushConfig}) ->
  flush (FlushConfig),
  {noreply, State#state {}};
handle_info (_Info, State) ->
  {noreply, State}.

terminate (_Reason, #state { channels = ChannelDict }) ->
  close_all (ChannelDict),
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
send_event (Events) when is_list (Events) ->
  [ send_event (Event) || Event <- Events ];
send_event (Event = #lwes_event { name = Name }) ->
  gen_server:cast (?MODULE, {send, Name, lwes_event:to_binary (Event)}).

flush_state_init () ->
  % for flushing keep track of if stats are enabled and use that as the state
  case not mondemand_config:lwes_stats_disabled() of
    true -> true;
    false -> false
  end.

flush_one_stat (StatsMsg, true) ->
  send_event (mondemand_statsmsg:to_lwes (StatsMsg)),
  true;
flush_one_stat (_, State) ->
  State.

open_all (Config) ->
  lists:foldl (fun ({T,C},{ok, D}) ->
                     EventKey =
                       case T of
                         trace -> ?MD_TRACE_EVENT;
                         log -> ?MD_LOG_EVENT;
                         perf -> ?MD_PERF_EVENT;
                         stats -> ?MD_STATS_EVENT;
                         annotation -> ?MD_ANNOTATION_EVENT
                       end,
                     case lwes:open (emitters, C) of
                       {ok, Chnnls} ->
                         {ok, dict:store (EventKey, Chnnls,D)};
                       {error, Error} ->
                         {error, Error}
                     end;
                   (_,{error, Error}) ->
                     {error, Error}
               end,
               {ok, dict:new()},
               Config
              ).

close_all (ChannelDict) ->
  [ lwes:close (Channels) || {_, Channels} <- dict:to_list (ChannelDict) ].

post_via_http (Bin, HttpConfig) ->
  case HttpConfig of
    undefined -> ok;
    _ -> Endpoint = proplists:get_value (trace, HttpConfig),
         {ok, _ } = httpc:request (post,
                                   {Endpoint, [], "", Bin},
                                   [], [])
  end.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
%-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

%-endif.
