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
-export ( [start_link/0,
           get_state/0,

           % stats functions
           % counters
           increment/2,   % (ProgId, Key)
           increment/3,   % (ProgId, Key, Amount | [{ContextKey,ContextValue}])
           increment/4,   % (ProgId, Key, Amount | [{ContextKey,ContextValue}], [{ContextKey,ContextValue}]| Amount )
           % gauges
           set/3,         % (ProgId, Key, Value)
           set/4,         % (ProgId, Key, [{ContextKey,ContextValue}], Value)
           % statsets
           add_sample/3,  % (ProgId, Key, Value)
           add_sample/4,  % (ProgId, Key, [{ContextKey,ContextValue}], Value)

           % tracing functions
           send_trace/3,
           send_trace/5,

           % performance tracing functions
           send_perf_info/3,  % (Id, CallerLabel, Timings)
           send_perf_info/4,  % (Id, CallerLabel, Timings, [{ContextKey,ContextValue}])
           send_perf_info/5,  % (Id, CallerLabel, Label, StartTime, StopTime)
           send_perf_info/6,  % (Id, CallerLabel, Label, StartTime, StopTime, [{ContextKey,ContextValue}])

           % annotation functions
           send_annotation/4, % (Id, Timestamp, Description, Text)
           send_annotation/5, % (Id, Timestamp, Description, Text, [Tag])
           send_annotation/6, % (Id, Timestamp, Description, Text, [Tag],[{ContextKey,ContextValue}])

           % other functions
           send_stats/3,
           reset_stats/0,
           stats/0,
           all/0,
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
                  http_config
                }).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

get_state() ->
  gen_server:call (?MODULE, get_state).

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
  mondemand_statdb:increment (ProgId, Key, Context, Amount).

set (ProgId, Key, Amount) when is_integer (Amount) ->
  set (ProgId, Key, [], Amount).

set (ProgId, Key, Context, Amount)
  when is_integer (Amount), is_list (Context) ->
  mondemand_statdb:set (ProgId, Key, Context, Amount).

add_sample (ProgId, Key, Value) ->
  add_sample (ProgId, Key, [], Value).
add_sample (ProgId, Key, Context, Value)
  when is_integer (Value), is_list (Context) ->
  mondemand_statdb:add_sample (ProgId, Key, Context, Value).


all () ->
  mondemand_statdb:all().

get_lwes_config () ->
  mondemand_config:lwes_config().

stats () ->
  mondemand_statdb:all().

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

send_stats (ProgId, Context, Stats) ->
  Event =
    mondemand_statsmsg:to_lwes (
      mondemand_statsmsg:new (ProgId, Context, Stats)
    ),
  send_event (Event).

flush () ->
  case application:get_env (mondemand, vmstats) of
    {ok, L} when is_list (L) ->
      case proplists:get_value (program_id, L) of
        undefined -> ok;
        ProgId ->
          Context = proplists:get_value (context, L, []),
          VmStats = mondemand_vmstats:to_mondemand (),
          send_stats (ProgId, Context, VmStats)
      end;
    _ -> ok
  end,
  mondemand_statdb:flush (1, fun flush_one/1).

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

  IntervalSecs =
    case application:get_env (mondemand, send_interval) of
      {ok, I} when is_integer (I) -> I;
      _ -> ?MD_DEFAULT_SEND_INTERVAL
    end,

  % use milliseconds for interval
  Interval = IntervalSecs * 1000,

  HttpConfig =
    case mondemand_config:get_http_config () of
      {error, _} -> undefined;
      HC -> HC
    end,

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
                J = crypto:rand_uniform (1, IntervalSecs) * 1000,

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
              http_config = HttpConfig
            }
          };
        {error, Error} ->
          { stop, {error, lwes, Error} }
      end
  end.

% just return the state
handle_call (get_state, _From, State) ->
  {reply, State, State};
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

handle_info (flush, State = #state {timer = undefined, interval = Interval}) ->
  {ok, TRef} = timer:send_interval (Interval, ?MODULE, flush),
  flush (),
  {noreply, State#state {timer = TRef}};
handle_info (flush, State = #state {}) ->
  flush (),
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

flush_one (Stats) ->
  send_event (mondemand_statsmsg:to_lwes (Stats)).

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
