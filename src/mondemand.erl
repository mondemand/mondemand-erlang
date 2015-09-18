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
           increment/3,   % (ProgId, Key, Amount | Context )
           increment/4,   % (ProgId, Key, Amount | Context, Context | Amount )
           % gauges
           set/3,         % (ProgId, Key, Value)
           set/4,         % (ProgId, Key, Context, Value)
           % statsets
           add_sample/3,  % (ProgId, Key, Value)
           add_sample/4,  % (ProgId, Key, Context, Value)

           % tracing functions
           send_trace/3,
           send_trace/5,

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

-record (state, {config, interval, jitter, timer, channel}).

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

send_stats (ProgId, Context, Stats) ->
  Event =
    mondemand_statsmsg:to_lwes (
      mondemand_statsmsg:new (ProgId, Context, Stats)
    ),
  send_event (Event).

flush () ->
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

  case mondemand_config:lwes_config () of
    {error, Error} ->
      {stop, {error, Error}};
    Config ->
      case lwes:open (emitters, Config) of
        {ok, Channel} ->

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
              channel = Channel
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
             State = #state { config = Config, channel = OldChannel }) ->
  case lwes:open (emitters, Config) of
    {ok, NewChannel} ->
      lwes:close (OldChannel),
      {reply, ok, State#state {channel = NewChannel}};
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
handle_cast ({send, Event}, State = #state { channel = Channel }) ->
  lwes:emit (Channel, Event),
  {noreply, State};
% fallback
handle_cast (_Request, State) ->
  {noreply, State}.

handle_info (flush, State = #state {timer = undefined, interval = Interval}) ->
  {ok, TRef} = timer:send_interval (Interval, ?MODULE, flush),
  flush (),
  {noreply, State#state {timer = TRef}};
handle_info (flush, State = #state {}) ->
  flush (),
  {noreply, State#state {}};
handle_info (_Info, State) ->
  {noreply, State}.

terminate (_Reason, _State) ->
  ok.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================
send_event (Events) when is_list (Events) ->
  [ send_event (Event) || Event <- Events ];
send_event (Event = #lwes_event {}) ->
  gen_server:cast (?MODULE, {send, lwes_event:to_binary (Event)}).

flush_one (Stats) ->
  send_event (mondemand_statsmsg:to_lwes (Stats)).

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
%-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

%-endif.
