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
%% * Gauges - instantaneous values such as temperature (or more regular
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

           % stats functions
           increment/2,
           increment/3,
           increment/4,
           set/3,
           set/4,
           send_stats/3,
           reset_stats/0,
           stats/0,

           % tracing functions
           send_trace/3,
           send_trace/5,

           % other functions
           all/0,
           get_lwes_config/0,
           log_to_mondemand/0,
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

-record (state, {config, interval, delay, table, timer, channel}).

-define (TABLE, mondemand_stats).
-define (DEFAULT_SEND_INTERVAL, 60).
-define (MAX_METRIC_VALUE, 9223372036854775807).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

increment (ProgId, Key) ->
  increment (ProgId, Key, 1, []).

increment (ProgId, Key, Amount) when is_integer (Amount) ->
  increment (ProgId, Key, Amount, []).

increment (ProgId, Key, Amount, Context)
  when is_integer (Amount), is_list (Context) ->
  SortedContext = lists:keysort (1, Context),

  % LWES is sending int64 values, so wrap at the max int64 integer back to
  % zero
  try ets:update_counter (?TABLE,
                          {ProgId, SortedContext, counter, Key},
                          {2, Amount, ?MAX_METRIC_VALUE, 0}) of
    _ -> ok
  catch
    error:badarg ->
      ets:insert (?TABLE, { {ProgId, SortedContext, counter, Key}, Amount })
  end.

set (ProgId, Key, Amount) when is_integer (Amount) ->
  set (ProgId, Key, Amount, []).

set (ProgId, Key, Amount, Context)
  when is_integer (Amount), is_list (Context) ->
  SortedContext = lists:keysort (1,Context),

  % if we would overflow a gauge, instead of going negative just leave it
  % at the max value and return false from set
  case Amount =< ?MAX_METRIC_VALUE of
    true ->
      ets:insert (?TABLE, { {ProgId, SortedContext, gauge, Key}, Amount});
    false ->
      ets:insert (?TABLE, { {ProgId, SortedContext, gauge, Key},
                          ?MAX_METRIC_VALUE}),
      false
  end.

% return current config
get_lwes_config () ->
  % lwes config can be specified in one of two ways
  %   1. the lwes_channel application variable
  %   2. from the file referenced by the config_file application variable
  % they are checked in that order
  case application:get_env (mondemand, lwes_channel) of
    {ok, {H,P}} when is_list (H), is_integer (P) ->
      {1, [{H,P}]}; % allow old config style to continue to work
    {ok, Config} -> Config;      % new style is to just pass anything through
    undefined ->
      case application:get_env (mondemand, config_file) of
        {ok, File} ->
          case file:read_file (File) of
            {ok, Bin} ->
              {match, [HostOrHosts]} =
                re:run (Bin, "MONDEMAND_ADDR=\"([^\"]+)\"",
                             [{capture, all_but_first, list}]),
              {match, [Port]} = re:run (Bin, "MONDEMAND_PORT=\"([^\"]+)\"",
                                        [{capture, all_but_first, list}]),
              IntPort = list_to_integer (Port),
              io:format ("HostOrHosts ~p~n",[HostOrHosts]),
              Split = re:split(HostOrHosts,",",[{return, list}]),
              io:format ("Split ~p~n",[Split]),
              case Split of
                [] -> {error, config_file_issue};
                [Host] -> {1, [ {Host, IntPort} ]};
                L when is_list (L) ->
                  % allow emitting the same thing to multiple addresses
                  { length (L), [ {H, IntPort} || H <- L ] }
              end;
            E ->
              E
          end;
        undefined ->
          {error, no_lwes_channel_configured}
      end
  end.

all () ->
  ets:tab2list (?TABLE).

stats () ->
  io:format ("~-21s ~-35s ~-20s~n",["prog_id", "key", "value"]),
  io:format ("~-21s ~-35s ~-20s~n",["---------------------",
                                    "-----------------------------------",
                                    "--------------------"]),
  [
    begin
      io:format ("~-21s ~-35s ~-20b~n",
                 [
                   case ProgId of
                     undefined -> "unknown";
                     P ->  mondemand_util:stringify (P)
                   end,
                   mondemand_util:stringify (Key),
                   Value
                 ]),
      lists:foreach (fun ({CKey, CValue}) ->
                           io:format (" {~s, ~s}~n", [
                               mondemand_util:stringify (CKey),
                               mondemand_util:stringify (CValue)
                             ])
                     end, Context)
    end
    || {{ProgId, Context, _Type, Key},Value}
    <- lists:sort (ets:tab2list (?TABLE))
  ],
  ok.

send_trace (ProgId, Message, Context) ->
  case Context of
    Dict when is_tuple (Context) andalso element (1, Context) =:= dict ->
      case mondemand_util:key_in_dict (?TRACE_ID_KEY, Dict)
        andalso mondemand_util:key_in_dict (?OWNER_ID_KEY, Dict) of
        true ->
          send_event (
            #lwes_event {
              name = ?TRACE_EVENT,
              attrs = dict:store (?PROG_ID_KEY, ProgId,
                        dict:store (?SRC_HOST_KEY, net_adm:localhost(),
                          dict:store (?MESSAGE_KEY, Message,
                                      Dict)))
            });
        false ->
          {error, required_fields_not_set}
      end;
    List when is_list (Context) ->
      case mondemand_util:key_in_list (?TRACE_ID_KEY, List)
        andalso mondemand_util:key_in_list (?OWNER_ID_KEY, List) of
          true ->
            send_event (
              #lwes_event {
                name = ?TRACE_EVENT,
                attrs = dict:from_list (
                          [ {?SRC_HOST_KEY, net_adm:localhost() },
                            {?PROG_ID_KEY, ProgId},
                            {?MESSAGE_KEY, Message}
                            | List ]
                        )
              });
          false ->
            {error, required_fields_not_set}
      end;
    _ ->
      {error, context_format_not_recognized}
  end.

send_trace (ProgId, Owner, TraceId, Message, Context) ->
  case Context of
    Dict when is_tuple (Context) andalso element (1, Context) =:= dict ->
      send_event (
        #lwes_event {
          name = ?TRACE_EVENT,
          attrs = dict:store (?PROG_ID_KEY, ProgId,
                    dict:store (?OWNER_ID_KEY, Owner,
                      dict:store (?TRACE_ID_KEY, TraceId,
                        dict:store (?SRC_HOST_KEY, net_adm:localhost(),
                          dict:store (?MESSAGE_KEY, Message,
                                      Dict)))))
        });
    List when is_list (Context) ->
      send_event (
        #lwes_event {
          name = ?TRACE_EVENT,
          attrs = dict:from_list (
                    [ { ?PROG_ID_KEY, ProgId },
                      { ?OWNER_ID_KEY, Owner },
                      { ?TRACE_ID_KEY, TraceId },
                      { ?SRC_HOST_KEY, net_adm:localhost() },
                      { ?MESSAGE_KEY, Message }
                      | List ]
                  )
        });
    _ ->
      {error, context_format_not_recognized}
  end.

send_stats (ProgId, Context, Stats) ->
  Event =
    mondemand_stats:to_lwes (
      mondemand_stats:new (ProgId, Context, Stats)
    ),
  send_event (Event).

log_to_mondemand () ->
  send_event (
    mondemand_stats:to_lwes (mondemand_stats:from_ets (?TABLE))
  ).

reset_stats () ->
  ets:foldl (fun ({K, _}, Prev) ->
               ets:update_element (?TABLE, K, [{2,0}]) andalso Prev
             end,
             true,
             ?TABLE).

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
  Interval =
    case application:get_env (mondemand, send_interval) of
      {ok, I} when is_integer (I) -> I;
      _ -> ?DEFAULT_SEND_INTERVAL
    end,

  % reload interval is seconds, but gen_server timeouts are millis so convert
  Delay = Interval * 1000,

  case get_lwes_config () of
    {error, Error} ->
      {stop, {error, Error}};
    Config ->
      case lwes:open (emitters, Config) of
        {ok, Channel} ->
          TabId = ets:new (?TABLE, [set,
                                    public,
                                    named_table,
                                    {write_concurrency, true}]),

          % a Delay of 0 actually turns off emission
          {ok, TRef} =
            case Delay =/= 0 of
              true ->
                timer:apply_interval (Delay, ?MODULE, log_to_mondemand, []);
              false ->
                {ok, undefined}
            end,

          { ok,
            #state{
              config = Config,
              table = TabId,
              delay = Delay,
              interval = Interval,
              timer = TRef,
              channel = Channel
            },
            Delay
          };
        {error, Error} ->
          { stop, {error, lwes, Error}}
      end
  end.

% restart with currently assigned config
handle_call (restart, _From,
             State = #state { config = Config, channel = Channel }) ->
  case lwes:open (emitter, Config) of
    {ok, NewChannel} ->
      lwes:close (Channel),
      {reply, ok, State#state {channel = NewChannel}};
    {error, E} ->
      {reply, {error, E, Config}, State}
  end;
% reload config, and set in state
handle_call (reload, _From, State) ->
  case get_lwes_config() of
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

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
