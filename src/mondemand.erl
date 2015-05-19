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
          parse_config (File);
        undefined ->
          {error, no_lwes_channel_configured}
      end
  end.

all () ->
  mondemand_statdb:all().

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
                          [ {?MD_TRACE_SRC_HOST_KEY, net_adm:localhost() },
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
                        dict:store (?MD_TRACE_SRC_HOST_KEY, net_adm:localhost(),
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
                      { ?MD_TRACE_SRC_HOST_KEY, net_adm:localhost() },
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
                        dict:store (?MD_TRACE_SRC_HOST_KEY, net_adm:localhost(),
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

  % use milliseconds for interval and for jitter
  Interval = IntervalSecs * 1000,
  Jitter = crypto:rand_uniform (1, IntervalSecs) * 1000,

  case get_lwes_config () of
    {error, Error} ->
      {stop, {error, Error}};
    Config ->
      case lwes:open (emitters, Config) of
        {ok, Channel} ->

          case Interval =/= 0 of
            true ->
              % shoot for starting flushes some time after the next
              % round minute
              timer:send_after (
                mondemand_util:millis_to_next_round_minute() + Jitter,
                ?MODULE,
                flush);
            false ->
              ok
          end,

          { ok,
            #state{
              config = Config,
              jitter = Jitter ,
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
  case lwes:open (emitter, Config) of
    {ok, NewChannel} ->
      lwes:close (OldChannel),
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

parse_config (File) ->
  case file:read_file (File) of
    {ok, Bin} ->
      HostOrHosts =
        case re:run (Bin, "MONDEMAND_ADDR=\"([^\"]+)\"",
                     [{capture, all_but_first, list}]) of
          nomatch -> [];
          {match, [H]} -> H
        end,
      Port =
        case re:run (Bin, "MONDEMAND_PORT=\"([^\"]+)\"",
                     [{capture, all_but_first, list}]) of
          nomatch -> undefined;
          {match, [P]} -> list_to_integer(P)
        end,
      TTL =
        case re:run (Bin, "MONDEMAND_TTL=\"([^\"]+)\"",
                          [{capture, all_but_first, list}]) of
          nomatch -> undefined;
          {match, [T]} -> list_to_integer(T)
        end,

      case Port of
        undefined -> {error, no_port_in_config_file};
        _ ->
          Split = re:split(HostOrHosts,",",[{return, list}]),
          case Split of
            [] -> {error, config_file_issue};
            [Host] ->
              case TTL of
                undefined -> {1, [ {Host, Port} ]};
                _ -> {1, [ {Host, Port, TTL} ] }
              end;
            L when is_list (L) ->
              % allow emitting the same thing to multiple addresses
              case TTL of
                undefined -> { length (L), [ {H, Port} || H <- L ] };
                _ -> { length (L), [ {H, Port, TTL} || H <- L ] }
              end
          end
      end;
    E ->
      E
  end.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
%-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

config_file_test_ () ->
  ConfigFile = "mondemand.config",
  WriteConfig =
    fun (Host, Port, TTL) ->
      % TODO: should I permute the order of the config lines?
      {ok, File} = file:open (ConfigFile, [write]),
      ok = io:format (File, "MONDEMAND_ADDR=\"~s\"~n",[Host]),
      ok = io:format (File, "MONDEMAND_PORT=\"~b\"~n",[Port]),
      case TTL of
        undefined -> ok;
        _ -> ok = io:format (File, "MONDEMAND_TTL=\"~b\"~n",[TTL])
      end,
      file:close (File)
    end,
  DeleteConfig =
    fun () ->
      file:delete (ConfigFile)
    end,
  { inorder,
    [
      % delete any files from failed tests
      fun () -> DeleteConfig () end,
      fun () -> ok = WriteConfig ("localhost", 20602, undefined) end,
      ?_assertEqual ({1, [{"localhost",20602}]}, parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end,
      fun () -> ok = WriteConfig ("localhost", 20602, 3) end,
      ?_assertEqual ({1, [{"localhost",20602, 3}]}, parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end
    ]
  }.

%-endif.
