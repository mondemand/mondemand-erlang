-module (mondemand).

-include_lib ("lwes/include/lwes.hrl").

-compile({parse_transform, ct_expand}).

-behaviour (gen_server).

%% API
-export ( [start_link/0,
           increment/2,
           increment/3,
           increment/4,
           all/0,
           get_lwes_config/0,
           log_to_mondemand/0,
           send_trace/3,
           send_trace/5,
           send_stats/3,
           reset_stats/0,
           reload_config/0,
           restart/0,
           current_config/0,
           stats/0,
           stat_key/1,
           stat_val/1,
           stat_type/1,
           ctxt_key/1,
           ctxt_val/1
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

-define (STATS_EVENT, "MonDemand::StatsMsg").
-define (TRACE_EVENT, "MonDemand::TraceMsg").

% tokens in Stats message
-define (PROG_ID,    "prog_id").
-define (STATS_NUM,  "num").
-define (STATS_K,    "k").
-define (STATS_V,    "v").
-define (STATS_T,    "t").
-define (CTXT_NUM,   "ctxt_num").
-define (CTXT_K,     "ctxt_k").
-define (CTXT_V,     "ctxt_v").
-define (STATS_HOST, "host").

% tokens in trace message
-define (TRACE_ID_KEY, "mondemand.trace_id").
-define (OWNER_ID_KEY, "mondemand.owner").
-define (PROG_ID_KEY,  "mondemand.prog_id").
-define (SRC_HOST_KEY, "mondemand.src_host").
-define (MESSAGE_KEY,  "mondemand.message").

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
  gen_server:start_link ({local, ?MODULE}, ?MODULE, [], []).

increment (ProgId, Key) ->
  increment (ProgId, Key, 1, []).

increment (ProgId, Key, Amount) when is_integer (Amount) ->
  increment (ProgId, Key, Amount, []).

increment (ProgId, Key, Amount, Context) when is_integer (Amount) ->
  SortedContext = lists:sort ([{?PROG_ID, ProgId} | Context]),

  try ets:update_counter (?TABLE, {SortedContext, Key}, {2, Amount}) of
    _ -> ok
  catch
    error:badarg -> ets:insert (?TABLE, { {SortedContext, Key}, Amount })
  end.

% return current config
get_lwes_config () ->
  % lwes config can be specified in one of two ways
  %   1. the lwes_channel application variable
  %   2. from the file referenced by the config_file application variable
  % they are checked in that order
  case application:get_env (mondemand, lwes_channel) of
    {ok, {H,P}} ->
      {H,P};
    undefined ->
      case application:get_env (mondemand, config_file) of
        {ok, File} ->
          case file:read_file (File) of
            {ok, Bin} ->
              {match, [Host]} = re:run (Bin, "MONDEMAND_ADDR=\"([^\"]+)\"",
                                        [{capture, all_but_first, list}]),
              {match, [Port]} = re:run (Bin, "MONDEMAND_PORT=\"([^\"]+)\"",
                                        [{capture, all_but_first, list}]),
              IntPort = list_to_integer (Port),
              {Host, IntPort};
            E ->
              E
          end;
        undefined ->
          {error, no_lwes_channel_configed}
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
                 [stringify (proplists:get_value ("prog_id",Context,"unknown")),
                  stringify (Key),
                  Value]),
      lists:foreach (fun ({"prog_id",_}) ->
                           ok;
                         ({CKey, CValue}) ->
                           io:format (" {~s, ~s}~n", [stringify (CKey),
                                                      stringify (CValue)])
                     end, Context)
    end
    || {{Context,Key},Value}
    <- lists:sort (ets:tab2list (?TABLE))
  ],
  ok.

send_trace (ProgId, Message, Context) ->
  case Context of
    Dict when is_tuple (Context) andalso element (1, Context) =:= dict ->
      case key_in_dict (?TRACE_ID_KEY, Dict)
        andalso key_in_dict (?OWNER_ID_KEY, Dict) of
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
      case key_in_list (?TRACE_ID_KEY, List)
        andalso key_in_list (?OWNER_ID_KEY, List) of
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
  % add 1 for host
  ContextNum = length (Context) + 1,
  StatsNum   = length (Stats),
  Event =
    #lwes_event {
      name  = ?STATS_EVENT,
      attrs = lists:flatten (
                [ { ?LWES_STRING, ?PROG_ID, ProgId },
                  { ?LWES_U_INT_16, ?STATS_NUM, StatsNum },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                          [ {?LWES_STRING, stat_key (I), stringify (K)},
                            {?LWES_INT_64, stat_val (I), V}
                          ];
                        (I, {T, K, V}) ->
                          [ {?LWES_STRING, stat_key (I), stringify (K)},
                            {?LWES_INT_64, stat_val (I), V},
                            {?LWES_STRING, stat_type (I), stringify (T)}
                          ]
                    end, lists:seq (1, StatsNum), Stats),
                  { ?LWES_U_INT_16, ?CTXT_NUM, ContextNum },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                        [ {?LWES_STRING, ctxt_key (I), stringify (K)},
                          {?LWES_STRING, ctxt_val (I), stringify (V)}
                        ]
                    end, lists:seq (1, ContextNum-1), Context),
                  {?LWES_STRING, ctxt_key (ContextNum), ?STATS_HOST },
                  {?LWES_STRING, ctxt_val (ContextNum), net_adm:localhost() }
                ])
    },
  send_event (Event).

log_to_mondemand () ->
  All = all(),
  % struct in ets is
  %  { { Context, Key }, Value }
  % but I want to send this in the fewest number of mondemand-tool calls
  % so I need to
  [ begin
      % extract prog id from the context
      {_, ProgId} = proplists:lookup (?PROG_ID, C),
      send_stats (ProgId,
                  % delete the prog_id from the context
                  proplists:delete (?PROG_ID, C),
                  % iterate through all the keys pulling out each for this
                  % context
                  [ { counter, K, V } || { { C2, K }, V } <- All, C2 =:= C ])
    end
    || C <- lists:usort ([CK || { { CK, _ }, _ } <- All])
  ],
  ok.

reset_stats () ->
  ets:delete_all_objects (?TABLE).

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
      case lwes:open (emitter, Config) of
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
send_event (Event) ->
  gen_server:cast (?MODULE, {send, lwes_event:to_binary (Event)}).

stringify (I) when is_integer (I) ->
  integer_to_list (I);
stringify (F) when is_float (F) ->
  float_to_list (F);
stringify (A) when is_atom (A) ->
  atom_to_list (A);
stringify (L) ->
  L.

key_in_dict (Key, Dict) when is_list (Key) ->
  dict:is_key (Key, Dict)
    orelse dict:is_key (list_to_binary(Key), Dict)
    orelse dict:is_key (list_to_atom(Key), Dict).

key_in_list (Key, List) when is_list (Key), is_list (List) ->
  proplists:is_defined (Key, List)
   orelse proplists:is_defined (list_to_binary(Key), List)
   orelse proplists:is_defined (list_to_atom(Key), List).

% generate lookup tables for lwes keys so save some time in production
-define (ELEMENT_OF_TUPLE_LIST(N,Prefix),
         element (N,
                  ct_expand:term (
                    begin
                      list_to_tuple (
                        [
                          list_to_binary (
                            lists:concat ([Prefix, integer_to_list(E-1)])
                          )
                          || E <- lists:seq(1,1024)
                        ]
                      )
                    end))).

stat_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_K).

stat_val (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_V).

stat_type (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_T).

ctxt_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_K).

ctxt_val (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_V).

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
