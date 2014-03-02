-module (mondemand).

-include_lib ("lwes/include/lwes.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
           stats/0
         ]).

%% gen_server callbacks
-export ( [ init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3
          ]).

-record (state, {config, table, timer, channel, http_config}).

-define (TABLE, mondemand_stats).

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

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
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

get_http_config () ->
  case application:get_env (mondemand, http_endpoint) of
    {ok, HttpConfig} ->
      HttpConfig;
    undefined ->
      case application:get_env (mondemand, config_file) of
        {ok, File} ->
          case file:read_file (File) of
            {ok, Bin} ->
              {match, [TraceEndPoint]} = 
                 re:run (Bin, "MONDEMAND_HTTP_ENDPOINT_TRACE=\"([^\"]+)\"",
                              [{capture, all_but_first, list}]),
              [{trace, TraceEndPoint}];
            E ->
              E
          end;
        undefined ->
          {error, no_http_configured}
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
    io:format ("~-21s ~-35s ~-20b~n",
               [stringify (proplists:get_value ("prog_id",Context,"unknown")),
                stringify (Key),
                Value])
    || {{Context,Key},Value}
    <- lists:sort (ets:tab2list (?TABLE))
  ],
  ok.

stringify (A) when is_atom (A) ->
  atom_to_list (A);
stringify (L) ->
  L.

send_trace (ProgId, Message, Context) ->
  Context2 = split_heavy_contexts (Context), 
  case Context2 of
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
                attrs = [ {?SRC_HOST_KEY, net_adm:localhost() },
                          {?PROG_ID_KEY, ProgId},
                          {?MESSAGE_KEY, Message}
                          | List ]
              });
          false ->
            {error, required_fields_not_set}
      end;
    _ ->
      {error, context_format_not_recognized}
  end.

send_trace (ProgId, Owner, TraceId, Message, Context) ->
  Context2 = split_heavy_contexts (Context),
  case Context2 of
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
          attrs = [ { ?PROG_ID_KEY, ProgId },
                    { ?OWNER_ID_KEY, Owner },
                    { ?TRACE_ID_KEY, TraceId },
                    { ?SRC_HOST_KEY, net_adm:localhost() },
                    { ?MESSAGE_KEY, Message }
                    | List ]
        });
    _ ->
      {error, context_format_not_recognized}
  end.

send_stats (ProgId, Context, Stats) ->
  ContextNum = length (Context),
  StatsNum   = length (Stats),
  Event =
    #lwes_event {
      name  = ?STATS_EVENT,
      attrs = lists:flatten (
                [ { ?LWES_STRING, ?PROG_ID, ProgId },
                  { ?LWES_U_INT_16, ?STATS_NUM, StatsNum },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                          [ {?LWES_STRING,
                              ?STATS_K++integer_to_list(I), to_string (K)},
                            {?LWES_INT_64,
                              ?STATS_V++integer_to_list(I), V}
                          ];
                        (I, {T, K, V}) ->
                          [ {?LWES_STRING,
                              ?STATS_K++integer_to_list(I), to_string (K)},
                            {?LWES_INT_64,
                              ?STATS_V++integer_to_list(I), V},
                            {?LWES_STRING,
                              ?STATS_T++integer_to_list(I), to_string (T)}
                          ]
                    end, lists:seq (0, StatsNum - 1), Stats),
                  % add 1 for host
                  { ?LWES_U_INT_16, ?CTXT_NUM, ContextNum + 1 },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                        [ {?LWES_STRING,
                            ?CTXT_K++integer_to_list(I), to_string (K)},
                          {?LWES_STRING,
                            ?CTXT_V++integer_to_list(I), to_string (V)}
                        ]
                    end, lists:seq (0, ContextNum - 1), Context),
                  {?LWES_STRING, ?CTXT_K++integer_to_list (ContextNum),
                    ?STATS_HOST },
                  {?LWES_STRING, ?CTXT_V++integer_to_list (ContextNum),
                    net_adm:localhost() }
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

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
  HttpConfig =
    case get_http_config () of 
      {error, _} -> undefined;
      HC -> HC
    end,           
  
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
          {ok, TRef} =
            timer:apply_interval (60 * 1000, ?MODULE, log_to_mondemand, []),
          { ok,
            #state{
              config = Config,
              table = TabId,
              timer = TRef,
              channel = Channel,
              http_config = HttpConfig
            }
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
handle_cast ({send, Event}, 
             State = #state { channel = Channel,
                              http_config = HttpConfig}) ->
  {lwes_event, EventName, Attrs} = Event,
  case EventName of 
    "MonDemand::TraceMsg" 
      -> Bin = lwes_event:to_binary(Event),
         case size(Bin) of 
           X when X > 65535 -> post_via_http (Bin, HttpConfig);
           _ -> lwes:emit (Channel, Event)
         end; 
     _ -> lwes:emit (Channel, Event)
  end,
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
  gen_server:cast (?MODULE, {send, Event}).

post_via_http(Bin, HttpConfig) ->
  case HttpConfig of 
    undefined -> ok;
    _ -> Endpoint = proplists:get_value("trace", HttpConfig), 
         httpc:request
                  (post,
                   {Endpoint, [], "", Bin},
                   [], [])
  end.

split_heavy_contexts (Context) when is_tuple (Context) ->
  dict:from_list (
    split_heavy_contexts (
      dict:to_list (Context))); 
split_heavy_contexts (Context) -> 
  lists:foldl (
    fun ({K, V}, A) 
      -> case V of 
           B when is_binary(B) -> A ++ split_chunk (K, V, 50000, 0);
           O -> A ++ [{K, O}]  
         end
    end,
    [], 
    Context).                    
                      
split_chunk (_, <<>>, _, _) -> [];
split_chunk (FieldName, Bin, ChunkSize, PartIndex) 
                       when size(Bin) =< ChunkSize ->
  case PartIndex of 
    0 -> [{FieldName, Bin}];
    _ -> [{part_name(FieldName, PartIndex), Bin}]
  end; 

split_chunk (FieldName, Bin, ChunkSize, PartIndex) -> 
  { Chunk, Rest} = split_binary (Bin, ChunkSize),
  [ {part_name(FieldName, PartIndex), Chunk} | 
      split_chunk (FieldName, Rest, ChunkSize, PartIndex + 1) ]. 

part_name (Name, Part) when is_binary (Name) ->
  list_to_binary (
    part_name (binary_to_list (Name), Part));
part_name (Name, Part) ->
  Name ++ "-" ++ integer_to_list (Part).

to_string (In) when is_list (In) ->
  In;
to_string (In) when is_atom (In) ->
  atom_to_list (In);
to_string (In) when is_integer (In) ->
  integer_to_list (In).

key_in_dict (Key, Dict) when is_list (Key) ->
  dict:is_key (Key, Dict)
    orelse dict:is_key (list_to_binary(Key), Dict)
    orelse dict:is_key (list_to_atom(Key), Dict).

key_in_list (Key, List) when is_list (Key), is_list (List) ->
  proplists:is_defined (Key, List)
   orelse proplists:is_defined (list_to_binary(Key), List)
   orelse proplists:is_defined (list_to_atom(Key), List).

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-endif.
