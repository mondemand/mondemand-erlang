-module (mondemand).

-include_lib ("lwes/include/lwes.hrl").

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour (gen_server).

%% API
-export ( [ start_link/0,
            increment/2,
            increment/3,
            increment/4,
            all/0,
            log_to_mondemand/0,
            send_event/1,
            send_stats/3]).

%% gen_server callbacks
-export ( [ init/1,
            handle_call/3,
            handle_cast/2,
            handle_info/2,
            terminate/2,
            code_change/3]).

-record (state, {host, port, table, timer, channel}).
-define (PROG_ID, "prog_id").
-define (TABLE, mondemand_stats).

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
  gen_server:cast (?MODULE, {increment, ProgId, Key, 1, []}).

increment (ProgId, Key, Amount) when is_integer (Amount) ->
  gen_server:cast (?MODULE, {increment, ProgId, Key, Amount, []}).

increment (ProgId, Key, Amount, Context) when is_integer (Amount) ->
  gen_server:cast (?MODULE, {increment, ProgId, Key, Amount, Context}).

all () ->
  ets:tab2list (?TABLE).
%  case ets:info (?TABLE, size) of
%    undefined -> [];
%    0 -> [];
%    _ -> 
%  end.

send_event (Event) ->
  gen_server:cast (?MODULE, {send, Event}).

send_stats (ProgId, Context, Stats) ->
  ContextNum = length (Context),
  StatsNum   = length (Stats),
  Event =
    #lwes_event {
      name  = "MonDemand::StatsMsg",
      attrs = lists:flatten (
                [ { ?LWES_STRING, "prog_id", ProgId },
                  { ?LWES_U_INT_16, "num", StatsNum },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                        [ {?LWES_STRING,
                            "k"++integer_to_list(I), to_string (K)},
                          {?LWES_INT_64,
                            "v"++integer_to_list(I), V}
                        ]
                    end, lists:seq (0, StatsNum - 1), Stats),
                  { ?LWES_U_INT_16, "ctxt_num", ContextNum },
                  lists:zipwith (
                    fun (I, {K, V}) ->
                        [ {?LWES_STRING,
                            "ctxt_k"++integer_to_list(I), to_string (K)},
                          {?LWES_STRING,
                            "ctxt_v"++integer_to_list(I), to_string (V)}
                        ]
                    end, lists:seq (0, ContextNum - 1), Context),
                  {?LWES_STRING, "ctxt_k"++integer_to_list (ContextNum),
                    "host" },
                  {?LWES_STRING, "ctxt_v"++integer_to_list (ContextNum),
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
                  [ { K, V } || { { C2, K }, V } <- All, C2 =:= C ])
    end
    || C <- lists:usort ([CK || { { CK, _ }, _ } <- All])
  ],
  ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([]) ->
  {ok, File} = application:get_env (config_file),
  case file:read_file (File) of
    {ok, Bin} ->
      {match, [Host]} = re:run (Bin, "MONDEMAND_ADDR=\"([^\"]+)\"",
                                [{capture, all_but_first, list}]),
      {match, [Port]} = re:run (Bin, "MONDEMAND_PORT=\"([^\"]+)\"",
                                [{capture, all_but_first, list}]),

      Channel = lwes:open (emitter, Host, Port),
      TabId = ets:new (?TABLE, [set, protected, named_table]),
      {ok, TRef} =
        timer:apply_interval (60 * 1000, ?MODULE, log_to_mondemand, []),
      { ok,
        #state{
          host = Host,
          port = Port,
          table = TabId,
          timer = TRef,
          channel = Channel
        }
      };
    {error, Error} ->
      error_logger:error_msg ("Config file ~s not found!~n", [File]),
      {stop, {error, Error}}
  end.

handle_call (_Request, _From, State) ->
  {reply, ok, State}.

handle_cast ({send, Event}, State = #state { channel = Channel }) ->
  lwes:emit (Channel, Event),
  {noreply, State};

handle_cast ({increment, ProgId, Key, Amount, Context},
             State = #state { table = TabId }) ->
  SortedContext = lists:sort ([{?PROG_ID, ProgId} | Context]),

  try ets:update_counter (TabId, {SortedContext, Key}, {2, Amount}) of
    _ -> ok
  catch
    error:badarg -> ets:insert (TabId, { {SortedContext, Key}, Amount })
  end,
  {noreply, State};

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

to_string (In) when is_list (In) ->
  In;
to_string (In) when is_atom (In) ->
  atom_to_list (In);
to_string (In) when is_integer (In) ->
  integer_to_list (In).

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-endif.
