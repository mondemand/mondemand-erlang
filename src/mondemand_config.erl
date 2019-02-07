-module (mondemand_config).
%
% There are currently 2 ways to configure mondemand lwes channels
%
% 1. via application variables
% 2. via a config file (the default location is /etc/mondemand/mondemand.conf)
%
% the format of the file is
%
% MONDEMAND_ADDR="127.0.0.1,127.0.0.1"
% MONDEMAND_PORT="12221,12321"
% MONDEMAND_TTL="3"
%
% MONDEMAND_PERF_ADDR="127.0.0.1,127.0.0.1"
% MONDEMAND_PERF_PORT="12221,12321"
% MONDEMAND_PERF_SENDTO="1"
%
% there are currently no comment characters
%
-export ([ init/0,
           clear/0,
           host/0,
           default_max_sample_size/0,
           default_stats/0,
           send_interval/0,
           lwes_config/0,
           minutes_to_keep/0,
           max_metrics/0,
           parse_config/1,
           get_http_config/0,
           vmstats_enabled/0,
           vmstats_prog_id/0,
           vmstats_context/0,
           vmstats_disable_scheduler_wall_time/0,
           vmstats_legacy_workaround/0,
           flush_config/0
         ]).

-define (DEFAULT_SEND_INTERVAL, 60).
-define (DEFAULT_MAX_SAMPLE_SIZE, 10).
-define (DEFAULT_STATS, [min, max, sum, count]).
-define (DEFAULT_MINUTES_TO_KEEP, 10).
-define (DEFAULT_MAX_METRICS, 512).
-define (DEFAULT_SEND_HOST_PORT,{"127.0.0.1", 11211}).

-define (TYPES, [stats, perf, log, trace, annotation]).

-define (MOCHI_SENDER_HOST, mondemand_sender_host_global).
-define (MOCHI_MAX_METRICS, mondemand_max_metrics_global).

% this function is meant to be called before the supervisor and
% pulls all those configs which are mostly static.
init () ->
  Host =
    case application:get_env (mondemand, sender_host) of
      undefined -> net_adm:localhost();
      {ok, H} -> H
    end,
  mondemand_global:put (?MOCHI_SENDER_HOST, Host),
  MaxMetrics =
    case application:get_env (mondemand, max_metrics) of
      undefined -> ?DEFAULT_MAX_METRICS;
      {ok, M} -> M
    end,
  mondemand_global:put (?MOCHI_MAX_METRICS, MaxMetrics).

clear() ->
  mondemand_global:delete(?MOCHI_SENDER_HOST),
  mondemand_global:delete(?MOCHI_MAX_METRICS).

host () ->
  mondemand_global:get (?MOCHI_SENDER_HOST).

default_max_sample_size () ->
  case application:get_env (mondemand, default_max_sample_size) of
    undefined -> ?DEFAULT_MAX_SAMPLE_SIZE;
    {ok, I} when is_integer(I), I > 0 -> I
  end.

default_stats () ->
  case application:get_env (mondemand, default_stats) of
    undefined -> ?DEFAULT_STATS;
    {ok, L} when is_list (L) -> L
  end.

send_interval () ->
  case application:get_env (mondemand, send_interval) of
    {ok, I} when is_integer (I) -> I;
    _ -> ?DEFAULT_SEND_INTERVAL
  end.

lwes_config () ->
  % lwes config can be specified in one of two ways
  %   1. the lwes_channel application variable
  %   2. from the file referenced by the config_file application variable
  % they are checked in that order
  case application:get_env (mondemand, lwes_channel) of
    {ok, {H,P}} when is_list (H), is_integer (P) ->
      % allow old config style to continue to work
      [ {T, {1,[{H,P}]}} || T <- ?TYPES ];
    {ok, {N, L}} when is_integer (N), is_list (L) ->
      % allow old config style to continue to work
      [ {T, {N, L} } || T <- ?TYPES ];
    {ok, Config} when is_list (Config) ->
      case valid_config (Config) of
        false ->
          {error, invalid_config}; % new style is to just pass anything through
        true ->
          Config
      end;
    undefined ->
      case application:get_env (mondemand, config_file) of
        {ok, File} ->
          case parse_config (File) of
            {error, enoent} ->
              error_logger:warning_msg (
                "Config File ~p missing, using default of ~p~n",
                [File, ?DEFAULT_SEND_HOST_PORT]),
              [ {T, {1,[?DEFAULT_SEND_HOST_PORT]}} || T <- ?TYPES ];
            FinalConfig ->
              FinalConfig
          end;
        undefined ->
          error_logger:warning_msg (
            "No lwes channel configured, using default of ~p~n",
            [?DEFAULT_SEND_HOST_PORT]),
          [ {T, {1,[?DEFAULT_SEND_HOST_PORT]}} || T <- ?TYPES ]
      end
  end.

valid_config (Config) when is_list (Config) ->
  lists:foldl (fun (T, A) ->
                 lists:keymember (T, 1, Config) andalso A
               end,
               true,
               ?TYPES).

parse_config (File) ->
  % read and parse the file
  case read_and_parse_file (File) of
    {error, E} -> {error, E};
    Config ->
      % get the default config by using the default prefix
      case find_prefix_config (type_to_prefix(default), Config) of
        {error, E } -> {error, E};
        DefaultConfig ->
          % then check for overrides by type
          lists:foldl (
            fun (T,A) ->
                [ {T, case find_prefix_config (type_to_prefix(T), Config) of
                        {error, _} -> DefaultConfig;
                        C -> C
                      end } | A ]
            end,
            [],
            ?TYPES)
      end
  end.

type_to_prefix (default) -> "MONDEMAND_";
type_to_prefix (perf) -> "MONDEMAND_PERF_";
type_to_prefix (stats) -> "MONDEMAND_STATS_";
type_to_prefix (log) -> "MONDEMAND_LOG_";
type_to_prefix (trace) -> "MONDEMAND_TRACE_";
type_to_prefix (annotation) -> "MONDEMAND_ANNOTATION_".

read_and_parse_file (File) ->
  case file:read_file (File) of
    {ok, Bin} ->
      case mondemand_config_scanner:string(binary_to_list(Bin)) of
        {ok,Tokens,_} ->
          % add in an eof token so there is always one
          case mondemand_config_parser:parse(Tokens++[{eof,100000}]) of
            {ok, Res} -> Res;
            {error, {Line,mondemand_config_parser,_}} ->
              {error, {parse_error, Line}}
          end;
        {error,{Line,mondemand_config_scanner,_},_} ->
          {error, {scan_error, Line}}
      end;
    E -> E
  end.

find_entry (Key, Config) ->
  case lists:keyfind (Key, 1, Config) of
    false -> undefined;
    {_, V} -> V
  end.

find_prefix_config (Prefix, Config) ->
  HostOrHosts = find_entry (Prefix ++ "ADDR", Config),
  PortOrPorts = find_entry (Prefix ++ "PORT", Config),
  TTLOrTTLs = find_entry (Prefix ++ "TTL", Config),
  MaybeSendTo = case find_entry (Prefix ++ "SENDTO", Config) of
                  [I] when is_integer(I) -> I;
                  O -> O  % FIXME: probably should fix in parser
                end,
  combiner (HostOrHosts, PortOrPorts, TTLOrTTLs, MaybeSendTo).

combiner (HostOrHosts, PortOrPorts, TTLOrTTLs, MaybeSendTo) ->
  case is_list (HostOrHosts)
       andalso is_list (PortOrPorts)
       andalso (TTLOrTTLs =:= undefined orelse is_list (TTLOrTTLs))
       andalso (MaybeSendTo =:= undefined orelse is_integer (MaybeSendTo))
  of
    false -> {error, bad_config};
    true ->
      NumHosts = length (HostOrHosts),
      NumPorts = length (PortOrPorts),
      NumTTLs = case TTLOrTTLs =:= undefined of
                  true -> 0;
                  false -> length (TTLOrTTLs)
                end,
      SendTo =
        case MaybeSendTo =:= undefined of
          true -> NumHosts;
          false -> MaybeSendTo
        end,
      % valid configs are 1 host, 1 port, 0 or 1 ttl
      % or a list of hosts, 1 port, 0 or 1 ttl, sendto 1
      % or a list of hosts,
      %    a list of ports (of the same size),
      %    0 or 1 ttl, sendto less than or equal to number of hosts
      % or a list of hosts, a list of ports (of the same size),
      %    and a list of ttls of the same size,
      %    and sendto less than or equal to number of hosts
      case (NumHosts =:= 1
            andalso NumPorts =:= 1
            andalso (NumTTLs =:= 0
                     orelse NumTTLs =:= 1)
            andalso SendTo =:= 1)
        orelse
           (NumHosts > 1
            andalso NumPorts =:= 1
            andalso (NumTTLs =:= 0
                     orelse NumTTLs =:= 1
                     orelse NumTTLs =:= NumHosts)
            andalso SendTo =< NumHosts)
        orelse
           (NumHosts =:= NumPorts
            andalso (NumTTLs =:= 0
                     orelse NumTTLs =:= 1
                     orelse NumTTLs =:= NumHosts)
            andalso SendTo =< NumHosts)
      of
        false -> {error, config_mismatch};
        true ->
          {SendTo, build (HostOrHosts, PortOrPorts, TTLOrTTLs)}
      end
  end.

build (Hosts, Ports, TTLs) ->
  build (Hosts, Ports, TTLs, []).
build ([],_,_,A) ->
  lists:reverse (A);
build ([Host|RestHosts], [Port], undefined, A) ->
  build (RestHosts, [Port], undefined, [ {Host, Port} | A ]);
build ([Host|RestHosts], [Port], [TTL], A) ->
  build (RestHosts, [Port], [TTL], [ {Host, Port, [{ttl,TTL}]} | A ]);
build ([Host|RestHosts], [Port], [TTL|RestTTLs], A) ->
  build (RestHosts, [Port], RestTTLs, [ {Host, Port, [{ttl,TTL}]} | A ]);
build ([Host|RestHosts], [Port|RestPorts], undefined, A) ->
  build (RestHosts, RestPorts, undefined, [ {Host, Port} | A ]);
build ([Host|RestHosts], [Port|RestPorts], [TTL], A) ->
  build (RestHosts, RestPorts, [TTL], [ {Host, Port, [{ttl,TTL}]} | A ]);
build ([Host|RestHosts], [Port|RestPorts], [TTL|RestTTLs], A) ->
  build (RestHosts, RestPorts, RestTTLs, [ {Host, Port, [{ttl,TTL}]} | A ]).

minutes_to_keep () ->
  case application:get_env (mondemand, minutes_to_keep) of
    undefined -> ?DEFAULT_MINUTES_TO_KEEP;
    {ok, I} when is_integer (I) -> I
  end.

max_metrics () ->
 mondemand_global:get (?MOCHI_MAX_METRICS).

get_http_config () ->
  case application:get_env (mondemand, http_endpoint) of
    {ok, HttpConfig} ->
      HttpConfig;
    undefined ->
      case application:get_env (mondemand, config_file) of
        {ok, File} ->
          case file:read_file (File) of
            {ok, Bin} ->
              case re:run (Bin,
                           "MONDEMAND_TRACE_HTTP_ENDPOINT=\"([^\"]+)\"",
                           [{capture, all_but_first, list}]) of
                {match, [TraceEndPoint]} ->
                  [{trace, TraceEndPoint}];
                _ ->
                  {error, no_http_configured}
              end;
            E ->
              E
          end;
        undefined ->
          {error, no_http_configured}
      end
  end.

vmstats_enabled () ->
  case application:get_env (mondemand, vmstats) of
    {ok, L} when is_list (L) ->
      case proplists:get_value (program_id, L) of
        undefined -> false;
        _ -> true
      end;
    _ ->
      false
  end.

vmstats_prog_id () ->
  vmstats_config (program_id, undefined).
vmstats_context () ->
  vmstats_config (context, []).
vmstats_disable_scheduler_wall_time () ->
  vmstats_config (disable_scheduler_wall_time, false).

vmstats_legacy_workaround () ->
  case application:get_env(mondemand,r15b_workaround) of
    {ok, true} -> true;
    _ -> false
  end.

vmstats_config (K, Default) ->
  case application:get_env (mondemand, vmstats) of
    {ok, L} when is_list (L) ->
      case proplists:get_value (K, L) of
        undefined -> Default;
        V -> V
      end;
    _ -> Default
  end.

flush_config () ->
  case application:get_env (mondemand, flush_config) of
    {ok, {Module, FlushStatePrepFunction, FlushFunction}} ->
      {Module, FlushStatePrepFunction, FlushFunction};
    undefined ->
      {mondemand, flush_state_init, flush_one_stat}
  end.

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef (TEST).
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
      fun () -> ok = WriteConfig ("127.0.0.1", 20602, undefined) end,
      ?_assertEqual ([{annotation,{1,[{"127.0.0.1",20602}]}},
                      {trace,{1,[{"127.0.0.1",20602}]}},
                      {log,{1,[{"127.0.0.1",20602}]}},
                      {perf,{1,[{"127.0.0.1",20602}]}},
                      {stats,{1,[{"127.0.0.1",20602}]}}],
                     parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end,
      fun () -> ok = WriteConfig ("127.0.0.1", 20602, 3) end,
      ?_assertEqual ([{annotation,{1,[{"127.0.0.1",20602,[{ttl,3}]}]}},
                      {trace,{1,[{"127.0.0.1",20602,[{ttl,3}]}]}},
                      {log,{1,[{"127.0.0.1",20602,[{ttl,3}]}]}},
                      {perf,{1,[{"127.0.0.1",20602,[{ttl,3}]}]}},
                      {stats,{1, [{"127.0.0.1",20602,[{ttl,3}]}]}}],
                      parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end
    ]
  }.

canned_config_file_test_ () ->
  [
    ?_assertEqual(Expected, parse_config(ConfigFile))
    || {Expected, ConfigFile}
    <- [
         { [{annotation,{1,[{"239.5.1.1",10201,[{ttl,25}]}]}},
            {trace,{1,[{"239.5.1.1",10201,[{ttl,25}]}]}},
            {log,{1,[{"239.5.1.1",10201,[{ttl,25}]}]}},
            {perf,{2,[{"10.5.27.41",11211},{"10.5.30.40",11211}]}},
            {stats,{1,[{"239.5.1.1",10201,[{ttl,25}]}]}}],
           "tests/mondemand1.conf" },
         { [{annotation,{1,[{"127.0.0.1",11211,[{ttl,25}]}]}},
            {trace,{1,[{"127.0.0.1",11211,[{ttl,25}]}]}},
            {log,{1,[{"127.0.0.1",11211,[{ttl,25}]}]}},
            {perf,{1,[{"127.0.0.1",11211,[{ttl,25}]}]}},
            {stats,{1,[{"127.0.0.1",11211,[{ttl,25}]}]}}],
           "tests/mondemand2.conf" },
         {{error,bad_config}, "tests/mondemand3.conf"},
         { [{annotation,{1,[{"239.1.1.1",10201,[{ttl,25}]}]}},
            {trace,{1,[{"239.1.1.1",10201,[{ttl,25}]}]}},
            {log,{1,[{"239.1.1.1",10201,[{ttl,25}]}]}},
            {perf,{2,[{"10.5.27.41",11211},{"10.5.30.40",11211}]}},
            {stats,{1,[{"239.1.1.1",10201,[{ttl,25}]}]}}],
           "tests/mondemand4.conf" },
         { [{annotation,{1,[{"10.5.42.19",20402,[{ttl,25}]}]}},
            {trace,{1,[{"10.5.42.19",20402,[{ttl,25}]}]}},
            {log,{1,[{"10.5.42.19",20402,[{ttl,25}]}]}},
            {perf,{1,[{"10.5.42.19",20402,[{ttl,25}]}]}},
            {stats,{1,[{"10.5.42.19",20402,[{ttl,25}]}]}}],
           "tests/mondemand5.conf" },
         {{error,{scan_error,5}}, "tests/mondemand6.conf"},
         {{error,bad_config}, "tests/mondemand7.conf"},
         {[{annotation,{1,[{"127.0.0.1",20602}]}},
           {trace,{1,[{"127.0.0.1",20602}]}},
           {log,{1,[{"127.0.0.1",20602}]}},
           {perf,{1,[{"127.0.0.1",20602}]}},
           {stats,{1,[{"127.0.0.1",20602}]}}], "tests/mondemand8.conf"}
       ]
  ].

-endif.
