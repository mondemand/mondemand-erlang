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
           host/0,
           default_max_sample_size/0,
           default_stats/0,
           lwes_config/0,
           minutes_to_keep/0,
           max_metrics/0,
           parse_config/1
         ]).

-define (DEFAULT_MAX_SAMPLE_SIZE, 10).
-define (DEFAULT_STATS, [min, max, sum, count]).
-define (DEFAULT_MINUTES_TO_KEEP, 10).
-define (MOCHI_SENDER_HOST, mondemand_sender_host_global).
-define (MOCHI_MAX_METRICS, mondemand_max_metrics_global).
-define (DEFAULT_MAX_METRICS, 512).

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

lwes_config () ->
  % lwes config can be specified in one of two ways
  %   1. the lwes_channel application variable
  %   2. from the file referenced by the config_file application variable
  % they are checked in that order
  case application:get_env (mondemand, lwes_channel) of
    {ok, {H,P}} when is_list (H), is_integer (P) ->
      % allow old config style to continue to work
      [ {T, {1,[{H,P}]}} || T <- [ stats, perf, log, trace, annotation ] ];
    {ok, {N, L}} when is_integer (N), is_list (L) ->
      % allow old config style to continue to work
      [ {T, {N, L} } || T <- [ stats, perf, log, trace, annotation ] ];
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
          parse_config (File);
        undefined ->
          {error, no_lwes_channel_configured}
      end
  end.

valid_config (Config) when is_list (Config) ->
  lists:foldl (fun (T, A) ->
                 lists:keymember (T, 1, Config) andalso A
               end,
               true,
               [stats, perf, log, trace, annotation]).


parse_config (File) ->
  case read_file (File) of
    {error, E} -> {error, E};
    Bin ->
      case parse_star_config (Bin) of
        {error, E } -> {error, E};
        Config ->
          lists:foldl (
            fun (T,A) ->
                [ {T, case parse_type_config (Bin, T) of
                        {error, _} -> Config;
                        C -> C
                      end } | A ]
            end,
            [],
            [ stats, perf, log, trace, annotation])
      end
  end.

parse_star_config (File) ->
  parse_prefix_config ("MONDEMAND_", File).

parse_type_config (File, perf) ->
  parse_prefix_config ("MONDEMAND_PERF_", File);
parse_type_config (File, stats) ->
  parse_prefix_config ("MONDEMAND_STATS_", File);
parse_type_config (File, log) ->
  parse_prefix_config ("MONDEMAND_LOG_", File);
parse_type_config (File, trace) ->
  parse_prefix_config ("MONDEMAND_TRACE_", File);
parse_type_config (File, annotation) ->
  parse_prefix_config ("MONDEMAND_ANNOTATION_", File).


parse_value (Prefix, Suffix, Line) ->
  parse_value (Prefix, Suffix, Line, undefined).

parse_value (Prefix, Suffix, Line, Default) ->
  case re:run (Line, Prefix ++ Suffix ++ "=\"([^\"]+)\"",
               [{capture, all_but_first, list}]) of
    nomatch -> Default;
    {match, [H]} ->
      case string:tokens (H, " ,") of
        [] -> Default;
        H1 -> H1
      end
  end.

coerce (undefined, _) -> undefined;
coerce (L = [[_|_]|_], T) ->
  [ case T of
      list -> E;
      integer -> list_to_integer (E)
    end || E <- L
  ].

single (undefined) -> undefined;
single ([H]) -> H.

read_file (File) ->
  case file:read_file (File) of
    {ok, Bin} -> Bin;
    E -> E
  end.

parse_prefix_config (Prefix, Bin) ->
  HostOrHosts = coerce (parse_value (Prefix, "ADDR", Bin), list),
  PortOrPorts = coerce (parse_value (Prefix, "PORT", Bin), integer),
  TTLOrTTLs = coerce (parse_value (Prefix, "TTL", Bin), integer),
  MaybeSendTo =
    single (coerce (parse_value (Prefix, "SENDTO", Bin), integer)),
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
  build (RestHosts, [Port], [TTL], [ {Host, Port, TTL} | A ]);
build ([Host|RestHosts], [Port], [TTL|RestTTLs], A) ->
  build (RestHosts, [Port], RestTTLs, [ {Host, Port, TTL} | A ]);
build ([Host|RestHosts], [Port|RestPorts], undefined, A) ->
  build (RestHosts, RestPorts, undefined, [ {Host, Port} | A ]);
build ([Host|RestHosts], [Port|RestPorts], [TTL], A) ->
  build (RestHosts, RestPorts, [TTL], [ {Host, Port, TTL} | A ]);
build ([Host|RestHosts], [Port|RestPorts], [TTL|RestTTLs], A) ->
  build (RestHosts, RestPorts, RestTTLs, [ {Host, Port, TTL} | A ]).

minutes_to_keep () ->
  case application:get_env (mondemand, minutes_to_keep) of
    undefined -> ?DEFAULT_MINUTES_TO_KEEP;
    {ok, I} when is_integer (I) -> I
  end.

max_metrics () ->
 mondemand_global:get (?MOCHI_MAX_METRICS).

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
      ?_assertEqual ([{annotation,{1,[{"localhost",20602}]}},
                      {trace,{1,[{"localhost",20602}]}},
                      {log,{1,[{"localhost",20602}]}},
                      {perf,{1,[{"localhost",20602}]}},
                      {stats,{1,[{"localhost",20602}]}}],
                     parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end,
      fun () -> ok = WriteConfig ("localhost", 20602, 3) end,
      ?_assertEqual ([{annotation,{1,[{"localhost",20602,3}]}},
                      {trace,{1,[{"localhost",20602,3}]}},
                      {log,{1,[{"localhost",20602,3}]}},
                      {perf,{1,[{"localhost",20602,3}]}},
                      {stats,{1, [{"localhost",20602,3}]}}],
                      parse_config (ConfigFile)),
      fun () -> ok = DeleteConfig () end
    ]
  }.

%-endif.
