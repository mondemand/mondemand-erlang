-module (mondemand_config).

-export ([ default_max_sample_size/0,
           default_stats/0,
           lwes_config/0
         ]).

-define (DEFAULT_MAX_SAMPLE_SIZE, 10).
-define (DEFAULT_STATS, [min, max, sum, count]).

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
