-module(mondemand_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%-=====================================================================-
%-                        supervisor callbacks                         -
%-=====================================================================-
init([]) ->
  VMStatsChild =
    case application:get_env (mondemand, vmstats) of
      {ok, true} ->
        [
          { mondemand_vmstats,
           {mondemand_vmstats, start_link, []},
           permanent,
           2000,
           worker,
           [mondemand_vmstats]
          }
          ];
      _ ->
        []
    end,

  ServiceChildren =
    VMStatsChild ++
    [
      { mondemand_statdb,
        {mondemand_statdb, start_link, []},
        permanent,
        2000,
        worker,
        [mondemand_statdb]
      },
      {
        mondemand,                           % child spec id
        {mondemand, start_link, []},         % child function to call {M,F,A}
        permanent,                           % always restart
        2000,                                % time to wait for child shutdown
        worker,                              % type of child
        [mondemand]                          % modules used by child
      }
    ],
  {ok,{{one_for_one,5,60}, ServiceChildren}}.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-


%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
