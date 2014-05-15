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
    ServiceChild =
      {
        mondemand,                           % child spec id
        {mondemand, start_link, []},         % child function to call {M,F,A}
        permanent,                           % always restart
        2000,                                % time to wait for child shutdown
        worker,                              % type of child
        [mondemand]                          % modules used by child
      },
    {ok,{{one_for_one,5,60}, [ServiceChild]}}.

%-=====================================================================-
%-                               Private                               -
%-=====================================================================-


%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

-endif.
