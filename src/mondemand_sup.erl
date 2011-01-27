-module(mondemand_sup).

-behaviour(supervisor).

-ifdef(HAVE_EUNIT).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using.
%% supervisor:start_link/[2,3], this function is called by the new process.
%% to find out about restart strategy, maximum restart frequency and child.
%% specifications.
%%--------------------------------------------------------------------
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

%%====================================================================
%% Internal functions
%%====================================================================

%%--------------------------------------------------------------------
%%% Test functions
%%--------------------------------------------------------------------
-ifdef(EUNIT).

-endif.
