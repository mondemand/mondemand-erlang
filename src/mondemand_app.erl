-module (mondemand_app).

-behaviour (application).

%% API
-export([start/0]).

%% Application callbacks
-export([start/2, stop/1]).

%-=====================================================================-
%-                                  API                                -
%-=====================================================================-
start() ->
  application:ensure_all_started (mondemand).

%-=====================================================================-
%-                        application callbacks                        -
%-=====================================================================-
start(_Type, _StartArgs) ->
  mondemand_sup:start_link().

stop(_State) ->
  ok.

%-=====================================================================-
%-                            Test Functions                           -
%-=====================================================================-
-ifdef (TEST).
-include_lib ("eunit/include/eunit.hrl").

mondemand_app_test_ () ->
  [
    fun() ->
      {R, _} = mondemand_app:start(),
      ?assertEqual (ok, R)
    end,
    ?_assertEqual ({ok, []},mondemand_app:start()),
    ?_assertEqual (ok, application:stop (mondemand)),
    fun() ->
      mondemand_config:clear()
    end
  ].

-endif.
