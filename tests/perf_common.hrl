-ifndef(_perf_common_included).
-define(_perf_common_included, yup).

-include ("mondemand_internal.hrl").
-include_lib ("lwes/include/lwes.hrl").

-export([compare_all/2,
         load_test_cases/2,
         process_journal/2,
         process_journal/3,
         run_procs/2,
         run_procs/3,
         run_procs/4,
         spawn_all/3,
         run_tests/2
        ]).

% code for comparing some or all entries in a journal
compare_all (N, Journal) ->
  process_journal (Journal, N, fun compare/1).

% code for loading events (aka test cases) from a journal
load_test_cases (N, Journal) ->
  process_journal (Journal, N).

process_journal (File, N) ->
  process_journal (File, N, undefined, []).

process_journal (File, N, CB) when is_function (CB, 1) ->
  process_journal (File, N, CB, 0).

process_journal (File, N, CB, A) ->
  {ok, Dev} = file:open (File, [read, raw, compressed, binary]),
  case read_next (Dev, N, CB, A) of
    {ok, A} -> A;
    E -> E
  end.

read_next (_, 0, CB, A) when is_function (CB, 1) ->
  {ok, A};
read_next (_, 0, _, A) ->
  {ok, lists:reverse(A)};
read_next (Dev, N, CB, A) when is_integer(N) orelse N =:= undefined ->
  case file:read (Dev, 22) of
    {ok, <<S:16/integer-unsigned-big,  % 2 length
           M:64/integer-unsigned-big,  % 8 receipttime
           V4:8/integer-unsigned-big,   % 1 ip
           V3:8/integer-unsigned-big,   % 1 ip
           V2:8/integer-unsigned-big,   % 1 ip
           V1:8/integer-unsigned-big,   % 1 ip
           P:16/integer-unsigned-big,   % 2 port
           _:16/integer-signed-big,    % 2 id
           0:32/integer-signed-big     % 4
         >> } ->
     case file:read (Dev, S) of
       {ok, B} ->
         UDP = {udp, M, {V1, V2, V3, V4}, P, B},
         case CB of
           undefined ->
             read_next (Dev,
                        case N of undefined -> undefined; _ -> N - 1 end,
                        CB,
                        [ UDP | A]);
           F when is_function (F, 1) ->
             CB (UDP),
             read_next (Dev,
                        case N of undefined -> undefined; _ -> N - 1 end,
                        CB,
                        A + 1)
         end;
       eof ->
         read_next (Dev, 0, CB, A);
       E -> E
     end;
    eof ->
      read_next (Dev, 0, CB, A);
    E -> E
  end.

% code for running testcases
run_procs (N, Journal) ->
  run_procs (N, Journal, false).

run_procs (N, Journal, FlameFile) ->
  Funcs = functions (),
  run_procs (N, Journal, Funcs, FlameFile).

run_procs (N, Journal, Funcs, FlameFile) ->
  Parent = self(),
  {ok, Tests} = load_test_cases (N, Journal),
  flame_all (FlameFile, Parent, Tests, Funcs),
  Res = receive_all (Funcs,[]),
  io:format ("~p~n",[Res]).

flame_all (WithFlames, Parent, Tests, Funcs) ->
  case WithFlames of
    L when is_list(L) ->
      spawn (fun() ->
               io:format ("Tracing started...\n",[]),
               eflame2:write_trace (global_and_local_calls_plus_new_procs,
                                    L,
                                    new,
                                    ?MODULE,
                                    spawn_all,
                                    [Parent, Tests, Funcs]),
               io:format ("Tracing stopped...\n", [])
             end);
    false ->
      spawn_all (Parent, Tests, Funcs)
  end.

run_tests ([], _) ->
  ok;
run_tests ([H|R], Fun) ->
  Fun(H),
  run_tests (R, Fun).

spawn_all (Parent, Tests, Funcs) ->
  [
    begin
      spawn (fun() ->
            {Time, _} = timer:tc (?MODULE, run_tests, [Tests,Fun]),
            Parent ! {Label, Time, self(), process_info(self(), reductions)}
        end)
    end
    || {Label, Fun}
    <- Funcs
  ].

receive_all ([],A) ->
  A;
receive_all ([_|T],A) ->
  Res = receive R -> R end,
  receive_all (T, [Res | A]).




-endif.
