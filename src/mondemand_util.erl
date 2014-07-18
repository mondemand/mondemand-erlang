-module (mondemand_util).

-include ("mondemand_internal.hrl").

-compile({parse_transform, ct_expand}).

-export ([ stringify/1,
           key_in_dict/2,
           key_in_list/2,
           metric_name_key/1,
           metric_value_key/1,
           metric_type_key/1,
           context_name_key/1,
           context_value_key/1
         ]).

stringify (I) when is_integer (I) ->
  integer_to_list (I);
stringify (F) when is_float (F) ->
  float_to_list (F);
stringify (A) when is_atom (A) ->
  atom_to_list (A);
stringify (L) ->
  L.

key_in_dict (Key, Dict) when is_list (Key) ->
  dict:is_key (Key, Dict)
    orelse dict:is_key (list_to_binary(Key), Dict)
    orelse dict:is_key (list_to_atom(Key), Dict).

key_in_list (Key, List) when is_list (Key), is_list (List) ->
  proplists:is_defined (Key, List)
   orelse proplists:is_defined (list_to_binary(Key), List)
   orelse proplists:is_defined (list_to_atom(Key), List).

% generate lookup tables for lwes keys so save some time in production
-define (ELEMENT_OF_TUPLE_LIST(N,Prefix),
         element (N,
                  ct_expand:term (
                    begin
                      list_to_tuple (
                        [
                          list_to_binary (
                            lists:concat ([Prefix, integer_to_list(E-1)])
                          )
                          || E <- lists:seq(1,1024)
                        ]
                      )
                    end))).

metric_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_K).

metric_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_V).

metric_type_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?STATS_T).

context_name_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_K).

context_value_key (N) ->
  ?ELEMENT_OF_TUPLE_LIST (N, ?CTXT_V).
