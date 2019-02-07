% This is a parser for mondemand.conf files, in general these files are a
% shell compatible set of key/value pairs, an example of which might be
%
% MONDEMAND_ADDR="127.0.0.1"
% # MONDEMAND_PORT="33333"
% MONDEMAND_PORT="11211"
% MONDEMAND_TTL="25"
%
% or
%
% MONDEMAND_ADDR="239.5.1.1"
% MONDEMAND_PORT="10201"
% MONDEMAND_TTL="25"
%
% MONDEMAND_PERF_ADDR="10.5.27.41,10.5.30.40"
% MONDEMAND_PERF_PORT="11211,11211"
% MONDEMAND_PERF_SENDTO="2"
%
% In order for this to be a bit more flexible, quotations are optional in
% the parser although historically this has not been the case so when in
% doubt include the quotes around the values.  Also the comment character
% '#' is also new for this parser, so it may be best to avoid commenting
% in the file itself.

Terminals string ip integer eq quote separator eof.
Nonterminals environment keyval val vallist.
Rootsymbol environment.

environment -> eof : [].
environment -> keyval : [ '$1' ].
environment -> keyval environment : [ '$1' | '$2' ].

keyval -> string eq vallist : { get_value('$1'), '$3' }.
keyval -> string eq quote vallist quote : {get_value('$1'), '$4' }.

vallist -> val : [ '$1' ].
vallist -> val separator vallist : [ '$1' | '$3' ].

val -> ip : get_value('$1').
val -> integer : get_value('$1').

Erlang code.

% convert values to their base types
get_value({string,_,Value}) -> Value;
get_value({integer,_,Value}) -> list_to_integer(Value);
get_value({ip,_,Value}) -> Value.
