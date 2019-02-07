% this is tokenizer for the mondemand.conf file, it's pretty simple
% as it only distiguishes the current set of tokens which are the keys
% and the values of either positive integers or ip addresses.

Definitions.

% white space
WS = [\000-\s]
EQ = [=]
WORD = ([a-zA-Z][a-zA-Z0-9_\-]*)
IPADDR_LITERAL = ([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\.([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])
POS_INTEGER_LITERAL = [0-9]([0-9])*
SEPARATOR = [\,]
COMMENT = #[^\r\n]*(\r\n|\r|\n)?
QUOTE = [\"\']

Rules.

{WORD} : {token, {string, TokenLine, TokenChars}}.
{IPADDR_LITERAL} : {token, { ip, TokenLine, TokenChars}}.
{POS_INTEGER_LITERAL} : { token, { integer, TokenLine, TokenChars}}.
{EQ} : { token, {eq, TokenLine} }.
{QUOTE} : { token, {quote, TokenLine} }.
{SEPARATOR} : { token, {separator, TokenLine} }.

{COMMENT}|{WS} : skip_token.

. : {error, syntax}.

Erlang code.


