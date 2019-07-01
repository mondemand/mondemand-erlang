-module(mondemand_httpd).

-export([do/1]).

-include_lib("inets/include/httpd.hrl").

-define(SERVER_NAME, "Mondemand metrics.").

do(#mod { method = Method, request_uri = URI }) ->
  case URI =:= mondemand_config:httpd_metrics_endpoint()
       andalso Method =:= "GET" of
    true ->
      Body = mondemand:export_as_prometheus(),
      ContentLength = integer_to_list(iolist_size(Body)),
      RespHeaders = [{"accept","text/plain"},
                     {code, 200},
                     {content_type, "text/plain"},
                     {content_length,ContentLength}],
      {break,[{response, {response, RespHeaders, [Body]}}]};
    false ->
      {break,[{response, {404,""}}]}
  end.
