all:
	@rebar get-deps compile

perf:
	@(cd tests ; erlc -I../src -I../include *.erl)

edoc:
	@rebar skip_deps=true doc

check:
	@rm -rf .eunit
	@mkdir -p .eunit
	@dialyzer --src src -I deps -pa deps/parse_trans/ebin
	@rebar skip_deps=true eunit

clean:
	@rebar clean

maintainer-clean:
	@rebar clean
	@rebar delete-deps
	@rm -rf deps
