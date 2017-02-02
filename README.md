# Overview

This is an Erlang application which allows metrics to be sent to
[Mondemand](http://www.mondemand.org/).

# Configuration
Mondemand requires the setting of one or more [LWES](http://www.lwes.org)
channels to send stats and other events to.  This is configured as an
application variable.  Either directly as
```
{ lwes_channel, {1, [ {"127.0.0.1", 20602} ] } }
```
or via a config located on the system somewhere
```
{ config_file, "/etc/mondemand/mondemand.conf" }
```

In addition if you want to collect stats from the erlang VM itself you
can enable this with
```
{ vmstats, [ { program_id, erlang_vm } ] }
```
By default the vmstats will include scheduler utilization which can cause
some increased CPU utilization under light loads, so if you wish to disable
that you can as
```
{ vmstats, [ { program_id, erlang_vm }, {disable_scheduler_wall_time, true } ] }
```

# Usage

Mondemand has the concept of two types of metrics

* Counters - values which typically increase until rolling back to 0
* Gauges - instantaneous values such as temperature (or more regularly a reseting counter or non-mondemand held counter).

Clients will typically add calls like
```
mondemand:increment (ProgramName, CounterName, AmountToIncrementBy).
```
for manipulating counters, and
```
mondemand:set (ProgramName, GaugeName, AmountToSetGaugeTo).
```
for setting gauges.

In addition both calls above accept an optional 4th parameter of a list
of Key/Value pairs representing the context of the call.

Mondemand will add the hostname to all metrics sent to the central
mondemand-server.

