
# collectd-haproxy

This is a collectd plugin for HAProxy (tested and working as of HAProxy 1.8).

It uses the UNIX Socket commands to monitor stats from the `show info`, `show stat` and `show resolvers` commands. This allows monitoring of haproxy status as well as frontends, backends, servers and resolvers configured.

This plugin was forked and modified from [Signalfx's haproxy plugin](https://github.com/signalfx/collectd-haproxy) in order to output non signalfx specific names and output resolver/nameserver information.
### License

This code is open source software licensed under the [MIT License]("https://opensource.org/licenses/MIT").
