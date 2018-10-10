# haproxy-collectd-plugin - haproxy.py
#
# Author: Michael Leinartas
# Description: This is a collectd plugin which runs under the Python plugin to
# collect metrics from haproxy.
# Plugin structure and logging func taken from
# https://github.com/phrawzty/rabbitmq-collectd-plugin
#
# Modified by "Warren Turkal" <wt@signalfuse.com>, "Volodymyr Zhabiuk" <vzhabiuk@signalfx.com>

import cStringIO as StringIO
import socket
import csv
import pprint

import collectd

PLUGIN_NAME = 'haproxy'
RECV_SIZE = 1024

METRICS_TO_COLLECT = {
    'ConnRate': 'gauge', 'CumReq': 'derive', 'Idle_pct': 'gauge', 'scur': 'gauge', 'SessRate': 'gauge',
    'lbtot': 'counter', 'bout': 'derive', 'bin': 'derive', 'ttime': 'gauge', 'req_rate': 'gauge', 'rate': 'gauge',
    'hrsp_2xx': 'derive', 'hrsp_4xx': 'derive', 'hrsp_5xx': 'derive', 'ereq': 'derive', 'dreq': 'derive',
    'econ': 'derive', 'dresp': 'derive', 'qcur': 'gauge', 'qtime': 'gauge', 'rtime': 'gauge', 'eresp': 'derive',
    'wretr': 'derive', 'wredis': 'derive', 'MaxConn': 'gauge', 'CumConns': 'derive', 'MaxConnRate': 'gauge',
    'MaxSessRate': 'gauge', 'MaxSslConns': 'gauge', 'CumSslConns': 'derive', 'MaxPipes': 'gauge', 'Tasks': 'gauge',
    'Run_queue': 'gauge', 'PipesUsed': 'gauge', 'PipesFree': 'gauge', 'Uptime_sec': 'derive', 'CurrConns': 'gauge',
    'CurrSslConns': 'gauge', 'SslRate': 'gauge', 'SslFrontendKeyRate': 'gauge', 'SslBackendKeyRate': 'gauge',
    'SslCacheLookups': 'derive', 'SslCacheMisses': 'derive', 'CompressBpsIn': 'derive', 'CompressBpsOut': 'derive',
    'ZlibMemUsage': 'gauge', 'chkfail': 'derive', 'downtime': 'derive', 'hrsp_1xx': 'derive', 'hrsp_3xx': 'derive',
    'hrsp_other': 'derive', 'qmax': 'gauge', 'qlimit': 'gauge', 'rate_lim': 'gauge', 'rate_max': 'gauge',
    'req_rate_max': 'gauge', 'stot': 'derive', 'slim': 'gauge', 'smax': 'gauge', 'throttle': 'gauge',
    'cli_abrt': 'derive', 'srv_abrt': 'derive', 'comp_in': 'derive', 'comp_out': 'derive', 'comp_byp': 'derive',
    'comp_rsp': 'derive', 'ctime': 'gauge', 'act': 'gauge', 'bck': 'gauge', 'check_duration': 'gauge',
    'lastsess': 'gauge', 'conn_rate': 'gauge', 'conn_rate_max': 'gauge', 'conn_tot': 'counter', 'intercepted': 'gauge',
    'dcon': 'gauge', 'dses': 'gauge', 'sent': 'gauge', 'snd_error': 'gauge', 'valid': 'gauge', 'update': 'gauge',
    'cname': 'gauge', 'cname_error': 'gauge', 'any_err': 'gauge', 'nx': 'gauge', 'timeout': 'gauge', 'refused': 'gauge',
    'other': 'gauge', 'invalid': 'gauge', 'too_big': 'gauge', 'truncated': 'gauge', 'outdated': 'gauge'
}

DEFAULT_SOCKET = '/var/run/haproxy.sock'
DEFAULT_PROXY_MONITORS = ['server', 'frontend', 'backend']


class HAProxySocket(object):
    """
            Encapsulates communication with HAProxy via the socket interface
    """

    def __init__(self, socket_file=DEFAULT_SOCKET):
        self.socket_file = socket_file

    def connect(self):
        # unix sockets all start with '/', use tcp otherwise
        is_unix = self.socket_file.startswith('/')
        if is_unix:
            stat_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            stat_sock.connect(self.socket_file)
            return stat_sock
        else:
            socket_host, separator, port = self.socket_file.rpartition(':')
            if socket_host != '' and port != '' and separator == ':':
                stat_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                stat_sock.connect((socket_host, int(port)))
                return stat_sock
            else:
                collectd.error('Could not connect to socket with host %s. Check HAProxy config.' % self.socket_file)
                return

    def communicate(self, command):
        '''Get response from single command.

        Args:
            command: string command to send to haproxy stat socket

        Returns:
            a string of the response data
        '''
        if not command.endswith('\n'):
            command += '\n'
        stat_sock = self.connect()
        if stat_sock is None:
            return ''
        stat_sock.sendall(command)
        result_buf = StringIO.StringIO()
        buf = stat_sock.recv(RECV_SIZE)
        while buf:
            result_buf.write(buf)
            buf = stat_sock.recv(RECV_SIZE)

        stat_sock.close()
        return result_buf.getvalue()

    # This method isn't nice but there's no other way to parse the output of show resolvers from haproxy
    def get_resolvers(self):
        ''' Gets the resolver config and returns a map of nameserver -> nameservermetrics
        The output from the socket looks like
        Resolvers section mydns
         nameserver dns1:
          sent:        8
          ...

        :return:
        map of nameserver -> nameservermetrics
        e.g. '{dns1': {'sent': '8', ...}, ...}
        '''
        result = {}
        output = self.communicate('show resolvers')
        nameserver = ''
        for line in output.splitlines():
            try:
                if 'Resolvers section' in line or line.strip() == '':
                    continue
                elif 'nameserver' in line:
                    _, unsanitied_nameserver = line.strip().split(' ', 1)
                    nameserver = unsanitied_nameserver[:-1]  # remove trailing ':'
                    result[nameserver] = {}
                else:
                    key, val = line.split(':', 1)
                    current_nameserver_stats = result[nameserver]
                    current_nameserver_stats[key.strip()] = val.strip()
                    result[nameserver] = current_nameserver_stats
            except ValueError:
                continue

        return result

    def get_server_info(self):
        result = {}
        output = self.communicate('show info')
        for line in output.splitlines():
            try:
                key, val = line.split(':', 1)
            except ValueError:
                continue
            result[key.strip()] = val.strip()

        return result

    def get_server_stats(self):
        output = self.communicate('show stat')
        # sanitize and make a list of lines
        output = output.lstrip('# ').strip()
        output = [l.strip(',') for l in output.splitlines()]
        csvreader = csv.DictReader(output)
        result = [d.copy() for d in csvreader]
        return result


def get_stats(module_config):
    """
        Makes two calls to haproxy to fetch server info and server stats.
        Returns the dict containing metric name as the key and a tuple of metric value and the dict of dimensions if any
    """
    if module_config['socket'] is None:
        collectd.error("Socket configuration parameter is undefined. Couldn't get the stats")
        return
    stats = []
    haproxy = HAProxySocket(module_config['socket'])

    try:
        server_info = haproxy.get_server_info()
        server_stats = haproxy.get_server_stats()
        resolver_stats = haproxy.get_resolvers()
    except socket.error:
        collectd.warning('status err Unable to connect to HAProxy socket at %s' % module_config['socket'])
        return stats

    # server wide stats
    for key, val in server_info.iteritems():
        try:
            stats.append((key, int(val), dict()))
        except (TypeError, ValueError):
            pass

    # proxy specific stats
    for statdict in server_stats:
        if not should_capture_metric(statdict, module_config):
            continue
        for metricname, val in statdict.items():
            try:
                stats.append((metricname, int(val), statdict))
            except (TypeError, ValueError):
                pass

    for resolver, resolver_stats in resolver_stats.iteritems():
        for metricname, val in resolver_stats.items():
            try:
                stats.append((metricname, int(val), {'is_resolver': True, 'nameserver': resolver}))
            except (TypeError, ValueError):
                pass
    return stats


def should_capture_metric(statdict, module_config):
    return (('svname' in statdict and statdict['svname'].lower() in module_config['proxy_monitors']) or
            ('pxname' in statdict and statdict['pxname'].lower() in module_config['proxy_monitors']) or
            is_backend_server_metric(statdict) and 'backend' in module_config['proxy_monitors'])


def is_backend_server_metric(statdict):
    return 'type' in statdict and _get_proxy_type(statdict['type']) == 'server'


def is_resolver_metric(statdict):
    return 'is_resolver' in statdict and statdict['is_resolver']


def config(config_values):
    """
    A callback method that  loads information from the HaProxy collectd plugin config file.
    Args:
    config_values (collectd.Config): Object containing config values
    """

    module_config = {}
    socket = DEFAULT_SOCKET
    proxy_monitors = []
    excluded_metrics = set()
    enhanced_metrics = False
    interval = None
    testing = False
    custom_dimensions = {}

    for node in config_values.children:
        if node.key == "ProxyMonitor" and node.values[0]:
            proxy_monitors.extend(node.values)
        elif node.key == "Socket" and node.values[0]:
            socket = node.values[0]
        elif node.key == "Interval" and node.values[0]:
            interval = node.values[0]
        elif node.key == "Testing" and node.values[0]:
            testing = _str_to_bool(node.values[0])
        elif node.key == 'Dimension':
            if len(node.values) == 2:
                custom_dimensions.update({node.values[0]: node.values[1]})
            else:
                collectd.warning("WARNING: Check configuration \
                                            setting for %s" % node.key)
        else:
            collectd.warning('Unknown config key: %s' % node.key)

    if not proxy_monitors:
        proxy_monitors += DEFAULT_PROXY_MONITORS

    module_config = {
        'socket': socket,
        'proxy_monitors': proxy_monitors,
        'interval': interval,
        'enhanced_metrics': enhanced_metrics,
        'excluded_metrics': excluded_metrics,
        'custom_dimensions': custom_dimensions,
        'testing': testing,
    }
    proxys = "_".join(proxy_monitors)

    if testing:
        return module_config

    interval_kwarg = {}
    if interval:
        interval_kwarg['interval'] = interval
    collectd.register_read(collect_metrics, data=module_config,
                           name='node_' + module_config['socket'] + '_' + proxys,
                           **interval_kwarg)


def _format_plugin_instance(dimensions):
    if is_backend_server_metric(dimensions):
        return "{0}.{1}.{2}".format("backend", dimensions['pxname'].lower(), dimensions['svname'])
    elif is_resolver_metric(dimensions):
        return "nameserver.{0}".format(dimensions['nameserver'])
    else:
        return "{0}.{1}".format(dimensions['svname'].lower(), dimensions['pxname'])


def _get_proxy_type(type_id):
    """
        Return human readable proxy type
        Args:
        type_id: 0=frontend, 1=backend, 2=server, 3=socket/listener
    """
    proxy_types = {
        0: 'frontend',
        1: 'backend',
        2: 'server',
        3: 'socket/listener',
    }
    return proxy_types.get(int(type_id))


def _str_to_bool(val):
    '''
    Converts a true/false string to a boolean
    '''
    val = str(val).strip().lower()
    if val == 'true':
        return True
    elif val != 'false':
        collectd.warning('Warning: String (%s) could not be converted to a boolean. Returning false.' % val)

    return False


def submit_metrics(metric_datapoint):
    datapoint = collectd.Values()
    datapoint.type = metric_datapoint['type']
    datapoint.type_instance = metric_datapoint['type_instance']
    datapoint.plugin = metric_datapoint['plugin']
    if 'plugin_instance' in metric_datapoint.keys():
        datapoint.plugin_instance = metric_datapoint['plugin_instance']
    datapoint.values = metric_datapoint['values']
    collectd.debug(pprint.pformat(metric_datapoint))
    datapoint.dispatch()


def collect_metrics(module_config):
    collectd.debug('beginning collect_metrics')
    """
        A callback method that gets metrics from HAProxy and records them to collectd.
    """

    info = get_stats(module_config)

    if not info:
        collectd.warning('%s: No data received' % PLUGIN_NAME)
        return

    for metric_name, metric_value, dimensions in info:
        # assert metric is in valid metrics lists
        if metric_name not in METRICS_TO_COLLECT:
            collectd.debug("metric %s is not in list of metrics to collect" % metric_name.lower())
            continue

        metric_datapoint = {
                    'plugin': PLUGIN_NAME,
                    'type': METRICS_TO_COLLECT[metric_name],
                    'type_instance': metric_name.lower(),
                    'values': (metric_value,)
                }
        if len(dimensions) > 0:
            metric_datapoint['plugin_instance'] = _format_plugin_instance(dimensions)
        collectd.debug(pprint.pformat(metric_datapoint))
        submit_metrics(metric_datapoint)


collectd.register_config(config)
