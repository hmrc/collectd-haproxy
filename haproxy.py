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

DEFAULT_METRICS = {
    'ConnRate': ('connection_rate', 'gauge'),
    'CumReq': ('requests', 'derive'),
    'Idle_pct': ('idle_pct', 'gauge'),
    'scur': ('session_current', 'gauge'),
    'SessRate': ('session_rate_all', 'gauge'),
    'lbtot': ('server_selected_total', 'counter'),
    'bout': ('bytes_out', 'derive'),
    'bin': ('bytes_in', 'derive'),
    'ttime': ('session_time_avg', 'gauge'),
    'req_rate': ('request_rate', 'gauge'),
    'rate': ('session_rate', 'gauge'),
    'hrsp_2xx': ('response_2xx', 'derive'),
    'hrsp_4xx': ('response_4xx', 'derive'),
    'hrsp_5xx': ('response_5xx', 'derive'),
    'ereq': ('error_request', 'derive'),
    'dreq': ('denied_request', 'derive'),
    'econ': ('error_connection', 'derive'),
    'dresp': ('denied_response', 'derive'),
    'qcur': ('queue_current', 'gauge'),
    'qtime': ('queue_time_avg', 'gauge'),
    'rtime': ('response_time_avg', 'gauge'),
    'eresp': ('error_response', 'derive'),
    'wretr': ('retries', 'derive'),
    'wredis': ('redispatched', 'derive'),
}

ENHANCED_METRICS = {
    # Metrics that are collected for the whole haproxy instance.
    # The format is  haproxy_metricname : {'signalfx_corresponding_metric': 'collectd_type'}
    # Currently signalfx_corresponding_metric match haproxy_metricname
    # Correspond to 'show info' socket command
    'MaxConn': ('max_connections', 'gauge'),
    'CumConns': ('connections', 'derive'),
    'MaxConnRate': ('max_connection_rate', 'gauge'),
    'MaxSessRate': ('max_session_rate', 'gauge'),
    'MaxSslConns': ('max_ssl_connections', 'gauge'),
    'CumSslConns': ('ssl_connections', 'derive'),
    'MaxPipes': ('max_pipes', 'gauge'),
    'Tasks': ('tasks', 'gauge'),
    'Run_queue': ('run_queue', 'gauge'),
    'PipesUsed': ('pipes_used', 'gauge'),
    'PipesFree': ('pipes_free', 'gauge'),
    'Uptime_sec': ('uptime_seconds', 'derive'),
    'CurrConns': ('current_connections', 'gauge'),
    'CurrSslConns': ('current_ssl_connections', 'gauge'),
    'SslRate': ('ssl_rate', 'gauge'),
    'SslFrontendKeyRate': ('ssl_frontend_key_rate', 'gauge'),
    'SslBackendKeyRate': ('ssl_backend_key_rate', 'gauge'),
    'SslCacheLookups': ('ssl_cache_lookups', 'derive'),
    'SslCacheMisses': ('ssl_cache_misses', 'derive'),
    'CompressBpsIn': ('compress_bps_in', 'derive'),
    'CompressBpsOut': ('compress_bps_out', 'derive'),
    'ZlibMemUsage': ('zlib_mem_usage', 'gauge'),

    # Metrics that are collected per each proxy separately.
    # Proxy name would be the dimension as well as service_name
    # Correspond to 'show stats' socket command
    'chkfail': ('failed_checks', 'derive'),
    'downtime': ('downtime', 'derive'),
    'hrsp_1xx': ('response_1xx', 'derive'),
    'hrsp_3xx': ('response_3xx', 'derive'),
    'hrsp_other': ('response_other', 'derive'),
    'qmax': ('queue_max', 'gauge'),
    'qlimit': ('queue_limit', 'gauge'),
    'rate_lim': ('session_rate_limit', 'gauge'),
    'rate_max': ('session_rate_max', 'gauge'),
    'req_rate_max': ('request_rate_max', 'gauge'),
    'stot': ('session_total', 'derive'),
    'slim': ('session_limit', 'gauge'),
    'smax': ('session_max', 'gauge'),
    'throttle': ('throttle', 'gauge'),
    'cli_abrt': ('cli_abrt', 'derive'),
    'srv_abrt': ('srv_abrt', 'derive'),
    'comp_in': ('comp_in', 'derive'),
    'comp_out': ('comp_out', 'derive'),
    'comp_byp': ('comp_byp', 'derive'),
    'comp_rsp': ('comp_rsp', 'derive'),
    'ctime': ('connect_time_avg', 'gauge'),
    'act': ('active_servers', 'gauge'),
    'bck': ('backup_servers', 'gauge'),
    'check_duration': ('health_check_duration', 'gauge'),
    'lastsess': ('last_session', 'gauge'),
    'conn_rate': ('conn_rate', 'gauge'),
    'conn_rate_max': ('conn_rate_max', 'gauge'),
    'conn_tot': ('conn_total', 'counter'),
    'intercepted': ('intercepted', 'gauge'),
    'dcon': ('denied_tcp_conn', 'gauge'),
    'dses': ('denied_tcp_sess', 'gauge'),
}

DIMENSIONS_LIST = [
    'pxname',
    'svname',
    'pid',
    'sid',
    'iid',
    'type',
    'addr',
    'cookie',
    'mode',
    'algo',
]

DEFAULT_METRICS = dict((k.lower(), v) for k, v in DEFAULT_METRICS.items())
ENHANCED_METRICS = dict((k.lower(), v) for k, v in ENHANCED_METRICS.items())
METRIC_DELIM = '.'  # for the frontend/backend stats

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
        dimensions = _build_dimension_dict(statdict)
        if not (('svname' in statdict and statdict['svname'].lower() in module_config['proxy_monitors']) or
                ('pxname' in statdict and statdict['pxname'].lower() in module_config['proxy_monitors'])):
            continue
        for metricname, val in statdict.items():
            try:
                stats.append((metricname, int(val), dimensions))
            except (TypeError, ValueError):
                pass

    return stats


def _build_dimension_dict(statdict):
    """
    Builds dimensions dict to send back with metrics with readable metric names
    Args:
    statdict dictionary of metrics from HAProxy to be filtered for dimensions
    """

    dimensions = {}

    for key in DIMENSIONS_LIST:
        if key in statdict and key == 'pxname':
            dimensions['proxy_name'] = statdict['pxname']
        elif key in statdict and key == 'svname':
            dimensions['service_name'] = statdict['svname']
        elif key in statdict and key == 'pid':
            dimensions['process_id'] = statdict['pid']
        elif key in statdict and key == 'sid':
            dimensions['server_id'] = statdict['sid']
        elif key in statdict and key == 'iid':
            dimensions['unique_proxy_id'] = statdict['iid']
        elif key in statdict and key == 'type':
            dimensions['type'] = _get_proxy_type(statdict['type'])
        elif key in statdict and key == 'addr':
            dimensions['address'] = statdict['addr']
        elif key in statdict and key == 'algo':
            dimensions['algorithm'] = statdict['algo']
        elif key in statdict:
            dimensions[key] = statdict[key]

    return dimensions


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
        elif node.key == "EnhancedMetrics" and node.values[0]:
            enhanced_metrics = _str_to_bool(node.values[0])
        elif node.key == "ExcludeMetric" and node.values[0]:
            excluded_metrics.add(node.values[0])
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


def _format_dimensions(dimensions):
    """
    Formats a dictionary of dimensions to a format that enables them to be
    specified as key, value pairs in plugin_instance to signalfx. E.g.
    >>> dimensions = {'a': 'foo', 'b': 'bar'}
    >>> _format_dimensions(dimensions)
    "[a=foo,b=bar]"
    Args:
    dimensions (dict): Mapping of {dimension_name: value, ...}
    Returns:
    str: Comma-separated list of dimensions
    """

    dim_pairs = ["%s=%s" % (k, v) for k, v in dimensions.iteritems()]
    return "[%s]" % (",".join(dim_pairs))


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
        if not metric_name.lower() in DEFAULT_METRICS and not metric_name.lower() in ENHANCED_METRICS:
            collectd.debug("metric %s is not in either metric list" % metric_name.lower())
            continue

        # skip metrics in enhanced metrics mode if not enabled
        if not module_config['enhanced_metrics'] and metric_name.lower() in ENHANCED_METRICS:
            continue

        # pull metric name & type from respective metrics list
        if metric_name.lower() in DEFAULT_METRICS:
            translated_metric_name, val_type = DEFAULT_METRICS[metric_name.lower()]
        else:
            translated_metric_name, val_type = ENHANCED_METRICS[metric_name.lower()]

        # skip over any exlcluded metrics
        if translated_metric_name in module_config['excluded_metrics']:
            collectd.debug("excluding metric %s" % translated_metric_name)
            continue

        # create datapoint and dispatch
        datapoint = collectd.Values()
        datapoint.type = val_type
        datapoint.type_instance = translated_metric_name
        datapoint.plugin = PLUGIN_NAME
        dimensions.update(module_config['custom_dimensions'])
        if len(dimensions) > 0:
            datapoint.plugin_instance = _format_dimensions(dimensions)
        datapoint.values = (metric_value,)
        pprint_dict = {
                    'plugin': datapoint.plugin,
                    'plugin_instance': datapoint.plugin_instance,
                    'type': datapoint.type,
                    'type_instance': datapoint.type_instance,
                    'values': datapoint.values
                }
        collectd.debug(pprint.pformat(pprint_dict))
        datapoint.dispatch()

collectd.register_config(config)
