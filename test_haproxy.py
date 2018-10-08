#!/usr/bin/env python
import collections
from mock import MagicMock
from mock import Mock
from mock import patch
from mock import call
import sys


class MockCollectd(MagicMock):
    """
    Mocks the functions and objects provided by the collectd module
    """

    @staticmethod
    def log(log_str):
        print log_str

    debug = log
    info = log
    warning = log
    error = log

class MockHAProxySocketSimple(object):
    def __init__(self, socket_file="whatever"):
        self.socket_file = socket_file

    def get_server_info(self):
        sample_data = {'ConnRate': '3', 'CumReq': '5', 'idle_pct': '78'}
        return sample_data

    def get_server_stats(self):
        sample_data = [{'bin': '3120628', 'lastchg': '', 'lbt': '', 'weight': '',
                        'wretr': '', 'slim': '50', 'pid': '1', 'wredis': '', 'dresp': '0',
                        'ereq': '0', 'pxname': 'sample_proxy', 'stot': '39728',
                        'sid': '0', 'bout': '188112702395', 'qlimit': '', 'status': 'OPEN',
                        'smax': '2', 'dreq': '0', 'econ': '', 'iid': '2', 'chkfail': '',
                        'downtime': '', 'qcur': '', 'eresp': '', 'throttle': '', 'scur': '0',
                        'bck': '', 'qmax': '', 'act': '', 'chkdown': '', 'svname': 'FRONTEND'}]
        return sample_data

sys.modules['collectd'] = MockCollectd()

import haproxy

ConfigOption = collections.namedtuple('ConfigOption', ('key', 'values'))

mock_config_default_values = Mock()
mock_config_default_values.children = [
    ConfigOption('Testing', ('True',))
]


def test_default_config():
    module_config = haproxy.config(mock_config_default_values)
    assert module_config['socket'] == '/var/run/haproxy.sock'
    assert not module_config['enhanced_metrics']
    assert module_config['proxy_monitors'] == ['server', 'frontend', 'backend']
    assert module_config['testing']
    assert module_config['excluded_metrics'] == set()


mock_config_enhanced_metrics_off = Mock()
mock_config_enhanced_metrics_off.children = [
    ConfigOption('Socket', ('/var/run/haproxy.sock',)),
    ConfigOption('EnhancedMetrics', ('False',)),
    ConfigOption('Testing', ('True',))
]


def test_enhanced_metrics_off_config():
    module_config = haproxy.config(mock_config_enhanced_metrics_off)
    assert module_config['socket'] == '/var/run/haproxy.sock'
    assert not module_config['enhanced_metrics']
    assert module_config['proxy_monitors'] == ['server', 'frontend', 'backend']
    assert module_config['testing']
    assert module_config['excluded_metrics'] == set()


mock_config_enhanced_metrics_on = Mock()
mock_config_enhanced_metrics_on.children = [
    ConfigOption('Socket', ('/var/run/haproxy.sock',)),
    ConfigOption('EnhancedMetrics', ('True',)),
    ConfigOption('Testing', ('True',))
]


def test_enhanced_metrics_on_config():
    module_config = haproxy.config(mock_config_enhanced_metrics_on)
    assert module_config['socket'] == '/var/run/haproxy.sock'
    assert module_config['enhanced_metrics']
    assert module_config['proxy_monitors'] == ['server', 'frontend', 'backend']
    assert module_config['testing']
    assert module_config['excluded_metrics'] == set()

mock_config_exclude_idle_pct = Mock()
mock_config_exclude_idle_pct.children = [
    ConfigOption('Socket', ('/var/run/haproxy.sock',)),
    ConfigOption('EnhancedMetrics', ('False',)),
    ConfigOption('ExcludeMetric', ('idle_pct',)),
    ConfigOption('Testing', ('True',))
]


def test_exclude_metrics_config():
    module_config = haproxy.config(mock_config_exclude_idle_pct)
    assert module_config['socket'] == '/var/run/haproxy.sock'
    assert not module_config['enhanced_metrics']
    assert module_config['proxy_monitors'] == ['server', 'frontend', 'backend']
    assert module_config['testing']
    assert module_config['excluded_metrics'] == set(['idle_pct'])

mock_config = Mock()
mock_config.children = [
    ConfigOption('Testing', ('True',))
]


@patch('haproxy.HAProxySocket', MockHAProxySocketSimple)
def test_read():
    haproxy.collect_metrics(haproxy.config(mock_config))

mock_config_exclude_bytes_out = Mock()
mock_config_exclude_bytes_out.children = [
    ConfigOption('ExcludeMetric', ('bytes_out',)),
    ConfigOption('Testing', ('True',))
]


@patch('haproxy.HAProxySocket', MockHAProxySocketSimple)
def test_exclude_metric():
    haproxy.collect_metrics(haproxy.config(mock_config_exclude_bytes_out))

mock_config_enhanced_sample = Mock()
mock_config_enhanced_sample.children = [
    ConfigOption('ProxyMonitor', ('sample_proxy',)),
    ConfigOption('EnhancedMetrics', ('True',)),
    ConfigOption('Testing', ('True',))
]


@patch('haproxy.HAProxySocket', MockHAProxySocketSimple)
def test_enhanced_metrics():
    haproxy.collect_metrics(haproxy.config(mock_config_enhanced_sample))

class MockHAProxySocketComplex(object):
    def __init__(self, socket_file="whatever"):
        self.socket_file = socket_file

    def get_server_info(self):
        sample_data = {'ConnRate': '3', 'CumReq': '5', 'idle_pct': '78'}
        return sample_data

    def get_server_stats(self):
        sample_data = [{'lastchg': '321093', 'agent_health': '', 'check_desc': 'Layer7 check passed',
                         'smax': '2', 'agent_rise': '', 'req_rate': '', 'check_status': 'L7OK', 'wredis': '0',
                         'comp_out': '', 'conn_rate': '', 'cli_abrt': '0', 'pxname': 'elasticsearch_backend',
                         'check_code': '0', 'check_health': '4', 'check_fall': '3', 'qlimit': '', 'bin': '0',
                         'conn_rate_max': '', 'hrsp_5xx': '', 'stot': '344777', 'econ': '0', 'iid': '3',
                         'hrsp_4xx': '', 'hanafail': '', 'downtime': '0', 'eresp': '0', 'bout': '0', 'dses': '',
                         'qtime': '0', 'srv_abrt': '0', 'throttle': '', 'ctime': '0', 'scur': '0', 'type': '2',
                         'check_rise': '2', 'intercepted': '', 'hrsp_2xx': '', 'mode': 'tcp', 'agent_code': '',
                         'qmax': '0', 'agent_desc': '', 'weight': '1', 'slim': '', 'pid': '1', 'comp_byp': '',
                         'lastsess': '0', 'comp_rsp': '', 'agent_status': '', 'check_duration': '0', 'rate': '2',
                         'rate_max': '9', 'dresp': '0', 'ereq': '', 'addr': '172.26.103.5:6379', 'comp_in': '',
                         'dcon': '', 'last_chk': '(tcp-check)', 'sid': '1', 'ttime': '18', 'hrsp_1xx': '',
                         'agent_duration': '', 'hrsp_other': '', 'status': 'UP', 'wretr': '0', 'lbtot': '344777',
                         'dreq': '', 'req_rate_max': '', 'conn_tot': '', 'chkfail': '0', 'cookie': '', 'qcur': '0',
                         'tracked': '', 'rtime': '0', 'last_agt': '', 'bck': '0', 'req_tot': '', 'rate_lim': '',
                         'hrsp_3xx': '', 'algo': '', 'act': '1', 'chkdown': '0', 'svname': 'elasticache',
                         'agent_fall': ''},
                        {'lastchg': '321093', 'agent_health': '', 'check_desc': '', 'smax': '2',
                         'agent_rise': '', 'req_rate': '', 'check_status': '', 'wredis': '0', 'comp_out': '0',
                         'conn_rate': '', 'cli_abrt': '0', 'pxname': 'elasticsearch_backend', 'check_code': '',
                         'check_health': '', 'check_fall': '', 'qlimit': '', 'bin': '0', 'conn_rate_max': '',
                         'hrsp_5xx': '', 'stot': '515751', 'econ': '0', 'iid': '3', 'hrsp_4xx': '', 'hanafail': '',
                         'downtime': '0', 'eresp': '0', 'bout': '0', 'dses': '', 'qtime': '0', 'srv_abrt': '0',
                         'throttle': '', 'ctime': '0', 'scur': '0', 'type': '1', 'check_rise': '', 'intercepted': '',
                         'hrsp_2xx': '', 'mode': 'tcp', 'agent_code': '', 'qmax': '0', 'agent_desc': '', 'weight': '1',
                         'slim': '800', 'pid': '1', 'comp_byp': '0', 'lastsess': '0', 'comp_rsp': '0',
                         'agent_status': '', 'check_duration': '', 'rate': '3', 'rate_max': '9', 'dresp': '0',
                         'ereq': '', 'addr': '', 'comp_in': '0', 'dcon': '', 'last_chk': '', 'sid': '0', 'ttime': '18',
                         'hrsp_1xx': '', 'agent_duration': '', 'hrsp_other': '', 'status': 'UP', 'wretr': '0',
                         'lbtot': '344777', 'dreq': '0', 'req_rate_max': '', 'conn_tot': '', 'chkfail': '',
                         'cookie': '', 'qcur': '0', 'tracked': '', 'rtime': '0', 'last_agt': '', 'bck': '0',
                         'req_tot': '', 'rate_lim': '', 'hrsp_3xx': '', 'algo': 'roundrobin', 'act': '1',
                         'chkdown': '0', 'svname': 'BACKEND', 'agent_fall': ''},
                        {'lastchg': '', 'agent_health': None, 'check_desc': None, 'smax': '0',
                         'agent_rise': None, 'req_rate': '0', 'check_status': '', 'wredis': '', 'comp_out': None,
                         'conn_rate': None, 'cli_abrt': None, 'pxname': 'sensu_frontend', 'check_code': '',
                         'check_health': None, 'check_fall': None, 'qlimit': '', 'bin': '0', 'conn_rate_max': None,
                         'hrsp_5xx': '', 'stot': '0', 'econ': '', 'iid': '4', 'hrsp_4xx': '', 'hanafail': '',
                         'downtime': '', 'eresp': '', 'bout': '0', 'dses': None, 'qtime': None, 'srv_abrt': None,
                         'throttle': '', 'ctime': None, 'scur': '0', 'type': '0', 'check_rise': None,
                         'intercepted': None, 'hrsp_2xx': '', 'mode': None, 'agent_code': None, 'qmax': '',
                         'agent_desc': None, 'weight': '', 'slim': '8000', 'pid': '1', 'comp_byp': None,
                         'lastsess': None, 'comp_rsp': None, 'agent_status': None, 'check_duration': '',
                         'rate': '0', 'rate_max': '10', 'dresp': '0', 'ereq': '0', 'addr': None, 'comp_in': None,
                         'dcon': None, 'last_chk': None, 'sid': '0', 'ttime': None, 'hrsp_1xx': '',
                         'agent_duration': None, 'hrsp_other': '', 'status': 'OPEN', 'wretr': '', 'lbtot': '',
                         'dreq': '0', 'req_rate_max': '0', 'conn_tot': None, 'chkfail': '', 'cookie': None, 'qcur': '',
                         'tracked': '', 'rtime': None, 'last_agt': None, 'bck': '', 'req_tot': '', 'rate_lim': '0',
                         'hrsp_3xx': '', 'algo': None, 'act': '', 'chkdown': '', 'svname': 'FRONTEND',
                         'agent_fall': None}]
        return sample_data

@patch('haproxy.HAProxySocket', MockHAProxySocketComplex)
def test_metrics_submitted_for_frontend_with_correct_names():
    haproxy.submit_metrics = MagicMock()
    mock_config = Mock()
    mock_config.children = [
        ConfigOption('ProxyMonitor', ('frontend',)),
        ConfigOption('EnhancedMetrics', ('True',)),
        ConfigOption('Testing', ('True',))
    ]
    haproxy.collect_metrics(haproxy.config(mock_config))
    haproxy.submit_metrics.assert_has_calls([
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_rate', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'request_rate', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'denied_response', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'error_request', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'denied_request', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'bytes_in', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_total', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'request_rate_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (8000,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_limit', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_rate_limit', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'bytes_out', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_current', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (10,), 'plugin_instance': 'frontend.sensu_frontend', 'type_instance': 'session_rate_max', 'type': 'gauge', 'plugin': 'haproxy'})]
    )

@patch('haproxy.HAProxySocket', MockHAProxySocketComplex)
def test_metrics_submitted_for_backend_and_server_with_correct_names():
    haproxy.submit_metrics = MagicMock()
    mock_config = Mock()
    mock_config.children = [
        ConfigOption('ProxyMonitor', ('backend',)),
        ConfigOption('EnhancedMetrics', ('True',)),
        ConfigOption('Testing', ('True',))
    ]
    haproxy.collect_metrics(haproxy.config(mock_config))
    haproxy.submit_metrics.assert_has_calls([
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'response_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (2,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'comp_byp', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'last_session', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (3,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_rate', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'redispatched', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'comp_out', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'error_response', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'denied_response', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'comp_in', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'denied_request', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'cli_abrt', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'bytes_in', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (344777,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'server_selected_total', 'type': 'counter', 'plugin': 'haproxy'}),
        call({'values': (515751,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_total', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'error_connection', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (18,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (800,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_limit', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'downtime', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'queue_current', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'comp_rsp', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'retries', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'queue_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'srv_abrt', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'bytes_out', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'connect_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_current', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'backup_servers', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'queue_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (9,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'session_rate_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (1,), 'plugin_instance': 'backend.elasticsearch_backend', 'type_instance': 'active_servers', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'response_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (2,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'last_session', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'health_check_duration', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (2,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_rate', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'redispatched', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'error_response', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'denied_response', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'cli_abrt', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'bytes_in', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (344777,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'server_selected_total', 'type': 'counter', 'plugin': 'haproxy'}),
        call({'values': (344777,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_total', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'error_connection', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (18,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'downtime', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'queue_current', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'retries', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'queue_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'srv_abrt', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'bytes_out', 'type': 'derive', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'connect_time_avg', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_current', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'backup_servers', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'queue_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (9,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'session_rate_max', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (1,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'active_servers', 'type': 'gauge', 'plugin': 'haproxy'}),
        call({'values': (0,), 'plugin_instance': 'backend.elasticsearch_backend.elasticache', 'type_instance': 'failed_checks', 'type': 'derive', 'plugin': 'haproxy'}),
    ], any_order=True
    )
