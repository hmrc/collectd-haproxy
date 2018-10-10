#!/usr/bin/env python
import collections
from mock import MagicMock
from mock import Mock
from mock import patch
import sys
import pytest
import csv


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


class MockHAProxySocket(object):
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


@patch('haproxy.HAProxySocket', MockHAProxySocket)
def test_read():
    haproxy.collect_metrics(haproxy.config(mock_config))

mock_config_exclude_bytes_out = Mock()
mock_config_exclude_bytes_out.children = [
    ConfigOption('ExcludeMetric', ('bytes_out',)),
    ConfigOption('Testing', ('True',))
]


@patch('haproxy.HAProxySocket', MockHAProxySocket)
def test_exclude_metric():
    haproxy.collect_metrics(haproxy.config(mock_config_exclude_bytes_out))

mock_config_enhanced_sample = Mock()
mock_config_enhanced_sample.children = [
    ConfigOption('ProxyMonitor', ('sample_proxy',)),
    ConfigOption('EnhancedMetrics', ('True',)),
    ConfigOption('Testing', ('True',))
]


@patch('haproxy.HAProxySocket', MockHAProxySocket)
def test_enhanced_metrics():
    haproxy.collect_metrics(haproxy.config(mock_config_enhanced_sample))
