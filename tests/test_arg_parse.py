import sys
from unittest import mock

from broker_ql.entrypoints.main import parse_args_config


def test_arg_parse_help():
    with mock.patch.object(sys, "exit") as mock_exit:
        args, config = parse_args_config(['--help'])
    assert mock_exit.call_args[0][0] == 0


def test_arg_parse():
    args, config = parse_args_config(['--port=1024', '--plugins=xxx,tws', '--plugin-tws-clientId=256'])
    assert args is not None
    assert config is not None
    assert config['plugin_tws'].getint('clientId') == 256
    assert config.defaults()['port'] == '1024'
    assert config.defaults()['plugins'] == 'xxx,tws'

    args, config = parse_args_config(['--plugin-tws-clientId=256'])
    assert args is not None
    assert config is not None
    assert config['plugin_tws'].getint('clientId') == 256
    assert config.defaults()['port'] != '3306'
