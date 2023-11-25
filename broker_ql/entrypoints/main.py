import argparse
import asyncio
import configparser
import logging.config
import os
import sys
from importlib import import_module, resources
from threading import Thread, Condition
from typing import Any, Dict, List, Tuple, Optional

import nest_asyncio

from broker_ql.server import BrokerQLServer
from broker_ql.version import __version__

nest_asyncio.apply()

DEFAULT_CNF = """
[server]
port=3306
plugins=tws

[plugin_tws]
host=127.0.0.1
port=7496
""".strip()


def parse_args_config(args_list) -> Tuple[argparse.Namespace, Optional[configparser.ConfigParser]]:
    parser = argparse.ArgumentParser(prog='BrokerQL', add_help=False)
    parser.add_argument('-v', '--version', action='version', version=f'%(prog)s v{__version__}')
    parser.add_argument('--cnf', default="~/.broker_ql.conf", help="config file")
    parser.add_argument('--cnf-gen-only', action='store_true', help="generate config file and exit")
    args, _ = parser.parse_known_args(args_list)
    cnf_path = os.path.expanduser(args.cnf)
    if not os.path.exists(cnf_path):
        with open(cnf_path, 'w') as f:
            f.write(DEFAULT_CNF)

    if args.cnf_gen_only:
        return args, None

    config = configparser.ConfigParser(default_section='server')
    config.optionxform = lambda option: option
    config.read(cnf_path)

    parser = argparse.ArgumentParser(prog='broker-ql', description="")
    parser.add_argument('-v', '--version', action='version', version=f'BrokerQL v{__version__}')
    parser.add_argument('--cnf', default="~/.broker_ql.conf", help="config file")
    parser.add_argument('--cnf-gen-only', action='store_true', help="generate config file and exit")
    parser.add_argument('--cli', action='store_true')
    parser.add_argument('--port', default=3306, help='')
    parser.add_argument('--plugins', default='tws', help='')

    for plugin_name in config[config.default_section].get('plugins', '').split(","):
        if plugin_name.strip() == '':
            continue
        try:
            plugin = import_module(f"broker_ql_plugin_{plugin_name}")
            if hasattr(plugin, 'plugin_arguments'):
                plugin_arguments: List[Dict[str, Any]] = plugin.plugin_arguments
                for p_args, p_kwargs in plugin_arguments:
                    parser.add_argument(*p_args, **p_kwargs)
        except ModuleNotFoundError:
            pass

    args = parser.parse_args(args_list)
    for section in config.sections() + [config.default_section]:
        if section == config.default_section:
            splitter = ''
            prefix = ''
        else:
            splitter = '_'
            prefix = section
        for entry in config[section].keys():
            attr = f"{prefix}{splitter}{entry}"
            arg = f"--{attr.replace('_', '-')}"
            if hasattr(args, attr) and any(map(lambda x: arg in x, args_list)):
                argv = getattr(args, attr)
                config[section][entry] = str(argv)
    return args, config


def main():
    args, config = parse_args_config(sys.argv[1:])

    if 'port' not in config.defaults():
        print(f"missing {config.default_section} section in config file {args.cnf}", file=sys.stderr)
        sys.exit(1)

    server_cnf = config[config.default_section]

    server = BrokerQLServer(port=server_cnf.getint('port'), config=config)
    if not args.cli:
        try:
            asyncio.run(server.serve_forever())
        except KeyboardInterrupt:
            pass
    else:
        import mycli
        from mycli import config
        from mycli.main import cli

        def create_default_config(list_values=True):
            default_config_file = resources.open_text('broker_ql', 'myclirc_nt' if os.name == 'nt' else 'myclirc')
            return config.read_config_file(default_config_file, list_values=list_values)

        def write_default_config(destination, overwrite=False):
            pass

        config.create_default_config = create_default_config
        mycli.main.write_default_config = write_default_config

        os.environ['MYCLI_HISTFILE'] = '~/.broker_ql_history'
        started = Condition()

        def lifecycle():
            with started:
                started.wait()
            logging.config.dictConfig({
                'version': 1,
                'disable_existing_loggers': True,
            })
            cli(["-h127.0.0.1", f"-P{server_cnf.getint('port')}", '--myclirc=~/.broker_ql_myclirc'],
                standalone_mode=False)
            server.close()

        Thread(target=lifecycle, daemon=True).start()

        async def wait_stop():
            await server.start_server()
            with started:
                started.notify()
            while server.sockets():
                await asyncio.sleep(0.1)

        try:
            asyncio.run(wait_stop())
        except KeyboardInterrupt:
            pass


if __name__ == '__main__':
    main()
