import asyncio
import configparser
import traceback
from importlib import import_module
from typing import Any

from mysql_mimic import MysqlServer
from mysql_mimic.control import TooManyConnections
from mysql_mimic.errors import ErrorCode
from mysql_mimic.stream import MysqlStream
from mysql_mimic.variables import SYSTEM_VARIABLES

from .connection import Connection
from .session import Session
from .version import __version__


class BrokerQLServer(MysqlServer):

    def __init__(self, config: configparser.ConfigParser, session_factory=Session, **serve_kwargs: Any):
        super().__init__(session_factory, **serve_kwargs)
        SYSTEM_VARIABLES.update({
            "version": (str, __version__, False),
            "version_comment": (str, "BrokerQL", False),
            "broker_ql_plugins": (str, config['server']['plugins'], False),
        })
        self.plugins = config['server']['plugins']
        self.config = config
        self.plugin_modules = []

    async def start_server(self, **kwargs: Any) -> None:
        for plugin_name in self.plugins.split(","):
            if plugin_name.strip() == "":
                continue
            try:
                plugin = import_module(f"broker_ql_plugin_{plugin_name}")
                plugin_cnf = self.config[f'plugin_{plugin_name}']
                if hasattr(plugin, 'plugin_config'):
                    plugin.plugin_config.update(plugin_cnf)
                if hasattr(plugin, 'init'):
                    if asyncio.iscoroutinefunction(plugin.init):
                        await plugin.init()
                    elif callable(plugin.init):
                        plugin.init()
                self.plugin_modules.append(plugin)
            except ModuleNotFoundError:
                print(f"load plugin {plugin_name} fail, try pip install broker_ql_plugin_{plugin_name}")
            except:
                print(traceback.format_exc())
        return await super().start_server(**kwargs)

    def close(self) -> None:
        super().close()
        for plugin in self.plugin_modules:
            if hasattr(plugin, 'destroy'):
                if asyncio.iscoroutinefunction(plugin.destroy):
                    asyncio.run(plugin.destroy())
                elif callable(plugin.destroy):
                    plugin.destroy()

    async def _client_connected_cb(
            self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        stream = MysqlStream(reader, writer)

        connection = Connection(
            stream=stream,
            session=self.session_factory(),
            control=self.control,
            server_capabilities=self.capabilities,
            identity_provider=self.identity_provider,
            ssl=self.ssl,
        )

        try:
            connection_id = await self.control.add(connection)
        except TooManyConnections:
            await stream.write(
                connection.error(
                    msg="Too many connections",
                    code=ErrorCode.CON_COUNT_ERROR,
                )
            )
            return
        connection.connection_id = connection_id
        try:
            return await connection.start()
        finally:
            writer.close()
            await self.control.remove(connection_id)
