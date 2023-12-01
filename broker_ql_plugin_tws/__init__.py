from broker_ql.session import Session
from .data import init, destroy, config as plugin_config, schema_provider, select, insert, update, delete
from .data import __database_name__

Session.SCHEMA_PROVIDERS.append(schema_provider)
Session.DATA_PROVIDERS[__database_name__] = select
Session.DATA_MODIFIERS[__database_name__] = update
Session.DATA_REMOVERS[__database_name__] = delete
Session.DATA_CREATORS[__database_name__] = insert

plugin_arguments = [
    (['--plugin-tws-clientId'], dict(help='TWS ClientId')),
]
