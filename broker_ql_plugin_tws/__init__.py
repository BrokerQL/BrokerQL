from broker_ql.session import Session
from .data import init, destroy, config as plugin_config, schema_provider, select, insert, update, delete

Session.SCHEMA_PROVIDERS.append(schema_provider)
Session.DATA_PROVIDERS['tws'] = select
Session.DATA_MODIFIERS['tws'] = update
Session.DATA_REMOVERS['tws'] = delete
Session.DATA_CREATORS['tws'] = insert

plugin_arguments = [
    (['--plugin-tws-clientId'], dict(help='TWS ClientId')),
]
