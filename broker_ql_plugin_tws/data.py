from __future__ import annotations

import configparser
import dataclasses
import functools
from io import UnsupportedOperation

import ib_insync.ib as _ib
from ib_insync import IB
from broker_ql import reloading

from broker_ql.session import Session
from .wrapper import Wrapper

_ib.Wrapper = Wrapper

ib = IB()

__plugin_name__ = "plugin_tws"

_config_parse = configparser.ConfigParser()
_config_parse.add_section(__plugin_name__)

config: configparser.SectionProxy = _config_parse[__plugin_name__]


async def init():
    print("connecting to tws...")
    if ib.isConnected():
        return
    await ib.connectAsync(
        config.get('host'),
        config.getint('port'),
        clientId=config.getint('clientId', 0),
        timeout=config.getint('timeout', 20),
    )
    print("tws connected")


def destroy():
    print("disconnect from tws")
    ib.disconnect()


def camel_case(s: str):
    parts = s.split('_')
    if len(parts) == 1:
        return parts[0]
    return parts[0] + ''.join([p.capitalize() for p in parts[1:]])


@reloading
def schema_provider():
    return {
        "tws": {
            "orders": {
                "order_id": "INT",
                "account": "VARCHAR",
                "symbol": "VARCHAR",
                "action": "VARCHAR",
                "total_quantity": "INT",
                "order_type": "VARCHAR",
                "tif": "VARCHAR",
                "aux_price": "DOUBLE",
                "lmt_price": "DOUBLE",
                "order_ref": "VARCHAR",
                "parent_id": "INT",
                "oca_group": "VARCHAR",
                "status": "VARCHAR",
            },
            "positions": {
                "account": "VARCHAR",
                "symbol": "VARCHAR",
                "position": "DOUBLE",
                "avg_cost": "DOUBLE",
            },
            "subscriptions": {
                "symbol": "VARCHAR",
                "sec_type": "VARCHAR",
                "exchange": "VARCHAR",
                "currency": "VARCHAR",
            },
            "quotes": {
                "symbol": "VARCHAR",
                "bid": "DOUBLE",
                "ask": "DOUBLE",
                "open": "DOUBLE",
                "high": "DOUBLE",
                "low": "DOUBLE",
                "last": "DOUBLE",
                "close": "DOUBLE",
            },
        }
    }


@reloading
def select(table_name):
    if table_name == 'orders':
        return mapping([t for t in ib.openTrades() if t.isActive()], schema_provider()['tws'][table_name], 'order', {
            'symbol': 'contract',
            'status': 'orderStatus',
        })
    elif table_name == 'positions':
        return mapping([p for p in ib.positions() if p.avgCost != 0], schema_provider()['tws'][table_name], 'self', {
            'symbol': 'contract',
        })
    elif table_name == 'subscriptions':
        return mapping([t for t in ib.tickers()], schema_provider()['tws'][table_name], 'contract', {
        })
    elif table_name == 'quotes':
        return mapping([t for t in ib.tickers()], schema_provider()['tws'][table_name], 'self', {
            'symbol': 'contract',
        })
    else:
        return []


@reloading
def mapping(objects: list, cols: list[str], obj_attr, specials: dict):
    rows = []
    for ele in objects:
        row = {}
        for col in cols:
            attr = camel_case(col)
            obj = getattr(ele, obj_attr) if obj_attr != 'self' else ele
            if attr in specials:
                obj = getattr(ele, specials[attr])
            row[col] = getattr(obj, attr)
        rows.append(row)
    return rows


@reloading
async def insert(session: Session, table_name: str, fields: list[str], rows: list) -> int:
    if table_name not in {'subscriptions'}:
        raise UnsupportedOperation()
    affected_rows = 0
    if table_name == 'subscriptions':
        if 'symbol' not in fields:
            return 0
        for row in rows:
            row = {camel_case(k): v for k, v in zip(fields, row)}
            contract = dataclasses.replace(_ib.Contract(exchange="SMART", currency="USD", secType="STK"), **row)
            qualified = await ib.qualifyContractsAsync(contract)
            if not qualified:
                continue
            contract = qualified[0]
            if ib.ticker(contract) is not None:
                continue
            ib.reqMktData(contract)
            affected_rows += 1
    return affected_rows


@reloading
async def update(session: Session, table_name: str, rows: list, fields: dict):
    if table_name not in {'orders'}:
        raise UnsupportedOperation()
    if table_name == 'orders':
        order_rows = {row['order_id']: row for row in rows}
        target_trades = list(filter(lambda x: x.order.orderId in order_rows and x.isActive(), ib.openTrades()))
        for trade in target_trades:
            order = dataclasses.replace(trade.order, parentId=0)
            for k, v in fields.items():
                if k in {'order_id'}:
                    continue
                setattr(order, camel_case(k), order_rows[trade.order.orderId][k])
            if session.in_trx:
                order.transmit = False
                commit = functools.partial(ib.placeOrder, trade.contract, dataclasses.replace(order, transmit=True))
                session.trx_commits.append(commit)
                rollback = functools.partial(ib.placeOrder, trade.contract,
                                             dataclasses.replace(trade.order, parentId=0))
                session.trx_rollbacks.append(rollback)
            ib.placeOrder(trade.contract, order)


@reloading
async def delete(session: Session, table_name: str, rows: list):
    if table_name not in {'orders', 'subscriptions'}:
        raise UnsupportedOperation()
    if table_name == 'orders':
        await session.handle_query("commit", {})
        order_rows = {row['order_id']: row for row in rows}
        target_trades = list(filter(lambda x: x.order.orderId in order_rows and x.isActive(), ib.openTrades()))
        for trade in target_trades:
            ib.cancelOrder(trade.order)
    elif table_name == 'subscriptions':
        for row in rows:
            for t in ib.tickers():
                for k in ['symbol', 'sec_type', 'currency']:
                    if row[k] != getattr(t.contract, camel_case(k)):
                        break
                else:
                    ib.cancelMktData(t.contract)
                    ib.wrapper.tickers.pop(id(t.contract))
                    break
