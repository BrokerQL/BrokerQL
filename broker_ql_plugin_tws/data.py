from __future__ import annotations

import asyncio
import configparser
import dataclasses
import functools
import inspect
import traceback
from asyncio import InvalidStateError
from io import UnsupportedOperation
from typing import Optional, Dict, List

import ib_insync.ib as _ib
from ib_insync import IB
from mysql_mimic.errors import MysqlError, ErrorCode
from sqlglot.executor.env import ENV as _ENV

from broker_ql import reloading
from broker_ql.session import Session
from .wrapper import Wrapper, UNSET_DOUBLE

_ib.Wrapper = Wrapper

ib = IB()

_FUTURES: Dict[int | str, asyncio.Future] = {}


def _on_error(req_id, err_code, err_string, contract):
    try:
        future = _FUTURES.pop(req_id)
        future.set_exception(Exception(err_string))
    except KeyError:
        pass
    except InvalidStateError:
        pass


def _on_order_open(trade: _ib.Trade):
    try:
        future = _FUTURES.pop(trade.order.orderId)
        future.set_result(None)
    except KeyError:
        pass
    except InvalidStateError:
        pass


ib.errorEvent += _on_error
ib.openOrderEvent += _on_order_open

__plugin_name__ = "plugin_tws"
__database_name__ = "tws"

_config_parse = configparser.ConfigParser()
_config_parse.add_section(__plugin_name__)

config: configparser.SectionProxy = _config_parse[__plugin_name__]


def next_order_id():
    return ib.client.getReqId()


_ENV['TWS_NEXT_ORDER_ID'] = next_order_id


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
        __database_name__: {
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
                "trailing_percent": "DOUBLE",
                "trail_stop_price": "DOUBLE",
                "order_ref": "VARCHAR",
                "parent_id": "INT",
                "oca_group": "VARCHAR",
                "transmit": "BOOLEAN",
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
            "ohlcv": {
                "date": "TIMESTAMP",
                "symbol": "VARCHAR",
                "open": "DOUBLE",
                "high": "DOUBLE",
                "low": "DOUBLE",
                "close": "DOUBLE",
                "volume": "DOUBLE",
            },
            "accounts": {
                "account": "VARCHAR",
                "net_liquidation": "DOUBLE",
                "total_cash_value": "DOUBLE",
            },
        }
    }


@reloading
async def select(table_name: str, where: Optional[List[Dict]] = None):
    schema = schema_provider()[__database_name__]
    if table_name not in schema:
        return None
    columns = schema[table_name]
    if table_name == 'orders':
        return mapping([t for t in ib.openTrades() if t.isActive()], columns, 'order', {
            'symbol': 'contract',
            'status': 'orderStatus',
        })
    elif table_name == 'positions':
        return mapping([p for p in ib.positions() if p.avgCost != 0], columns, 'self', {
            'symbol': 'contract',
        })
    elif table_name == 'subscriptions':
        return mapping([t for t in ib.tickers()], columns, 'contract', {
        })
    elif table_name == 'quotes':
        return mapping([t for t in ib.tickers()], columns, 'self', {
            'symbol': 'contract',
        })
    elif table_name == 'accounts':
        rows = []
        for account in ib.managedAccounts():
            account_info = {f"{tag.tag[0].lower()}{tag.tag[1:]}": tag.value for tag in
                            await ib.accountSummaryAsync(account) if
                            tag.currency in ['USD', '']}
            account_info['account'] = account
            row = {}
            for col, d_type in columns.items():
                value = account_info[camel_case(col)]
                if d_type == 'DOUBLE':
                    value = float(value)
                row[col] = value
            rows.append(row)
        return rows
    elif table_name == 'ohlcv':
        results = []
        if where is not None:
            symbols = [r['symbol'] for r in where]
        else:
            symbols = None
        for t in ib.tickers():
            if symbols is not None and t.contract.symbol not in symbols:
                continue
            bars = await ib.reqHistoricalDataAsync(
                t.contract, '', '50 D', '1 day', 'TRADES', True, formatDate=1,
                timeout=0)
            result = mapping(bars, columns, 'self', {}, consts={
                'symbol': bars.contract.symbol
            })
            results += result
        return results
    else:
        return []


@reloading
def mapping(objects: list, cols: list[str], obj_attr, specials: dict, consts: dict = None):
    if consts is None:
        consts = {}
    rows = []
    for ele in objects:
        row = {}
        for col in cols:
            attr = camel_case(col)
            obj = getattr(ele, obj_attr) if obj_attr != 'self' else ele
            if attr in consts:
                row[col] = consts[attr]
                continue
            if attr in specials:
                obj = getattr(ele, specials[attr])
            value = getattr(obj, attr)
            if isinstance(value, float) and value == UNSET_DOUBLE:
                value = float('nan')
            row[col] = value
        rows.append(row)
    return rows


@reloading
async def insert(session: Session, table_name: str, fields: List[str], rows: List) -> int:
    if table_name not in {'subscriptions', 'orders'}:
        raise UnsupportedOperation()
    affected_rows = 0
    schema = schema_provider()[__database_name__]
    if table_name not in schema:
        return 0
    columns = schema[table_name]
    for col in fields:
        if col not in columns:
            raise MysqlError(f"Unknown column '{table_name}.{col}'", code=ErrorCode.NO_DB_ERROR)
    for i, row in enumerate(rows):
        if len(row) != len(fields):
            raise MysqlError(f"Column count doesn't match value count at row {i + 1}'", code=ErrorCode.PARSE_ERROR)
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
    elif table_name == 'orders':
        if 'symbol' not in fields:
            return 0
        for idx, row in enumerate(rows):
            row = {camel_case(k): v for k, v in zip(fields, row)}
            for t in ib.tickers():
                if t.contract.symbol != row['symbol']:
                    continue
                contract = t.contract
                break
            else:
                contract = _ib.Contract(symbol=row['symbol'], exchange="SMART", currency="USD", secType="STK")
                qualified = await ib.qualifyContractsAsync(contract)
                if not qualified:
                    continue
                contract = qualified[0]
            params = inspect.signature(_ib.Order).parameters
            order = dataclasses.replace(_ib.Order(), **{k: v for k, v in row.items() if k in params})
            commit, rollback = None, None
            if session.in_trx:
                last_row = idx == len(rows) - 1
                order.transmit = False
                commit = functools.partial(ib.placeOrder, contract, dataclasses.replace(order, transmit=last_row))
                rollback = functools.partial(ib.cancelOrder, dataclasses.replace(order))
            try:
                future = asyncio.Future()
                if order.transmit:
                    _FUTURES[order.orderId] = future
                ib.placeOrder(contract, order)
                if order.transmit:
                    try:
                        await asyncio.wait_for(future, 0.2)
                    except asyncio.TimeoutError:
                        pass
                if session.in_trx:
                    session.trx_commits.append(commit)
                    session.trx_rollbacks.append(rollback)
            except Exception as e:
                raise MysqlError(str(e) or traceback.format_exc(), code=ErrorCode.UNKNOWN_ERROR)
            affected_rows += 1
    return affected_rows


@reloading
async def update(session: Session, table_name: str, rows: List, fields: Dict):
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
            commit, rollback = None, None
            if session.in_trx:
                order.transmit = False
                commit = functools.partial(ib.placeOrder, trade.contract, dataclasses.replace(order, transmit=True))
                rollback = functools.partial(ib.placeOrder, trade.contract,
                                             dataclasses.replace(trade.order, parentId=0))
            try:
                future = asyncio.Future()
                _FUTURES[order.orderId] = future
                ib.placeOrder(trade.contract, order)
                await asyncio.wait_for(future, 0.2)
            except asyncio.TimeoutError:
                if session.in_trx:
                    session.trx_commits.append(commit)
                    session.trx_rollbacks.append(rollback)
            except Exception as e:
                raise MysqlError(str(e) or traceback.format_exc(), code=ErrorCode.UNKNOWN_ERROR)


@reloading
async def delete(session: Session, table_name: str, rows: List):
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
