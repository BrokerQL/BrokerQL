from ib_insync import Wrapper as _Wrapper, Contract, Order, OrderState, OrderStatus, Trade
from ib_insync.util import UNSET_DOUBLE, dataclassAsDict


class Wrapper(_Wrapper):

    def startReq(self, key, contract=None, container=None):
        if key in self._futures:
            return self._futures[key]
        return super().startReq(key, contract, container)

    def openOrder(self, orderId: int, contract: Contract, order: Order, orderState: OrderState):
        if order.whatIf:
            # response to whatIfOrder
            if orderState.initMarginChange != str(UNSET_DOUBLE):
                self._endReq(order.orderId, orderState)
        else:
            key = self.orderKey(order.clientId, order.orderId, order.permId)
            trade = self.trades.get(key)
            if trade:
                trade.order.permId = order.permId
                trade.order.totalQuantity = order.totalQuantity
                trade.order.lmtPrice = order.lmtPrice
                trade.order.auxPrice = order.auxPrice
                trade.order.orderType = order.orderType
                trade.order.orderRef = order.orderRef
                for k, v in dataclassAsDict(order).items():
                    if v != '?':
                        setattr(trade.order, k, v)
            else:
                # ignore '?' values in the order
                order = Order(**{
                    k: v for k, v in dataclassAsDict(order).items()
                    if v != '?'})
                contract = Contract.create(**dataclassAsDict(contract))
                orderStatus = OrderStatus(
                    orderId=orderId, status=orderState.status)
                trade = Trade(contract, order, orderStatus, [], [])
                self.trades[key] = trade
                self._logger.info(f'openOrder: {trade}')
            self.permId2Trade.setdefault(order.permId, trade)
            results = self._results.get('openOrders')
            if results is None:
                self.ib.openOrderEvent.emit(trade)
            else:
                # response to reqOpenOrders or reqAllOpenOrders
                results.append(trade)

        self.ib.client.updateReqId(orderId + 1)
