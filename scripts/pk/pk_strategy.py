from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Tuple

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType, PositionAction, PriceType, TradeType
from hummingbot.core.event.events import (
    BuyOrderCreatedEvent,
    OrderCancelledEvent,
    OrderFilledEvent,
    SellOrderCreatedEvent,
)
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.position_executor.data_types import PositionExecutorConfig
from hummingbot.strategy_v2.models.executors import CloseType
from scripts.pk.pk_triple_barrier import TripleBarrier
from scripts.pk.pk_utils import (
    compute_take_profit_price,
    has_current_price_reached_stop_loss,
    has_current_price_reached_take_profit,
    has_filled_order_reached_time_limit,
    has_unfilled_order_expired,
)
from scripts.pk.take_profit_limit_order import TakeProfitLimitOrder
from scripts.pk.tracked_order_details import TrackedOrderDetails
from scripts.savings_config import ExcaliburConfig


class PkStrategy(StrategyV2Base):
    def __init__(self, connectors: Dict[str, ConnectorBase], config: ExcaliburConfig):
        super().__init__(connectors, config)
        self.config = config

        self.is_a_sell_order_being_created = False
        self.is_a_buy_order_being_created = False

        self.tracked_orders: List[TrackedOrderDetails] = []
        self.take_profit_limit_orders: List[TakeProfitLimitOrder] = []

    def get_mid_price(self) -> Decimal:
        connector_name = self.config.connector_name
        trading_pair = self.config.trading_pair

        return self.market_data_provider.get_price_by_type(connector_name, trading_pair, PriceType.MidPrice)

    def get_best_ask(self) -> Decimal:
        return self._get_best_ask_or_bid(PriceType.BestAsk)

    def get_best_bid(self) -> Decimal:
        return self._get_best_ask_or_bid(PriceType.BestBid)

    def _get_best_ask_or_bid(self, price_type: PriceType) -> Decimal:
        connector_name = self.config.connector_name
        trading_pair = self.config.trading_pair

        return self.market_data_provider.get_price_by_type(connector_name, trading_pair, price_type)

    def get_executor_config(self, side: TradeType, entry_price: Decimal, amount_quote: Decimal) -> PositionExecutorConfig:
        connector_name = self.config.connector_name
        trading_pair = self.config.trading_pair
        leverage = self.config.leverage

        amount: Decimal = amount_quote / entry_price

        return PositionExecutorConfig(
            timestamp=self.get_market_data_provider_time(),
            connector_name=connector_name,
            trading_pair=trading_pair,
            side=side,
            entry_price=entry_price,
            amount=amount,
            leverage=leverage,
            type = "position_executor"
        )

    def find_tracked_order_of_id(self, order_id: str) -> TrackedOrderDetails | None:
        orders_of_that_id = [order for order in self.tracked_orders if order.order_id == order_id]
        return None if len(orders_of_that_id) == 0 else orders_of_that_id[0]

    def find_last_terminated_filled_order(self, side: TradeType, ref: str) -> TrackedOrderDetails | None:
        terminated_filled_orders = [order for order in self.tracked_orders if (
            order.side == side and
            order.ref == ref and
            order.last_filled_at and
            order.terminated_at
        )]

        if len(terminated_filled_orders) == 0:
            return None

        return max(terminated_filled_orders, key=lambda order: order.terminated_at)

    def get_active_tracked_orders(self, ref: str | None = None) -> List[TrackedOrderDetails]:
        active_tracked_orders = [order for order in self.tracked_orders if order.created_at and not order.terminated_at]

        if ref:
            active_tracked_orders = [order for order in active_tracked_orders if order.ref == ref]

        return active_tracked_orders

    def get_active_tracked_orders_by_side(self, ref: str | None = None) -> Tuple[List[TrackedOrderDetails], List[TrackedOrderDetails]]:
        active_orders = self.get_active_tracked_orders(ref)
        active_sell_orders = [order for order in active_orders if order.side == TradeType.SELL]
        active_buy_orders = [order for order in active_orders if order.side == TradeType.BUY]
        return active_sell_orders, active_buy_orders

    def get_unfilled_tracked_orders_by_side(self, ref: str | None = None) -> Tuple[List[TrackedOrderDetails], List[TrackedOrderDetails]]:
        active_sell_orders, active_buy_orders = self.get_active_tracked_orders_by_side(ref)
        unfilled_sell_orders = [order for order in active_sell_orders if not order.last_filled_at]
        unfilled_buy_orders = [order for order in active_buy_orders if not order.last_filled_at]
        return unfilled_sell_orders, unfilled_buy_orders

    def get_filled_tracked_orders_by_side(self, ref: str | None = None) -> Tuple[List[TrackedOrderDetails], List[TrackedOrderDetails]]:
        active_sell_orders, active_buy_orders = self.get_active_tracked_orders_by_side(ref)
        filled_sell_orders = [order for order in active_sell_orders if order.last_filled_at]
        filled_buy_orders = [order for order in active_buy_orders if order.last_filled_at]
        return filled_sell_orders, filled_buy_orders

    def get_all_unfilled_tp_limit_orders(self) -> List[TakeProfitLimitOrder]:
        return [order for order in self.take_profit_limit_orders if not order.last_filled_at]

    def get_all_filled_tp_limit_orders(self) -> List[TakeProfitLimitOrder]:
        return [order for order in self.take_profit_limit_orders if order.last_filled_at]

    def get_unfilled_tp_limit_orders(self, tracked_order: TrackedOrderDetails) -> List[TakeProfitLimitOrder]:
        return [order for order in self.get_all_unfilled_tp_limit_orders() if order.tracked_order.order_id == tracked_order.order_id]

    def get_latest_filled_tp_limit_order(self) -> TakeProfitLimitOrder | None:
        filled_tp_orders = self.get_all_filled_tp_limit_orders()

        if len(filled_tp_orders) == 0:
            return None

        return filled_tp_orders[-1]  # The latest filled TP is necessarily the last one created

    def create_order(self, side: TradeType, entry_price: Decimal, triple_barrier: TripleBarrier, amount_quote: Decimal, ref: str):
        executor_config = self.get_executor_config(side, entry_price, amount_quote)
        self.create_individual_order(executor_config, triple_barrier, ref)

    # async def create_twap_market_orders(self, side: TradeType, entry_price: Decimal, triple_barrier: TripleBarrier, amount_quote: Decimal, ref: str):
    #     executor_config = self.get_executor_config(side, entry_price, amount_quote, True)
    #
    #     for _ in range(self.config.market_order_twap_count):
    #         is_an_order_being_created: bool = self.is_a_sell_order_being_created if executor_config.side == TradeType.SELL else self.is_a_buy_order_being_created
    #
    #         if is_an_order_being_created:
    #             self.logger().error("ERROR: Cannot create another individual order, as one is being created")
    #         else:
    #             self.create_individual_order(executor_config, triple_barrier, ref)
    #             await asyncio.sleep(self.config.market_order_twap_interval)

    def create_individual_order(self, executor_config: PositionExecutorConfig, triple_barrier: TripleBarrier, ref: str):
        connector_name = executor_config.connector_name
        trading_pair = executor_config.trading_pair
        amount = executor_config.amount
        entry_price = executor_config.entry_price
        open_order_type = triple_barrier.open_order_type

        if executor_config.side == TradeType.SELL:
            self.is_a_sell_order_being_created = True

            order_id = self.sell(connector_name, trading_pair, amount, open_order_type, entry_price)

            self.tracked_orders.append(TrackedOrderDetails(
                connector_name=connector_name,
                trading_pair=trading_pair,
                side=TradeType.SELL,
                order_id=order_id,
                amount=amount,
                entry_price=entry_price,
                triple_barrier=triple_barrier,
                ref=ref,
                created_at=self.get_market_data_provider_time()  # Because some exchanges such as gate_io trigger the `did_create_xxx_order` event after 1s
            ))

            self.is_a_sell_order_being_created = False

        else:
            self.is_a_buy_order_being_created = True

            order_id = self.buy(connector_name, trading_pair, amount, open_order_type, entry_price)

            self.tracked_orders.append(TrackedOrderDetails(
                connector_name=connector_name,
                trading_pair=trading_pair,
                side=TradeType.BUY,
                order_id=order_id,
                amount=amount,
                entry_price=entry_price,
                triple_barrier=triple_barrier,
                ref=ref,
                created_at=self.get_market_data_provider_time()
            ))

            self.is_a_buy_order_being_created = False

        self.logger().info(f"create_order: {self.tracked_orders[-1]}")

    def create_tp_limit_order(self, tracked_order: TrackedOrderDetails, amount: Decimal, entry_price: Decimal):
        side: TradeType = TradeType.SELL if tracked_order.side == TradeType.BUY else TradeType.BUY
        trading_pair: str = tracked_order.trading_pair

        executor_config = self.get_executor_config(side, entry_price, amount)
        connector_name: str = executor_config.connector_name

        if executor_config.side == TradeType.SELL:
            order_id = self.sell(connector_name, trading_pair, amount, OrderType.LIMIT, entry_price, PositionAction.CLOSE)

            self.take_profit_limit_orders.append(TakeProfitLimitOrder(
                order_id=order_id,
                tracked_order=tracked_order,
                amount=amount,
                entry_price=entry_price,
                created_at=self.get_market_data_provider_time()
            ))

        else:
            order_id = self.buy(connector_name, trading_pair, amount, OrderType.LIMIT, entry_price, PositionAction.CLOSE)

            self.take_profit_limit_orders.append(TakeProfitLimitOrder(
                order_id=order_id,
                tracked_order=tracked_order,
                amount=amount,
                entry_price=entry_price,
                created_at=self.get_market_data_provider_time()
            ))

        self.logger().info(f"create_tp_limit_order: {self.take_profit_limit_orders[-1]}")

    # TODO: remove?
    # def cancel_tracked_order(self, tracked_order: TrackedOrderDetails):
    #     if tracked_order.last_filled_at:
    #         self.close_filled_order(tracked_order, OrderType.MARKET, CloseType.EARLY_STOP)
    #         self.cancel_take_profit_for_order(tracked_order)
    #     else:
    #         self.cancel_unfilled_order(tracked_order)

    def close_filled_order(self, filled_order: TrackedOrderDetails, market_or_limit: OrderType, close_type: CloseType):
        connector_name = filled_order.connector_name
        trading_pair = filled_order.trading_pair
        filled_amount = filled_order.filled_amount

        close_price_sell = self.get_best_bid() * Decimal(1 - self.config.limit_take_profit_price_delta_bps / 10000)
        close_price_buy = self.get_best_ask() * Decimal(1 + self.config.limit_take_profit_price_delta_bps / 10000)

        self.logger().info(f"close_filled_order | close_price:{close_price_sell if filled_order.side == TradeType.SELL else close_price_buy}")

        if filled_order.side == TradeType.SELL:
            self.buy(connector_name, trading_pair, filled_amount, market_or_limit, close_price_sell, PositionAction.CLOSE)
        else:
            self.sell(connector_name, trading_pair, filled_amount, market_or_limit, close_price_buy, PositionAction.CLOSE)

        for order in self.tracked_orders:
            if order.order_id == filled_order.order_id:
                order.terminated_at = self.get_market_data_provider_time()
                order.close_type = close_type
                break

    def close_filled_orders(self, filled_orders: List[TrackedOrderDetails], market_or_limit: OrderType, close_type: CloseType):
        for filled_order in filled_orders:
            self.close_filled_order(filled_order, market_or_limit, close_type)
            self.cancel_take_profit_for_order(filled_order)

    def cancel_unfilled_order(self, tracked_order: TrackedOrderDetails):
        connector_name = tracked_order.connector_name
        trading_pair = tracked_order.trading_pair
        order_id = tracked_order.order_id

        self.logger().info(f"cancel_unfilled_order: {tracked_order}")
        self.cancel(connector_name, trading_pair, order_id)

    def cancel_take_profit_for_order(self, filled_order: TrackedOrderDetails):
        connector_name = filled_order.connector_name
        trading_pair = filled_order.trading_pair

        for tp_limit_order in self.get_unfilled_tp_limit_orders(filled_order):
            order_id = tp_limit_order.order_id
            self.logger().info(f"cancel_take_profit_for_order: {tp_limit_order}")
            self.cancel(connector_name, trading_pair, order_id)

    def did_create_sell_order(self, created_event: SellOrderCreatedEvent):
        position = created_event.position

        if not position or position == PositionAction.CLOSE.value:
            self.logger().info(f"did_create_sell_order | position:{position}")
            return

        for tracked_order in self.tracked_orders:
            if tracked_order.order_id == created_event.order_id:
                tracked_order.exchange_order_id = created_event.exchange_order_id
                self.logger().info(f"did_create_sell_order: {tracked_order}")
                break

    def did_create_buy_order(self, created_event: BuyOrderCreatedEvent):
        position = created_event.position

        if not position or position == PositionAction.CLOSE.value:
            self.logger().info(f"did_create_buy_order | position:{position}")
            return

        for tracked_order in self.tracked_orders:
            if tracked_order.order_id == created_event.order_id:
                tracked_order.exchange_order_id = created_event.exchange_order_id
                self.logger().info(f"did_create_buy_order: {tracked_order}")
                break

    def did_fill_order(self, filled_event: OrderFilledEvent):
        position = filled_event.position

        if not position or position == PositionAction.CLOSE.value:
            self.logger().info(f"did_fill_order | position:{position}")

            for take_profit_limit_order in self.take_profit_limit_orders:
                if take_profit_limit_order.order_id == filled_event.order_id:
                    self.logger().info(f"did_fill_order | Take Profit price reached for tracked order:{take_profit_limit_order.tracked_order}")

                    take_profit_limit_order.filled_amount += filled_event.amount
                    take_profit_limit_order.last_filled_at = filled_event.timestamp
                    take_profit_limit_order.last_filled_price = filled_event.price

                    self.logger().info(f"did_fill_order | amount:{filled_event.amount} at price:{filled_event.price}")

                    if take_profit_limit_order.filled_amount != take_profit_limit_order.amount:
                        self.logger().info("did_fill_order > OMG we got a partial fill of a limit TP!!!")

                    for tracked_order in self.tracked_orders:
                        if tracked_order.order_id == take_profit_limit_order.tracked_order.order_id:
                            tracked_order.filled_amount -= filled_event.amount
                            self.logger().info(f"did_fill_order | tracked_order.filled_amount reduced to:{tracked_order.filled_amount}")

                            if tracked_order.filled_amount == 0:
                                self.logger().info("did_fill_order > tracked_order.filled_amount == 0! Closing it")
                                tracked_order.terminated_at = filled_event.timestamp
                                tracked_order.close_type = CloseType.TAKE_PROFIT

                            break

                    break

            return

        for tracked_order in self.tracked_orders:
            if tracked_order.order_id == filled_event.order_id:
                tracked_order.filled_amount += filled_event.amount
                tracked_order.last_filled_at = filled_event.timestamp
                tracked_order.last_filled_price = filled_event.price

                self.logger().info(f"did_fill_order | amount:{filled_event.amount} at price:{filled_event.price}")

                take_profit_delta = tracked_order.triple_barrier.take_profit_delta
                tp_order_type = tracked_order.triple_barrier.take_profit_order_type

                if take_profit_delta and tp_order_type == OrderType.LIMIT:
                    take_profit_price = compute_take_profit_price(tracked_order.side, filled_event.price, take_profit_delta)
                    self.logger().info(f"did_fill_order | take_profit_delta:{take_profit_delta} | take_profit_price:{take_profit_price}")
                    self.create_tp_limit_order(tracked_order, filled_event.amount, take_profit_price)

                break

    def did_cancel_order(self, cancelled_event: OrderCancelledEvent):
        self.logger().info(f"did_cancel_order | cancelled_event:{cancelled_event}")

        for order in self.tracked_orders:
            if order.order_id == cancelled_event.order_id:
                order.terminated_at = self.get_market_data_provider_time()
                order.close_type = CloseType.EXPIRED
                break

        for order in self.take_profit_limit_orders:
            if order.order_id == cancelled_event.order_id:
                self.take_profit_limit_orders.remove(order)
                break

    def can_create_order(self, side: TradeType, amount_quote: Decimal, ref: str, cooldown_time_min: int) -> bool:
        if amount_quote == 0:
            return False

        if side == TradeType.SELL and self.is_a_sell_order_being_created:
            self.logger().error("ERROR: Another SELL order is being created, avoiding a duplicate")
            return False

        if side == TradeType.BUY and self.is_a_buy_order_being_created:
            self.logger().error("ERROR: Another BUY order is being created, avoiding a duplicate")
            return False

        last_terminated_filled_order = self.find_last_terminated_filled_order(side, ref)

        if not last_terminated_filled_order:
            return True

        if last_terminated_filled_order.terminated_at + cooldown_time_min * 60 > self.get_market_data_provider_time():
            self.logger().info(f"Cooldown not passed yet for {side}")
            return False

        return True

    def check_orders(self):
        self.check_unfilled_orders()
        self.check_trading_orders()

    def check_unfilled_orders(self):
        unfilled_order_expiration = self.config.unfilled_order_expiration

        if not unfilled_order_expiration:
            return

        unfilled_sell_orders, unfilled_buy_orders = self.get_unfilled_tracked_orders_by_side()

        for unfilled_order in unfilled_sell_orders + unfilled_buy_orders:
            if has_unfilled_order_expired(unfilled_order, unfilled_order_expiration, self.get_market_data_provider_time()):
                self.logger().info("unfilled_order_has_expired")
                self.cancel_unfilled_order(unfilled_order)

    def check_trading_orders(self):
        current_price = self.get_mid_price()
        filled_sell_orders, filled_buy_orders = self.get_filled_tracked_orders_by_side()

        for filled_order in filled_sell_orders + filled_buy_orders:
            if has_current_price_reached_stop_loss(filled_order, current_price):
                self.logger().info(f"current_price_has_reached_stop_loss | current_price:{current_price}")
                self.close_filled_order(filled_order, OrderType.MARKET, CloseType.STOP_LOSS)
                self.cancel_take_profit_for_order(filled_order)
                continue

            if len(self.get_unfilled_tp_limit_orders(filled_order)) == 0 and has_current_price_reached_take_profit(filled_order, current_price):
                self.logger().info(f"current_price_has_reached_take_profit | current_price:{current_price}")
                take_profit_order_type = filled_order.triple_barrier.take_profit_order_type
                self.close_filled_order(filled_order, take_profit_order_type, CloseType.TAKE_PROFIT)
                continue

            if has_filled_order_reached_time_limit(filled_order, self.get_market_data_provider_time()):
                self.logger().info(f"filled_order_has_reached_time_limit | current_price:{current_price}")
                time_limit_order_type = filled_order.triple_barrier.time_limit_order_type
                self.close_filled_order(filled_order, time_limit_order_type, CloseType.TIME_LIMIT)
                self.cancel_take_profit_for_order(filled_order)

    @staticmethod
    def get_market_data_provider_time() -> float:
        return datetime.now().timestamp()
