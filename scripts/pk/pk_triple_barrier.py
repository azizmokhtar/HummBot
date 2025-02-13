from dataclasses import dataclass
from decimal import Decimal

from hummingbot.core.data_type.common import OrderType


@dataclass
class TripleBarrier:
    stop_loss_delta: Decimal | None = None
    take_profit_delta: Decimal | None = None
    time_limit: int | None = None
    open_order_type: OrderType = OrderType.LIMIT
    take_profit_order_type: OrderType = OrderType.LIMIT
    time_limit_order_type: OrderType = OrderType.MARKET
