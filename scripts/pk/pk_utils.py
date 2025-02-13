from datetime import datetime
from decimal import Decimal
from typing import List

import pandas as pd

from hummingbot.connector.derivative.position import Position
from hummingbot.core.data_type.common import TradeType
from scripts.pk.take_profit_limit_order import TakeProfitLimitOrder
from scripts.pk.tracked_order_details import TrackedOrderDetails


def average(*args) -> Decimal:
    result = sum(args) / len(args) if args else 0
    return Decimal(result)


def are_positions_equal(position_1: Position, position_2: Position) -> bool:
    return (position_1.position_side == position_2.position_side
            and position_1.trading_pair == position_2.trading_pair
            and position_1.amount == position_2.amount)


def compute_recent_price_delta_pct(low_series: pd.Series, high_series: pd.Series, nb_candles_to_consider: int, nb_excluded: int = 0) -> Decimal:
    start_index = nb_candles_to_consider + nb_excluded
    end_index = nb_excluded

    last_lows = low_series.iloc[-start_index:-end_index] if end_index > 0 else low_series.tail(start_index)
    lowest_price = Decimal(last_lows.min())

    last_highs = high_series.iloc[-start_index:-end_index] if end_index > 0 else high_series.tail(start_index)
    highest_price = Decimal(last_highs.max())

    return (highest_price - lowest_price) / highest_price * 100


def compute_sell_orders_pnl_pct(filled_sell_orders: List[TrackedOrderDetails], current_price: Decimal) -> Decimal:
    worst_filled_price = min(filled_sell_orders, key=lambda order: order.last_filled_price).last_filled_price
    return (worst_filled_price - current_price) / worst_filled_price * 100


def compute_buy_orders_pnl_pct(filled_buy_orders: List[TrackedOrderDetails], current_price: Decimal) -> Decimal:
    worst_filled_price = max(filled_buy_orders, key=lambda order: order.last_filled_price).last_filled_price
    return (current_price - worst_filled_price) / worst_filled_price * 100


def compute_stop_loss_price(side: TradeType, ref_price: Decimal, stop_loss_delta: Decimal) -> Decimal:
    if side == TradeType.SELL:
        return ref_price * (1 + stop_loss_delta)

    return ref_price * (1 - stop_loss_delta)


def compute_take_profit_price(side: TradeType, ref_price: Decimal, take_profit_delta: Decimal) -> Decimal:
    if side == TradeType.SELL:
        return ref_price * (1 - take_profit_delta)

    return ref_price * (1 + take_profit_delta)


def has_current_price_reached_stop_loss(tracked_order: TrackedOrderDetails, current_price: Decimal) -> bool:
    stop_loss_delta: Decimal | None = tracked_order.triple_barrier.stop_loss_delta

    if not stop_loss_delta:
        return False

    side: TradeType = tracked_order.side
    ref_price: Decimal = tracked_order.last_filled_price or tracked_order.entry_price
    stop_loss_price: Decimal = compute_stop_loss_price(side, ref_price, stop_loss_delta)

    if side == TradeType.SELL:
        return current_price > stop_loss_price

    return current_price < stop_loss_price


def has_current_price_reached_take_profit(tracked_order: TrackedOrderDetails, current_price: Decimal) -> bool:
    take_profit_delta: Decimal | None = tracked_order.triple_barrier.take_profit_delta

    if not take_profit_delta:
        return False

    side: TradeType = tracked_order.side
    ref_price: Decimal = tracked_order.last_filled_price or tracked_order.entry_price
    take_profit_price: Decimal = compute_take_profit_price(side, ref_price, take_profit_delta)

    if side == TradeType.SELL:
        return current_price < take_profit_price

    return current_price > take_profit_price


def has_unfilled_order_expired(order: TrackedOrderDetails | TakeProfitLimitOrder, expiration: int, current_timestamp: float) -> bool:
    created_at = order.created_at

    return created_at + expiration < current_timestamp


def has_filled_order_reached_time_limit(tracked_order: TrackedOrderDetails, current_timestamp: float) -> bool:
    time_limit: int | None = tracked_order.triple_barrier.time_limit

    if not time_limit:
        return False

    filled_at = tracked_order.last_filled_at

    return filled_at + time_limit < current_timestamp


def was_an_order_recently_opened(tracked_orders: List[TrackedOrderDetails], seconds: int, current_timestamp: float) -> bool:
    if len(tracked_orders) == 0:
        return False

    most_recent_created_at = max(tracked_orders, key=lambda order: order.created_at).created_at

    return most_recent_created_at + seconds > current_timestamp


def timestamp_to_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).isoformat()


def iso_to_timestamp(iso_date: str) -> float:
    return datetime.strptime(iso_date, "%Y-%m-%d").timestamp()


def normalize_timestamp_to_midnight(timestamp: float) -> float:
    dt = datetime.fromtimestamp(timestamp)
    return datetime(dt.year, dt.month, dt.day).timestamp()


# TODO: return int instead of Decimal
def compute_rsi_pullback_difference(rsi: Decimal) -> Decimal:
    """
    When `rsi > 50`:
    i:3 rsi:75 result:72 (rsi-i)
    i:4 rsi:78 result:74 (rsi-i)
    i:5 rsi:81 result:76 (rsi-i)
    i:6 rsi:84 result:78 (rsi-i)
    i:7 rsi:87 result:80 (rsi-i)
    i:8 rsi:90 result:82 (rsi-i)
    i:9 rsi:93 result:84 (rsi-i)

    When `rsi < 50`:
    i:3 rsi:25 result:28 (rsi+i)
    i:4 rsi:22 result:26 (rsi+i)
    i:5 rsi:18 result:24 (rsi+i)
    i:6 rsi:15 result:22 (rsi+i)
    i:7 rsi:12 result:20 (rsi+i)
    i:8 rsi:09 result:18 (rsi+i)
    i:9 rsi:06 result:16 (rsi+i)
    """
    if rsi > 50:
        if rsi < 75:
            return Decimal(2.0)

        decrement = ((rsi - 75) // 3) + 3
        return decrement

    if rsi > 25:
        return Decimal(2.0)

    increment = ((25 - rsi) // 3) + 3
    return increment


def compute_softened_leverage(leverage: int) -> int:
    """
    i:0 leverage:3   result:3   (leverage-i)
    i:1 leverage:5   result:4   (leverage-i)
    i:2 leverage:7   result:5   (leverage-i)
    i:3 leverage:9   result:6   (leverage-i)
    i:4 leverage:11  result:7   (leverage-i)
    i:5 leverage:13  result:8   (leverage-i)
    i:6 leverage:15  result:9   (leverage-i)
    i:7 leverage:17  result:10  (leverage-i)
    i:8 leverage:19  result:11  (leverage-i)
    i:9 leverage:21  result:12  (leverage-i)
    [...]
    """
    decrement: int = (leverage - 3) // 2

    return leverage - decrement
