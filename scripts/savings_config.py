from decimal import Decimal
from typing import Dict, List, Set

from pydantic import Field

from hummingbot.core.data_type.common import PositionMode
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2ConfigBase


class ExcaliburConfig(StrategyV2ConfigBase):
    # Standard attributes START - avoid renaming
    markets: Dict[str, Set[str]] = Field(default_factory=dict)

    candles_config: List[CandlesConfig] = Field(default_factory=lambda: [
        CandlesConfig(
            connector="hyperliquid_perpetual",
            interval="1m",
            max_records=20,
            trading_pair = "BTC-USD"
        )
    ])

    controllers_config: List[str] = Field(default_factory=list)
    config_update_interval: int = 10
    # Standard attributes END

    # Used by PkStrategy
    connector_name: str = "hyperliquid_perpetual"
    trading_pair: str = "BTC-USD"
    leverage: int = 2
    unfilled_order_expiration: int = 60
    limit_take_profit_price_delta_bps: int = 0

    position_mode: PositionMode = PositionMode.ONEWAY

    # Triple Barrier

    # Order settings
    amount_quote: Decimal = 15.0  # Hyperliquid rejects orders less than $10
    pyramiding: int = 25
    dca_trigger_pct: Decimal = 0.4
    tp_pct: Decimal = 1
