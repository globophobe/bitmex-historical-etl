#!/usr/bin/env python

# isort:skip_file
import typer

import pathfix  # noqa: F401
from cryptotick.aggregators import ThresholdAggregator
from cryptotick.utils import set_environment


def threshold_aggregator(
    source_table: str = None,
    destination_table: str = None,
    thresh_attr: str = None,
    thresh_value: str = None,
    symbol: str = None,
    min_slippage: float = None,
    max_slippage: float = None,
    min_volume: float = None,
    max_volume: float = None,
    min_notional: float = None,
    max_notional: float = None,
    min_exponent: int = None,
    max_exponent: int = None,
    tick_rule: int = None,
    top_n: int = 10,
    date_from: str = None,
    date_to: str = None,
    has_multiple_symbols: bool = False,
    verbose: bool = False,
):
    set_environment()
    ThresholdAggregator(
        source_table,
        destination_table,
        thresh_attr,
        thresh_value,
        symbol=symbol,
        min_slippage=min_slippage,
        max_slippage=max_slippage,
        min_volume=min_volume,
        max_volume=max_volume,
        min_notional=min_notional,
        max_notional=max_notional,
        min_exponent=min_exponent,
        max_exponent=max_exponent,
        tick_rule=tick_rule,
        top_n=top_n,
        date_from=date_from,
        date_to=date_to,
        has_multiple_symbols=has_multiple_symbols,
        verbose=verbose,
    ).main()


if __name__ == "__main__":
    typer.run(threshold_aggregator)
