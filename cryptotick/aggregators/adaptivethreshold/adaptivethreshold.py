from ..threshold.base import BaseThresholdAggregator
from ..threshold.lib import aggregate_threshold, parse_thresh_attr
from .constants import SMA
from .lib import (
    get_initial_adaptive_threshold_cache,
    parse_target_frequency,
    parse_target_type,
    parse_window_size,
    update_adaptive_cache_window,
)


class AdaptiveThresholdAggregator(BaseThresholdAggregator):
    def __init__(
        self,
        source_table,
        destination_table,
        thresh_attr,
        window_size,
        target_frequency="1h",
        target_type=SMA,
        symbol=None,
        min_slippage=None,
        max_slippage=None,
        min_volume=None,
        max_volume=None,
        min_notional=None,
        max_notional=None,
        min_exponent=None,
        max_exponent=None,
        tick_rule=None,
        top_n=10,
        date_from=None,
        date_to=None,
        has_multiple_symbols=False,
        verbose=False,
    ):
        super().__init__(
            source_table,
            destination_table,
            symbol=None,
            min_slippage=None,
            max_slippage=None,
            min_volume=None,
            max_volume=None,
            min_notional=None,
            max_notional=None,
            min_exponent=None,
            max_exponent=None,
            tick_rule=None,
            top_n=10,
            date_from=None,
            date_to=None,
            has_multiple_symbols=False,
            verbose=False,
        )

        self.thresh_attr = parse_thresh_attr(thresh_attr)
        self.window_size = parse_window_size(window_size)
        self.target_type = parse_target_type(target_type)
        self.target_frequency = parse_target_frequency(target_frequency)

    def get_initial_cache(self, data_frame):
        cache = get_initial_adaptive_threshold_cache(self.thresh_attr)
        return data_frame, cache

    def aggregate(self, data_frame, cache):
        # Warmup
        remaining = self.window_size - len(cache["window"])
        total = data_frame[self.thresh_attr].sum()
        if remaining > 0:
            data = []
        else:
            data, cache = aggregate_threshold(
                data_frame, cache, self.thresh_attr, top_n=self.top_n
            )
        # Update window and target
        cache = update_adaptive_cache_window(
            cache, total, self.window_size, target_frequency=self.target_frequency
        )
        assert len(cache["window"]) <= self.window_size
        return data, cache
