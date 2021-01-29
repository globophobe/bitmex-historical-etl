from .base import BaseThresholdAggregator
from .lib import aggregate_threshold, get_initial_threshold_cache, parse_thresh_attr


class ThresholdAggregator(BaseThresholdAggregator):
    def __init__(
        self,
        source_table,
        destination_table,
        thresh_attr,
        thresh_value,
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
        self.thresh_value = int(thresh_value)

    def get_initial_cache(self, data_frame):
        cache = get_initial_threshold_cache(self.thresh_attr, self.thresh_value)
        return data_frame, cache

    def aggregate(self, data_frame, cache):
        return aggregate_threshold(
            data_frame, cache, self.thresh_attr, self.thresh_value, top_n=self.top_n
        )
