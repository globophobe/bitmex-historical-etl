from ..lib import aggregate_rows, get_next_cache
from .constants import BUY_NOTIONAL, BUY_TICKS, BUY_VOLUME, NOTIONAL, TICKS, VOLUME


def parse_thresh_attr(thresh_attr):
    assert thresh_attr in (VOLUME, BUY_VOLUME, NOTIONAL, BUY_NOTIONAL, TICKS, BUY_TICKS)
    return thresh_attr


def get_initial_threshold_cache(thresh_attr, thresh_value):
    return {thresh_attr: 0, "target": thresh_value}


def aggregate_threshold(data_frame, cache, thresh_attr, top_n=10):
    start = 0
    samples = []
    for index, row in data_frame.iterrows():
        cache[thresh_attr] += row[thresh_attr]
        if cache[thresh_attr] >= cache["target"]:
            sample = aggregate_rows(data_frame, start, stop=index, top_n=top_n)
            samples.append(sample)
            # Reinitialize cache
            cache[thresh_attr] = 0
            # Next index
            start = index + 1
    # Cache
    is_last_row = start == len(data_frame)
    if not is_last_row:
        cache = get_next_cache(data_frame, cache, start)
    return samples, cache
