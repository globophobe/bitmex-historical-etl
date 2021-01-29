import pandas as pd

from .constants import EMA, SMA


def parse_window_size(window_size):
    delta = pd.Timedelta(window_size)
    # Days is total_seconds / 60 seconds / 60 minutes / 24 hours
    days = delta.total_seconds() / 60 / 60 / 24
    assert days > 1, "Minimum window size is 1 day."
    return days


def parse_target_frequency(target_frequency):
    pd.Timedelta(target_frequency)
    return target_frequency


def parse_target_type(target_type):
    assert target_type in (SMA, EMA)
    return target_type


def get_initial_adaptive_threshold_cache(thresh_attr):
    return {thresh_attr: 0, "target": float("Inf"), "window": []}


def get_adaptive_target_sma(window):
    series = pd.Series(window)
    return series.mean()


def get_adaptive_target_ema(window, window_size):
    series = pd.Series(window)
    return series.ewm(span=window_size, adjust=False).mean()


def get_adaptive_target(window, window_size, target_type):
    if target_type == SMA:
        return get_adaptive_target_sma(window)
    elif target_type == EMA:
        return get_adaptive_target_ema(window, window_size)
    else:
        raise NotImplementedError


def get_adaptive_target_for_frequency(target, target_frequency):
    delta = pd.Timedelta(target_frequency)
    # Days is total_seconds / 60 seconds / 60 minutes / 24 hours
    average_per_second = target / 60 / 60 / 24
    return average_per_second * delta.total_seconds()


def update_adaptive_cache_window(
    cache, value, window_size, target_type=SMA, target_frequency="1h"
):
    val = float(value)  # Firestore doesn't like 64bit types
    cache["window"].append(val)
    if len(cache["window"]) > window_size:
        cache["window"].pop(0)
        target = get_adaptive_target(cache["window"], window_size)
        cache["target"] = get_adaptive_target_for_frequency(target, target_frequency)
        assert len(cache["window"]) == window_size
    return cache
