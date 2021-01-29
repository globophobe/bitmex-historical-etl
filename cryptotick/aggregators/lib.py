from operator import itemgetter


def get_value_display(value):
    if value % 1 == 0:
        return str(int(value))
    else:
        return str(value).replace(".", "d")


def get_next_cache(data_frame, cache, start, extra={}):
    next_day = aggregate_rows(data_frame, start, top_n=cache["maxTopN"], extra=extra)
    if "nextDay" in cache:
        previous_day = cache.pop("nextDay")
        cache["nextDay"] = merge_cache(previous_day, next_day, top_n=cache["maxTopN"])
    else:
        cache["nextDay"] = next_day
    return cache


def aggregate_rows(data_frame, start, stop=None, top_n=10, extra={}):
    df = data_frame.loc[start:stop]
    first_row = df.iloc[0]
    last_row = df.iloc[-1]
    buy_side = df[df.tickRule == 1]
    data = {
        "date": last_row.timestamp.date(),
        "timestamp": last_row.timestamp,
        "nanoseconds": last_row.nanoseconds,
        "open": first_row.price,
        "high": df.price.max(),
        "low": df.price.min(),
        "close": last_row.price,
        "slippage": df.slippage.sum(),
        "buySlippage": buy_side.slippage.sum(),
        "volume": df.volume.sum(),
        "buyVolume": buy_side.volume.sum(),
        "notional": df.notional.sum(),
        "buyNotional": buy_side.notional.sum(),
        "ticks": len(df),
        "buyTicks": len(buy_side),
        "topN": get_top_n(df, top_n=top_n),
    }
    data.update(extra)
    return data


def get_top_n(data_frame, top_n=10):
    if top_n:
        top_n = data_frame.nlargest(top_n, "volume")
        top = top_n.to_dict("records")
        for record in top:
            for key in list(record):
                if key not in (
                    "timestamp",
                    "nanoseconds",
                    "price",
                    "slippage",
                    "volume",
                    "notional",
                    "exponent",
                    "tickRule",
                ):
                    del record[key]
        top.sort(key=itemgetter("timestamp", "nanoseconds"))
        return top
    return []


def merge_cache(previous, current, top_n=10):
    # Price
    current["open"] = previous["open"]
    current["high"] = max(previous["high"], current["high"])
    current["low"] = min(previous["low"], current["low"])
    # Stats
    for key in (
        "slippage",
        "buySlippage",
        "volume",
        "buyVolume",
        "notional",
        "buyNotional",
        "ticks",
        "buyTicks",
    ):
        current[key] += previous[key]
    # Top N
    merged_top = previous["topN"] + current["topN"]
    if len(merged_top):
        # Sort by volume
        sorted(merged_top, key=lambda x: x["volume"], reverse=True)
        # Slice top_n
        m = merged_top[:top_n]
        # Sort by timestamp, nanoseconds
        sorted(m, key=itemgetter("timestamp", "nanoseconds"))
        current["topN"] = m
    return current
