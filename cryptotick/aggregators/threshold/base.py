from google.cloud import bigquery

from ...bqloader import (
    MULTIPLE_SYMBOL_BAR_SCHEMA,
    SINGLE_SYMBOL_BAR_SCHEMA,
    BigQueryLoader,
    get_table_id,
    stringify_datetime_types,
)
from ...fscache import firestore_data
from ...utils import date_range
from ..base import BaseAggregator


class BaseThresholdAggregator(BaseAggregator):
    def __init__(
        self,
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
    ):
        super().__init__(
            source_table,
            date_from=date_from,
            date_to=date_to,
            require_cache=True,
            has_multiple_symbols=has_multiple_symbols,
            verbose=verbose,
        )

        self.destination_table = destination_table
        self.symbol = symbol
        self.min_slippage = min_slippage
        self.max_slippage = max_slippage
        self.min_volume = min_volume
        self.max_volume = max_volume
        self.min_notional = min_notional
        self.max_notional = max_notional
        self.min_exponent = min_exponent
        self.max_exponent = max_exponent
        self.tick_rule = tick_rule
        self.top_n = top_n

    @property
    def schema(self):
        if self.has_multiple_symbols:
            return MULTIPLE_SYMBOL_BAR_SCHEMA
        else:
            return SINGLE_SYMBOL_BAR_SCHEMA

    def get_destination(self, sep="_"):
        return self.destination_table.replace("_", sep)

    def main(self):
        for date in date_range(self.date_from, self.date_to):
            self.date = date
            document = date.isoformat()
            if self.firestore_source.has_data(document):
                if not self.firestore_destination.has_data(document):
                    data_frame = self.get_data_frame()
                    data_frame = self.preprocess_data_frame(data_frame)
                    data, cache = self.process_data_frame(data_frame)
                    self.write(data, cache)
                elif self.verbose:
                    print(f"{self.log_prefix}: {document} OK")
        print(
            f"{self.log_prefix}: "
            f"{self.date_from.isoformat()} to {self.date_to.isoformat()} OK"
        )

    def get_data_frame(self):
        table_id = get_table_id(self.source_table)
        query_parameters = (
            # Query by partition.
            bigquery.ScalarQueryParameter("date", "DATE", self.date),
            bigquery.ScalarQueryParameter("symbol", "STRING", self.symbol),
            bigquery.ScalarQueryParameter("min_volume", "INTEGER", self.min_volume),
            bigquery.ScalarQueryParameter("max_volume", "INTEGER", self.max_volume),
            bigquery.ScalarQueryParameter("min_exponent", "INTEGER", self.min_exponent),
            bigquery.ScalarQueryParameter("max_exponent", "INTEGER", self.max_exponent),
            bigquery.ScalarQueryParameter("min_notional", "FLOAT", self.min_notional),
            bigquery.ScalarQueryParameter("max_notional", "FLOAT", self.max_notional),
            bigquery.ScalarQueryParameter("tick_rule", "INTEGER", self.tick_rule),
        )
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
        max_exp = "=" if self.max_exponent == 0 else "<="
        where_clauses = " AND ".join(
            [
                clause[0]
                for clause in (
                    ("date = @date", self.date),
                    ("symbol = @symbol", self.symbol),
                    ("slippage >= @min_slippage", self.min_slippage),
                    ("slippage <= @max_slippage", self.max_slippage),
                    ("volume >= @min_volume", self.min_volume),
                    ("volume <= @max_volume", self.max_volume),
                    ("notional >= @min_notional", self.min_notional),
                    ("notional <= @max_notional", self.max_notional),
                    ("exponent >= @min_exponent", self.min_exponent),
                    (f"exponent {max_exp} @max_exponent", self.max_exponent),
                    ("tickRule = @tick_rule", self.tick_rule),
                )
                if clause[1] is not None
            ]
        )
        if self.has_multiple_symbols:
            fields = (
                "timestamp, nanoseconds, symbol, price, slippage, "
                "volume, notional, tickRule, exponent"
            )
            order_by = "symbol, timestamp, nanoseconds, index"
        else:
            fields = (
                "timestamp, nanoseconds, price, slippage, "
                "volume, notional, tickRule, exponent"
            )
            order_by = "timestamp, nanoseconds, index"
        sql = f"""
            SELECT {fields}
            FROM {table_id}
            WHERE {where_clauses}
            ORDER BY {order_by};
        """
        return BigQueryLoader(self.source_table, self.date).read_table(sql, job_config)

    def preprocess_data_frame(self, data_frame):
        data_frame["date"] = data_frame.apply(lambda x: x.timestamp.date(), axis=1)
        data_frame["hour"] = data_frame.apply(lambda x: x.timestamp.hour, axis=1)
        data_frame["buySlippage"] = data_frame.apply(
            lambda x: x.slippage if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellSlippage"] = data_frame.apply(
            lambda x: x.slippage if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellVolume"] = data_frame.apply(
            lambda x: x.volume if x.tickRule == -1 else 0, axis=1
        )
        data_frame["buyNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellNotional"] = data_frame.apply(
            lambda x: x.volume / x.price if x.tickRule == -1 else 0, axis=1
        )
        data_frame["ticks"] = 1
        data_frame["buyTicks"] = data_frame.apply(
            lambda x: 1 if x.tickRule == 1 else 0, axis=1
        )
        data_frame["sellTicks"] = data_frame.apply(
            lambda x: 1 if x.tickRule == -1 else 0, axis=1
        )
        return data_frame

    def process_data_frame(self, data_frame):
        data_frame, cache = self.get_cache(data_frame)
        data, cache = self.aggregate(data_frame, cache)
        # Index
        for index, d in enumerate(data):
            data[index] = stringify_datetime_types(firestore_data(d, strip_date=False))
            data[index]["topN"] = [
                stringify_datetime_types(firestore_data(t)) for t in d["topN"]
            ]
            data[index]["index"] = index
        return data, cache

    def aggregate(self, data_frame, cache):
        raise NotImplementedError

    def write(self, data, cache):
        # BigQuery
        table_name = self.get_destination(sep="_")
        bigquery_loader = BigQueryLoader(table_name, self.date)
        bigquery_loader.write_table(self.schema, data)
        # Firebase
        self.set_firebase(
            firestore_data(cache), attr="firestore_destination", is_complete=True
        )
