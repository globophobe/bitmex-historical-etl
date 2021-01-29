from google.cloud import bigquery

from ..bqloader import BigQueryLoader, get_schema_columns, get_table_id
from ..cryptotick import CryptoExchangeETL
from ..fscache import FirestoreCache
from ..utils import get_delta


class BaseAggregator(CryptoExchangeETL):
    def __init__(
        self,
        source_table,
        date_from=None,
        date_to=None,
        require_cache=False,
        has_multiple_symbols=False,
        verbose=False,
    ):
        self.source_table = source_table
        self.require_cache = require_cache
        self.has_multiple_symbols = has_multiple_symbols
        self.verbose = verbose

        min_date = self.get_min_date()
        self.initialize_dates(min_date, date_from, date_to)

    def get_source(self, sep="_"):
        return self.source_table.replace("_", sep)

    def get_destination(self, sep="_"):
        destination = self.get_source(sep=sep)
        return f"{destination}{sep}aggregated"

    @property
    def log_prefix(self):
        name = self.get_destination(sep=" ")
        return name[0].capitalize() + name[1:]  # Capitalize first letter

    @property
    def schema(self):
        raise NotImplementedError

    @property
    def columns(self):
        return get_schema_columns(self.schema)

    @property
    def firestore_source(self):
        collection = self.get_source(sep="-")
        return FirestoreCache(collection)

    @property
    def firestore_destination(self):
        collection = self.get_destination(sep="-")
        return FirestoreCache(collection)

    def get_min_date(self):
        data = self.firestore_source.get_one()
        if data:
            if self.has_multiple_symbols:
                dates = []
                for key, symbol in data.items():
                    if isinstance(symbol, dict):
                        if "candles" in symbol:
                            for candle in symbol["candles"]:
                                # Maybe no trades
                                if len(candle):
                                    date = candle["open"]["timestamp"].date()
                                    dates.append(date)
                                    break
                return min(dates)
            else:
                for candle in data["candles"]:
                    # Maybe no trades
                    if len(candle):
                        return candle["open"]["timestamp"].date()

    def get_cache(self, data_frame):
        document = get_delta(self.date, days=-1).isoformat()
        data = self.firestore_destination.get(document)
        # Is cache required, and no data?
        if self.require_cache and not data:
            # Is date greater than min date?
            if self.date > self.date_from:
                date = self.date.isoformat()
                assert data, f"Cache does not exist, {date}"
        if not data:
            # First row of dataframe may be discarded
            data_frame, data = self.get_initial_cache(data_frame)
        return data_frame, data

    def get_data_frame(self, date, index=None):
        table_id = get_table_id(self.table_name)
        # Query by partition.
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("date", "DATE", date),
            ]
        )
        fields = (
            "timestamp, nanoseconds, price, slippage, "
            "volume, notional, tickRule, exponent"
        )
        sql = f"""
            SELECT {fields}
            FROM {table_id}
            WHERE date = @date
            ORDER BY timestamp, nanoseconds, index;
        """
        return BigQueryLoader(self.table_name, self.date).read_table(sql, job_config)

    def get_initial_cache(self, data_frame):
        raise NotImplementedError
