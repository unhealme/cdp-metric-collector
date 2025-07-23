from datetime import datetime

from msgspec import Struct

from cdp_metric_collector.cm_lib.utils import calc_perc

from ._abc import Decodable, DTNoTZ


class AggregateTimeSeriesData(DTNoTZ):
    count: int
    min: float
    minTime: datetime
    max: float
    maxTime: datetime


class TimeSeriesData(DTNoTZ):
    timestamp: datetime
    value: float
    aggregateStatistics: AggregateTimeSeriesData


class TimeSeriesMetaAttr(Struct):
    poolName: str


class TimeSeriesMeta(DTNoTZ):
    metricName: str
    attributes: TimeSeriesMetaAttr


class TimeSeries(Struct):
    metadata: TimeSeriesMeta
    data: list[TimeSeriesData]


class DataItem(Struct):
    timeSeries: list[TimeSeries]


class TimeSeriesJoined(Struct, array_like=True):
    timestamp: datetime
    pool: str
    metric: str
    value: float
    perc_value: str | None
    min: float
    perc_min: str | None
    at_min: datetime
    max: float
    perc_max: str | None
    at_max: datetime
    aggregations: int

    def __iter__(self):
        for f in self.__struct_fields__:
            yield getattr(self, f)

    def to_row(self):
        for f in self:
            match f:
                case str():
                    yield f
                case datetime():
                    yield f.isoformat(" ", "milliseconds")
                case None:
                    yield ""
                case _:
                    yield str(f)

    def __len__(self):
        return len(self.__struct_fields__)

    def __getitem__(self, key: int):
        return getattr(self, self.__struct_fields__[key])


class TimeData(Decodable):
    items: list[DataItem]

    def join(self, reff: dict[str, tuple[int, int, int]]):
        for item in self.items:
            for ts in item.timeSeries:
                pool_name = ts.metadata.attributes.poolName
                vcore, mem, max_apps = reff.get(pool_name, (None, None, None))
                metric_name = ts.metadata.metricName
                match metric_name:
                    case "allocated_memory_mb":
                        of_value = mem
                    case "allocated_vcores":
                        of_value = vcore
                    case "apps_running":
                        of_value = max_apps
                    case _:
                        of_value = None
                for data in ts.data:
                    yield TimeSeriesJoined(
                        data.timestamp,
                        pool_name,
                        metric_name,
                        data.value,
                        calc_perc(data.value, of_value),
                        data.aggregateStatistics.min,
                        calc_perc(data.aggregateStatistics.min, of_value),
                        data.aggregateStatistics.minTime,
                        data.aggregateStatistics.max,
                        calc_perc(data.aggregateStatistics.max, of_value),
                        data.aggregateStatistics.maxTime,
                        data.aggregateStatistics.count,
                    )
