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
    perc_value: str
    min: float
    perc_min: str
    at_min: datetime
    max: float
    perc_max: str
    at_max: datetime
    aggregations: int

    def __iter__(self):
        yield self.timestamp.isoformat(" ", "milliseconds")
        yield self.pool
        yield self.metric
        yield str(self.value)
        yield self.perc_value
        yield str(self.min)
        yield self.perc_min
        yield self.at_min.isoformat(" ", "milliseconds")
        yield str(self.max)
        yield self.perc_max
        yield self.at_max.isoformat(" ", "milliseconds")
        yield str(self.aggregations)

    def __len__(self):
        return len(self.__struct_fields__)

    def __getitem__(self, key: int):
        return getattr(self, self.__struct_fields__[key])


class TimeData(Decodable):
    items: list[DataItem]

    def join(self, reff: dict[str, tuple[int, int]]):
        for item in self.items:
            for ts in item.timeSeries:
                pool_name = ts.metadata.attributes.poolName
                vcore, mem = reff.get(pool_name, (None, None))
                metric_name = ts.metadata.metricName
                match metric_name:
                    case "allocated_memory_mb":
                        of_value = mem
                    case "allocated_vcores":
                        of_value = vcore
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
