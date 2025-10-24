import shlex
from datetime import datetime, timedelta

from msgspec import Struct

from cdp_metric_collector.cm_lib.structs import Decodable


class ApplicationAttempt(Struct):
    sparkUser: str
    duration: int
    completed: bool
    startTimeEpoch: int
    endTimeEpoch: int
    appSparkVersion: str

    @property
    def appSparkMajorVersion(self):
        return ".".join(self.appSparkVersion.split(".")[:3])

    @property
    def startTime(self):
        return datetime.fromtimestamp(self.startTimeEpoch / 1000)

    @property
    def endTime(self):
        return datetime.fromtimestamp(self.endTimeEpoch / 1000)

    @property
    def durationParsed(self):
        return str(timedelta(seconds=self.duration / 1000))


class SparkApplication(Struct):
    id: str
    name: str
    attempts: list[ApplicationAttempt]


class SparkProperties(Struct, array_like=True):
    name: str
    value: str

    def as_bool(self):
        return self.value.lower() == "true"

    def as_int(self):
        return int(self.value, 10)

    def as_float(self):
        return float(self.value)

    def as_params(self):
        return shlex.split(self.value)


class ApplicationEnvironment(Decodable):
    sparkProperties: list[SparkProperties]

    @classmethod
    def new(cls):
        return cls([])

    def get_yarn_queue(self):
        for p in self.sparkProperties:
            if p.name == "spark.yarn.queue":
                return p.value
        return ""
