import math

from cdp_metric_collector.cm_lib.structs import Decodable


class RangerResultPage(Decodable):
    startIndex: int
    pageSize: int
    totalCount: int
    resultSize: int

    @property
    def maxPage(self):
        return math.ceil(self.totalCount / self.pageSize)

    @property
    def currentPage(self):
        return math.ceil(self.currentIndex / self.pageSize)

    @property
    def currentIndex(self):
        return self.startIndex + self.resultSize

    @property
    def has_next(self):
        return self.resultSize >= self.pageSize and self.currentIndex < self.totalCount
