from datetime import datetime

from cdp_metric_collector.cm_lib.structs import DTNoTZ

from . import RangerResultPage


class RangerVXUsers(DTNoTZ):
    id: int
    createDate: datetime
    updateDate: datetime
    name: str
    groupIdList: list[int]
    groupNameList: list[str]


class RangerUsers(RangerResultPage):
    vXUsers: list[RangerVXUsers]
