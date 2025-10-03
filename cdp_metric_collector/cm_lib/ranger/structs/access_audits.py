from datetime import datetime

from cdp_metric_collector.cm_lib.structs import DTNoTZ

from . import RangerResultPage


class RangerVXAccessAudits(DTNoTZ):
    id: int
    accessResult: int
    accessType: str
    agentId: str
    clientIP: str
    policyId: int
    repoName: str
    repoDisplayName: str
    repoType: int
    serviceType: str
    serviceTypeDisplayName: str
    sessionId: str
    eventTime: datetime
    requestUser: str
    action: str
    requestData: str
    resourcePath: str
    resourceType: str
    eventCount: int
    eventDuration: int
    clusterName: str
    agentHost: str
    policyVersion: int
    eventId: str

    def __iter__(self):
        for f in self.__struct_fields__:
            match getattr(self, f):
                case str(v):
                    yield v
                case datetime() as v:
                    yield v.isoformat(" ")
                case other:
                    yield str(other)


class RangerAccessAudit(RangerResultPage):
    vXAccessAudits: list[RangerVXAccessAudits]

    def __iter__(self):
        yield from self.vXAccessAudits
