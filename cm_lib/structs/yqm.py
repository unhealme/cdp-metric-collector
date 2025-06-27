import csv
from io import TextIOWrapper

from msgspec import Struct, field

from ._abc import Decodable

TYPE_CHECKING = False
if TYPE_CHECKING:
    from pathlib import Path


class YQMConfigProp(Struct):
    name: str
    value: str


class YQMConfigPayload(Struct):
    properties: list[YQMConfigProp]
    message: str


class YQCapacityResource(Struct):
    memory: str = field(name="memory-mb")
    vcores: str


class YQCapacity(Struct):
    percentage: str
    resource: YQCapacityResource


class YQProperties(Struct):
    aclAdmin: str = field(name="queueAcls.ADMINISTER_QUEUE")
    aclSubmit: str = field(name="queueAcls.SUBMIT_APP")
    userLimit: str = ""
    userLimitFactor: str = ""
    maxApplications: str = ""
    configuredMaxAMResourceLimit: str = ""


class YarnQueue(Struct):
    name: str
    queuePath: str
    capacity: YQCapacity
    maxCapacity: YQCapacity
    properties: YQProperties
    effectiveMinResource: YQCapacityResource
    effectiveMaxResource: YQCapacityResource
    state: str = ""

    def __iter__(self):
        yield self.queuePath
        names = self.queuePath.split(".")
        names.extend(["", ""])
        yield names[0]
        yield names[1]
        yield names[2]
        yield self.capacity.percentage
        yield "%s VCores, %s MB" % (
            self.effectiveMinResource.vcores,
            self.effectiveMinResource.memory,
        )
        yield self.maxCapacity.percentage
        yield "%s VCores, %s MB" % (
            self.effectiveMaxResource.vcores,
            self.effectiveMaxResource.memory,
        )
        try:
            am_resource_limit = float(self.properties.configuredMaxAMResourceLimit)
            yield f"{am_resource_limit:.0%}"
            yield "%s VCores, %s MB" % (
                round(int(self.effectiveMaxResource.vcores) * am_resource_limit),
                round(int(self.effectiveMaxResource.memory) * am_resource_limit),
            )
        except ValueError:
            yield ""
            yield ""
        yield self.state
        yield self.properties.userLimit
        yield self.properties.userLimitFactor
        yield self.properties.maxApplications
        submit_acl = self.properties.aclSubmit.partition(" ")
        yield submit_acl[0]
        yield submit_acl[2]


class YarnQMResponse(Decodable):
    queues: list[YarnQueue]

    def serialize_to_csv(self, output: "Path | int"):
        with TextIOWrapper(
            open(output, "wb", 0),
            encoding="utf-8",
            newline="",
            write_through=True,
        ) as fo:
            fw = csv.writer(fo, delimiter="|")
            fw.writerow(
                (
                    "Name",
                    "Level 1",
                    "Level 2",
                    "Level 3",
                    "Capacity",
                    "Effective Capacity",
                    "Max Capacity",
                    "Effective Max Capacity",
                    "Max AM Resource Limit",
                    "Effective Max AM Resource Limit",
                    "State",
                    "User Limit",
                    "User Limit Factor",
                    "Max Applications",
                    "Submit ACL User",
                    "Submit ACL Group",
                )
            )
            fw.writerows(self.queues)
