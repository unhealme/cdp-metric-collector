from msgspec import Struct

from cdp_metric_collector.cm_lib.structs import Decodable


class YARNApplication(Struct):
    id: str
    applicationTags: str

    @property
    def hive_query_id(self):
        for tag in self.applicationTags.split(","):
            if tag.startswith("hive_"):
                return tag
        err = f"no Hive query id found for application id {self.id}"
        raise KeyError(err)


class YARNApplicationResponse(Decodable):
    app: YARNApplication
