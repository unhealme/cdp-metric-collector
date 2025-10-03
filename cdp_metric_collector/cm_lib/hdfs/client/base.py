import builtins
import logging
import warnings
from io import BytesIO
from typing import Any, Literal, cast, overload
from xml.etree import ElementTree as ET

from msgspec import ValidationError
from requests import Session
from urllib3.exceptions import InsecureRequestWarning

from cdp_metric_collector.cm_lib.hdfs.structs import (
    ContentSummary,
    FileStatus,
    FileStatuses,
    FileStatusProperties,
    FileType,
    SparkListenerSQLExecutionStart,
)
from cdp_metric_collector.cm_lib.utils import ABC, wrap_async
from hdfs.util import HdfsError

TYPE_CHECKING = False
if TYPE_CHECKING:
    from urllib3 import HTTPResponse

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from hdfs.ext.kerberos import KerberosClient

logger = logging.getLogger(__name__)


warnings.filterwarnings("ignore", category=InsecureRequestWarning)


class HDFSClient(ABC):
    hdfs: KerberosClient

    def __init__(self, url: str | None = None):
        def find_name(root: "ET.ElementTree[Any]", path: str, name: str):
            for i in root.iterfind(path):
                match i.find("name"), i.find("value"):
                    case ET.Element() as iname, ET.Element() as value:
                        if iname.text == name:
                            return value.text or ""
            raise KeyError

        if url is None:
            logger.debug("no hdfs url is passed, trying to get url from hdfs-site.xml")
            hdfs_site = ET.parse("/etc/hadoop/conf/hdfs-site.xml")
            ns = find_name(hdfs_site, "property", "dfs.nameservices")
            nn = find_name(hdfs_site, "property", f"dfs.ha.namenodes.{ns}")
            url = ";".join(
                "https://"
                + find_name(
                    hdfs_site,
                    "property",
                    f"dfs.namenode.https-address.{ns}.{n}",
                )
                for n in nn.split(",")
            )
        session = Session()
        session.verify = False
        self.hdfs = KerberosClient(url, session=session)
        logger.debug("using %r as hdfs url", url)

    async def aread(self, path: str):
        with await wrap_async(self.hdfs.read, path) as f:
            f = cast("HTTPResponse", f)
            return BytesIO(await wrap_async(f.read))

    async def spark_sql(self, app_id: str):
        try:
            with await self.aread(f"/user/spark/applicationHistory/{app_id}") as buf:
                for i in buf:
                    try:
                        yield await wrap_async(
                            SparkListenerSQLExecutionStart.decode_json, i
                        )
                    except ValidationError:
                        pass
        except HdfsError as e:
            if "not found." not in e.message:
                raise
            logger.warning(e.message)

    def content(self, path: str):
        return ContentSummary.decode_json(
            self.hdfs._get_content_summary(path, strict=True).content  # type: ignore
        ).ContentSummary

    @overload
    def list(self, path: str) -> builtins.list[str]: ...
    @overload
    def list(
        self,
        path: str,
        status: Literal[True],
    ) -> builtins.list[tuple[str, FileStatusProperties]]: ...
    @overload
    def list(
        self,
        path: str,
        status: bool = False,
    ) -> builtins.list[str] | builtins.list[tuple[str, FileStatusProperties]]: ...
    def list(self, path: str, status: bool = False):
        full_path = self.hdfs.resolve(path)
        statuses = FileStatuses.decode_json(
            self.hdfs._list_status(full_path).content  # type: ignore
        ).FileStatuses.FileStatus
        match statuses:
            case [fs] if (
                not fs.pathSuffix or self.status(full_path).type is FileType.FILE
            ):
                err = f"{full_path} is not a directory"
                raise TypeError(err)
        if status:
            return [(f"{path}/{x.pathSuffix}", x) for x in statuses]
        return [f"{path}/{x.pathSuffix}" for x in statuses]

    def status(self, path: str):
        return FileStatus.decode_json(
            self.hdfs._get_file_status(path, strict=True).content  # type: ignore
        ).FileStatus
