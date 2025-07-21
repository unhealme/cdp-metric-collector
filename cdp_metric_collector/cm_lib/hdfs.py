import logging
import warnings
from xml.etree import ElementTree as ET

from requests import Session
from urllib3.exceptions import InsecureRequestWarning

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    from hdfs.ext.kerberos import KerberosClient

TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


class HDFSClientBase(KerberosClient):
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
        super().__init__(url, session=session)
        logger.debug("using %r as hdfs url", url)
