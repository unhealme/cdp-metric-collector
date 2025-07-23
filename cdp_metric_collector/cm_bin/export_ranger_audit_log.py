__version__ = "u2025.06.27-0"


import csv
import logging
import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import date, datetime
from io import TextIOWrapper
from pathlib import Path
from typing import Generic, TypeVar, overload

from aiohttp import BasicAuth, ClientSession

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import APIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs import Decodable, DTNoTZ
from cdp_metric_collector.cm_lib.utils import (
    ARGSBase,
    encode_json_str,
    parse_auth,
    setup_logging,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None

_AT = TypeVar("_AT", Path | int, None)


class Arguments(ARGSBase, Generic[_AT]):
    verbose: bool
    parser: ArgumentParser
    output: _AT
    auth_basic: tuple[str, str] | None
    auth_config: CMAuth | None
    start_date: date
    end_date: date | None
    service_name: str


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


class RangerAccessAudit(Decodable):
    startIndex: int
    pageSize: int
    totalCount: int
    resultSize: int
    vXAccessAudits: list[RangerVXAccessAudits]

    def __iter__(self):
        yield from self.vXAccessAudits


class RangerClient(APIClientBase):
    base_url: str

    def __init__(self, base_url: str, user: str, passw: str) -> None:
        self.base_url = base_url
        self._client = ClientSession(base_url, auth=BasicAuth(user, passw))

    async def get_access_audit(
        self,
        start_date: date,
        end_date: date | None,
        service_name: str,
        limit: int = 10000,
        index: int = 0,
    ):
        data = {
            "startDate": start_date.strftime(r"%m/%d/%Y"),
            "repoName": service_name,
            "pageSize": limit,
            "startIndex": index,
            "excludeServiceUser": "false",
            "sortBy": "eventTime",
            "sortType": "desc",
        }
        if end_date:
            data["endDate"] = end_date.strftime(r"%m/%d/%Y")
        logger.debug("sending data %s", encode_json_str(data))
        async with self._client.get(
            "/service/assets/accessAudit",
            ssl=False,
            params=data,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(RangerAccessAudit.decode_json, await resp.read())


async def fetch_data(
    client: RangerClient,
    start_date: date,
    end_date: date | None,
    service_name: str,
):
    index = 0
    max = 1
    logger.debug("current index: %s / %s", index, max)
    while max > index:
        data = await client.get_access_audit(
            start_date,
            end_date,
            service_name,
            index=index,
        )
        logger.debug("got %s records", data.resultSize)
        if max == 1:
            max = data.totalCount
        index = index + data.resultSize
        logger.debug("current index: %s / %s", index, max)
        yield data.vXAccessAudits


@overload
async def fetch_audit_log(args: Arguments[Path | int]) -> None: ...
@overload
async def fetch_audit_log(args: Arguments[None]) -> list[tuple[str, str, datetime]]: ...
async def fetch_audit_log(args: Arguments[Path | int] | Arguments[None]):
    if args.auth_config:
        user = args.auth_config.username
        passw = args.auth_config.password
    elif args.auth_basic:
        user, passw = args.auth_basic
    else:
        args.parser.error("No auth mechanism is passed")
    async with RangerClient(config.RANGER_HOST, user, passw) as client:
        uid: set[str] = set()
        if args.output is None:
            result: list[tuple[str, str, datetime]] = []
            async for data in fetch_data(
                client,
                args.start_date,
                args.end_date,
                args.service_name,
            ):
                for i in data:
                    if i.requestUser not in uid:
                        result.append((i.requestUser, i.clientIP, i.eventTime))
                        uid.add(i.requestUser)
            return result
        else:
            with TextIOWrapper(
                open(args.output, "wb", 0),
                encoding="utf-8",
                newline="",
                write_through=True,
            ) as f:
                fw = csv.writer(f, delimiter="|")
                fw.writerow(("Name", "Client IP", "Last Access"))
                async for data in fetch_data(
                    client,
                    args.start_date,
                    args.end_date,
                    args.service_name,
                ):
                    for i in data:
                        if i.requestUser not in uid:
                            fw.writerow(
                                (
                                    i.requestUser,
                                    i.clientIP,
                                    i.eventTime.isoformat(" "),
                                )
                            )
                            uid.add(i.requestUser)


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)
    config.load_all()
    await fetch_audit_log(args)


def parse_args(args: "Sequence[str] | None" = None) -> Arguments[Path | int]:
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=RawTextHelpFormatter,
    )
    misc = parser.add_argument_group()
    misc.add_argument("-h", "--help", action="help", help="print this help and exit")
    misc.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable verbose mode",
        dest="verbose",
    )
    misc.add_argument(
        "--version",
        action="version",
        help="print version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=sys.stdout.fileno(),
        dest="output",
    )
    parser.add_argument(
        "--from",
        action="store",
        help="fetch audit logs from DATE (ISO format)",
        metavar="DATE",
        type=date.fromisoformat,
        required=True,
        dest="start_date",
    )
    parser.add_argument(
        "--to",
        action="store",
        help="fetch audit logs to DATE (ISO format)",
        metavar="DATE",
        type=date.fromisoformat,
        default=None,
        dest="end_date",
    )
    parser.add_argument(
        "--service",
        action="store",
        help="filter ranger Service Name to fetch",
        metavar="NAME",
        type=str,
        required=True,
        dest="service_name",
    )
    auth = parser.add_argument_group("authentication")
    auth.add_argument(
        "-c",
        "--config",
        action="store",
        help="authentication config file path",
        metavar="FILE",
        type=CMAuth.from_path,
        default=None,
        dest="auth_config",
    )
    auth.add_argument(
        "-u",
        action="store",
        metavar="USER:PASS",
        type=parse_auth,
        default=None,
        dest="auth_basic",
    )
    return parser.parse_args(args, Arguments())
