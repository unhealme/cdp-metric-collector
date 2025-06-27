__version__ = "b2025.06.26-0"


import csv
import logging
import sys
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path

from aiohttp import BasicAuth, ClientSession, ClientTimeout
from msgspec import Struct

from cm_lib import config
from cm_lib.cm import APIClientBase, CMAuth
from cm_lib.errors import HTTPNotOK
from cm_lib.structs import Decodable
from cm_lib.utils import (
    ARGSBase,
    encode_json_str,
    parse_auth,
    setup_logging,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

logger = logging.getLogger(__name__)


class Arguments(ARGSBase):
    verbose: bool
    parser: ArgumentParser
    output: Path | int
    service: str
    filters: list[tuple[str, str]]
    auth_basic: tuple[str, str] | None
    auth_config: CMAuth | None


class RangerPolicyItemAccess(Struct):
    type: str
    isAllowed: bool


class RangerPolicyItem(Struct):
    accesses: list[RangerPolicyItemAccess]
    users: list[str]
    groups: list[str]
    roles: list[str]


class RangerPolicyResource(Struct):
    values: list[str]
    isExcludes: bool
    isRecursive: bool


class RangerPolicy(Struct):
    isEnabled: bool
    resources: dict[str, RangerPolicyResource]
    policyItems: list[RangerPolicyItem]
    denyPolicyItems: list[RangerPolicyItem]
    allowExceptions: list[RangerPolicyItem]
    denyExceptions: list[RangerPolicyItem]


class RangerPolicyList(Decodable):
    startIndex: int
    pageSize: int
    totalCount: int
    resultSize: int
    policies: list[RangerPolicy]


class RangerClient(APIClientBase):
    base_url: str

    def __init__(self, base_url: str, user: str, passwd: str) -> None:
        self.base_url = base_url
        self._client = ClientSession(
            base_url,
            auth=BasicAuth(user, passwd),
            timeout=ClientTimeout(total=None),
        )

    async def get_policies(
        self,
        service_type: str,
        limit: int = 10000,
        index: int = 0,
        **filters: str,
    ):
        params = {
            "policyType": 0,
            "serviceType": service_type,
            "pageSize": limit,
            "startIndex": index,
            **filters,
        }
        logger.debug("sending data %s", encode_json_str(params))
        async with self._client.get(
            "/service/plugins/policies",
            params=params,
            ssl=False,
        ) as resp:
            logger.debug("sending GET request to %s", resp.url)
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(RangerPolicyList.decode_json, await resp.read())


async def fetch_data(
    client: RangerClient,
    service_type: str,
    index: int = 0,
    **filters: "Any",
):
    max = -1
    hasnext = True
    while hasnext:
        logger.debug("current index: %s / %s", index, max)
        data = await client.get_policies(service_type, index=index, **filters)
        logger.debug("got %s records", data.resultSize)
        for i in data.policies:
            yield i
        if max == -1:
            max = data.totalCount
        index = data.startIndex + data.resultSize
        hasnext = data.resultSize >= data.pageSize and index < max


Schema = str
Access = set[str]
Filter = str


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger,), debug=args.verbose)
    logger.debug("parsed args %s", args)
    if args.auth_config:
        user = args.auth_config.username
        passw = args.auth_config.password
    elif args.auth_basic:
        user, passw = args.auth_basic
    else:
        args.parser.error("No auth mechanism is passed")
    if not args.filters:
        args.parser.error("atleast '-U' or '-G' must be specified")
    result: defaultdict[Schema, defaultdict[Filter, Access]] = defaultdict(
        lambda: defaultdict(set)
    )
    config.load_all()
    async with RangerClient(config.RANGER_HOST, user, passw) as client:
        for fk, fv in set(args.filters):
            async for data in fetch_data(client, args.service, 0, **{fk: fv}):
                match data.resources:
                    case {"database": d, "column": _, "table": t}:
                        for db in d.values:
                            for tb in t.values:
                                for i in data.policyItems:
                                    if fv in i.users or fv in i.groups:
                                        result[f"{db}.{tb}"][fv].update(
                                            a.type for a in i.accesses
                                        )
                                for i in data.allowExceptions:
                                    if fv in i.users or fv in i.groups:
                                        result[f"{db}.{tb}"][fv].difference_update(
                                            a.type for a in i.accesses
                                        )
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        fw = csv.writer(f)
        schema = sorted(result)
        fw.writerow(("Entity", *schema))
        for _, entity in args.filters:
            fw.writerow((entity, *(",".join(result[s][entity]) for s in schema)))


def parse_filter(key: str, val: str):
    return [(key, x) for x in val.split(",")]


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(add_help=False)
    parser.set_defaults(parser=parser)
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
        "-U",
        "--user",
        action="extend",
        help="comma separated user NAME for filter",
        metavar="NAME",
        type=lambda v: parse_filter("user", v),
        default=[],
        dest="filters",
    )
    parser.add_argument(
        "-G",
        "--group",
        action="extend",
        help="comma separated group NAME for filter",
        metavar="NAME",
        type=lambda v: parse_filter("group", v),
        default=[],
        dest="filters",
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
        "--service",
        action="store",
        help="ranger Service Type to filter",
        metavar="NAME",
        type=str,
        required=True,
        dest="service",
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


def __main__():
    import asyncio

    asyncio.run(main())
