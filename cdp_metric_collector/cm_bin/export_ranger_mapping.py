__version__ = "b2025.10.01-0"


import csv
import logging
import sys
from argparse import ArgumentParser
from collections import defaultdict
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAuth
from cdp_metric_collector.cm_lib.ranger import RangerClient
from cdp_metric_collector.cm_lib.utils import ARGSBase, parse_auth, setup_logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSBase):
    verbose: bool
    parser: ArgumentParser
    output: Path | int
    service: str
    filters: list[tuple[str, str]]
    auth_basic: tuple[str, str] | None
    auth_config: CMAuth | None


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
        data = await client.policies(service_type, index=index, **filters)
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
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
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
    async with RangerClient(config.RANGER_HOST, user, passw) as c:
        for fk, fv in set(args.filters):
            async for data in fetch_data(c, args.service, 0, **{fk: fv}):
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
