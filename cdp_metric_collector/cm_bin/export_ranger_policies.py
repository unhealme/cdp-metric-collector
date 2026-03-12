__version__ = "r2026.03.12-0"


import csv
import logging
import sys
from argparse import ArgumentParser
from itertools import chain
from pathlib import Path

from msgspec import json

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import Creds
from cdp_metric_collector.cm_lib.ranger import RangerClient, RangerPolicyList
from cdp_metric_collector.cm_lib.utils import ARGSBase, parse_auth, setup_logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence


logger = logging.getLogger(__name__)
prog: str | None = None

HeaderField = (
    "Service",
    "Resource Type",
    "Resource",
    "Entity Type",
    "Entity",
    "Mode",
    "Access",
)


class Arguments(ARGSBase):
    verbose: bool
    parser: ArgumentParser
    from_file: str | None
    output: str | None
    serialize: bool
    auth_basic: tuple[str, str] | None
    auth_config: Creds | None


async def fetch_data(client: RangerClient):
    services = await client.services()
    return await client.policies_export(
        [x.name for x in services.services],
        checkPoliciesExists="true",
    )


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("parsed args %s", args)
    if args.from_file:
        data = RangerPolicyList.decode_json(Path(args.from_file).read_bytes())
        args.serialize = True
    else:
        if args.auth_config:
            user = args.auth_config.username
            passw = args.auth_config.password
        elif args.auth_basic:
            user, passw = args.auth_basic
        else:
            if not config.CM_AUTH:
                args.parser.error("No auth mechanism is passed")
            user = config.CM_AUTH.username
            passw = config.CM_AUTH.password
        config.load_all()
        async with RangerClient(config.RANGER_HOST, user, passw) as c:
            data = await fetch_data(c)
    if args.serialize:
        with open(
            args.output or sys.stdout.fileno(), "w", encoding="utf-8", newline=""
        ) as f:
            fw = csv.writer(f)
            fw.writerow(HeaderField)
            for p in data.policies:
                st = p.serviceType
                r = p.decode_resource()
                rt = r.type
                for rv in r.format_values():
                    for i in p.policyItems:
                        for a in i.accesses:
                            access = "Allow" if a.isAllowed else "Deny"
                            for e, et in chain(
                                [(x, "user") for x in i.users],
                                [(x, "group") for x in i.groups],
                                [(x, "role") for x in i.roles],
                            ):
                                fw.writerow((st, rt, rv, et, e, a.type, access))
    else:
        with open(args.output or sys.stdout.fileno(), "wb") as f:
            f.write(json.encode(data))


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
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        dest="output",
    )
    parser.add_argument(
        "--from-file",
        action="store",
        help="use FILE as ranger policies. implies '--serialize'",
        metavar="FILE",
        dest="from_file",
    )
    parser.add_argument(
        "--serialize",
        action="store_true",
        help="serialize output to csv (default: json)",
        dest="serialize",
    )
    auth = parser.add_argument_group("authentication")
    auth.add_argument(
        "-c",
        "--config",
        action="store",
        help="authentication config file path",
        metavar="FILE",
        type=Creds.from_path,
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
