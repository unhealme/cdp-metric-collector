__version__ = "r2025.10.10-0"


import csv
import logging
import sys
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import AuthRoles, CMAPIClient, CMAuth
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSWithAuthBase):
    parser: ArgumentParser
    verbose: bool
    output: Path | int
    diff_file: Path | None
    roles_file: Path | None


async def fetch_roles(auth: CMAuth):
    async with CMAPIClient(config.CM_HOST, auth) as c:
        return await c.roles()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    match args.get_auth(), args.roles_file:
        case CMAuth() as auth, None:
            roles = await fetch_roles(auth)
        case _, Path() as hosts_file:
            roles = AuthRoles.decode_json(hosts_file.read_bytes())
        case _:
            args.parser.error("No auth mechanism is passed")

    diff_roles = None
    if args.diff_file:
        diff_roles = {
            x.displayName: {u.name for u in x.users}
            for x in AuthRoles.decode_json(args.diff_file.read_bytes()).items
        }

    with open(args.output, "w", encoding="utf-8", newline="") as outf:
        fw = csv.writer(outf)
        fw.writerow(("Role", "Count", "Changes") if diff_roles else ("Role", "Count"))
        for role in roles.items:
            if diff_roles:
                deletion = [
                    f"-{x}"
                    for x in diff_roles[role.displayName].difference(
                        x.name for x in role.users
                    )
                ]
                addition = [
                    f"+{x}"
                    for x in {x.name for x in role.users}.difference(
                        diff_roles[role.displayName]
                    )
                ]
                fw.writerow(
                    (
                        role.displayName,
                        str(len(role.users)),
                        ", ".join(addition + deletion),
                    )
                )
            else:
                fw.writerow((role.displayName, str(len(role.users))))


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
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
        "--diff-file",
        action="store",
        help="show the difference from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="diff_file",
    )
    parser.add_argument(
        "--roles-file",
        action="store",
        help="load roles from FILE",
        metavar="FILE",
        type=Path,
        default=None,
        dest="roles_file",
    )
    auth.add_argument(
        "-u",
        action="store",
        metavar="USER:PASS",
        type=parse_auth,
        default=None,
        dest="auth_basic",
    )
    auth.add_argument(
        "-s",
        action="store",
        metavar="SESSION_ID",
        type=str,
        default=None,
        dest="auth_session",
    )
    auth.add_argument(
        "-t",
        action="store",
        metavar="BASE64_TOKEN",
        type=str,
        default=None,
        dest="auth_header",
    )
    return parser.parse_args(args, Arguments())
