__version__ = "r2025.10.01-0"


import logging
import sys
from argparse import ArgumentParser
from contextlib import contextmanager
from functools import partial
from pathlib import Path

from msgspec import json

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import Creds
from cdp_metric_collector.cm_lib.ranger import RangerClient, RangerVXUsers
from cdp_metric_collector.cm_lib.utils import ARGSBase, parse_auth, setup_logging

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import IO

logger = logging.getLogger(__name__)
prog: str | None = None


class Arguments(ARGSBase):
    verbose: bool
    parser: ArgumentParser
    output: str | None
    diff: str | None
    auth_basic: tuple[str, str] | None
    auth_config: Creds | None


async def paginate(client: RangerClient):
    logger.debug("current page: %s / %s", 1, -1)
    r = await client.users(1, ["ROLE_SYS_ADMIN", "ROLE_USER", "ROLE_ADMIN_AUDITOR"])
    logger.debug("got %s records", r.resultSize)
    for i in r.vXUsers:
        yield i
    mp = r.maxPage
    p = r.currentPage + 1
    while r.has_next:
        logger.debug("current page: %s / %s", p, mp)
        index = r.currentIndex
        r = await client.users(
            1, ["ROLE_SYS_ADMIN", "ROLE_USER", "ROLE_ADMIN_AUDITOR"], p - 1, index=index
        )
        logger.debug("got %s records", r.resultSize)
        for i in r.vXUsers:
            yield i
        p += 1


def get_diff(base: list[RangerVXUsers], after: list[RangerVXUsers]):
    base_count = len(base)
    after_count = len(after)
    if (diff := after_count - base_count) > 0:
        diff = f"+{diff}"
    return base_count, after_count, diff


def get_diff_group(base: list[RangerVXUsers], after: list[RangerVXUsers], group: str):
    base_count = len([x for x in base if group in x.groupNameList])
    after_count = len([x for x in after if group in x.groupNameList])
    if (diff := after_count - base_count) > 0:
        diff = f"+{diff}"
    return base_count, after_count, diff


def get_changes(base: list[RangerVXUsers], after: list[RangerVXUsers]):
    base_users = {x.name for x in base}
    for user in after:
        if user.name not in base_users:
            yield "+ %s (added at %s)" % (
                user.name,
                user.createDate.strftime(r"%Y-%m-%d %H:%M:%S"),
            )
    for user in base_users.difference({x.name for x in after}):
        yield f"- {user}"


def get_modification(base: list[RangerVXUsers], after: list[RangerVXUsers]):
    base_users = {x.name: set(x.groupNameList) for x in base}
    for user in after:
        if (after_group := set(user.groupNameList)) != (
            base_group := base_users.get(user.name, set())
        ):
            yield f"### {user.name}"
            for group in after_group.difference(base_group):
                yield f"+ {group}"
            for group in base_group.difference(after_group):
                yield f"- {group}"


@contextmanager
def printer(file: "IO[str]"):
    yield partial(print, file=file)


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("parsed args %s", args)
    config.load_all()
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

    async with RangerClient(config.RANGER_HOST, user, passw) as c:
        base = [p async for p in paginate(c)]

    if args.diff:
        with open(args.output or sys.stdout.fileno(), "w", encoding="utf-8") as f:
            diff = json.decode(Path(args.diff).read_bytes(), type=list[RangerVXUsers])
            with printer(f) as print:
                print("Count All User : %s -> %s (%s)" % get_diff(base, diff))
                print(
                    "Count User in domain_users : %s -> %s (%s)"
                    % get_diff_group(base, diff, "domain_users")
                )
                print()
                print("New User:")
                for change in get_changes(base, diff):
                    print(change)
                print()
                print("Modifications:")
                for mod in get_modification(base, diff):
                    print(mod)
    else:
        with open(args.output or sys.stdout.fileno(), "wb") as f:
            f.write(json.encode(base))


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
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        dest="output",
    )
    parser.add_argument(
        "--diff",
        action="store",
        help="print diff from FILE",
        metavar="FILE",
        dest="diff",
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
