__version__ = "r2025.10.01-0"


import logging
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAuth, YQMCLient, YQMOperator, YQMQueueACL
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
    verbose: bool
    parser: ArgumentParser
    pool: str
    snapshot_path: Path
    users: list[YQMQueueACL]
    groups: list[YQMQueueACL]


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")

    async with YQMCLient(config.CM_HOST, auth) as c:
        snapshot = await c.get_config()
        snapshot.serialize_to_csv(
            args.snapshot_path / f"yqm_snapshot_{datetime.now().timestamp()}.csv"
        )
        last_state = ""
        for q in snapshot.queues:
            if q.queuePath == args.pool:
                last_state = q.properties.aclSubmit
                logger.info("using last state: %r", last_state)
        await c.update_config(args.pool, last_state, args.users, args.groups)


def parse_args(args: "Sequence[str] | None" = None):
    parser = ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=RawTextHelpFormatter,
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
    parser.add_argument(
        "--add-users",
        action="extend",
        metavar="USER",
        type=lambda s: [YQMQueueACL(x, YQMOperator.ADD) for x in s.split(",")],
        default=[],
        dest="users",
    )
    parser.add_argument(
        "--remove-users",
        action="extend",
        metavar="USER",
        type=lambda s: [YQMQueueACL(x, YQMOperator.REM) for x in s.split(",")],
        default=[],
        dest="users",
    )
    parser.add_argument(
        "--add-groups",
        action="extend",
        metavar="GROUP",
        type=lambda s: [YQMQueueACL(x, YQMOperator.ADD) for x in s.split(",")],
        default=[],
        dest="groups",
    )
    parser.add_argument(
        "--remove-groups",
        action="extend",
        metavar="GROUP",
        type=lambda s: [YQMQueueACL(x, YQMOperator.REM) for x in s.split(",")],
        default=[],
        dest="groups",
    )
    parser.add_argument(
        "--pool",
        "--queue",
        action="store",
        metavar="NAME",
        type=str,
        required=True,
        dest="pool",
    )
    parser.add_argument(
        "--snapshot-path",
        action="store",
        metavar="PATH",
        type=Path,
        default=Path.cwd(),
        dest="snapshot_path",
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
