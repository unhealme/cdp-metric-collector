__version__ = "r2025.10.01-0"


import argparse
import logging
from enum import Enum
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import APICommand, CMAPIClient, CMAuth
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


class CMD(Enum):
    START = 0
    STOP = 1


class Arguments(ARGSWithAuthBase):
    verbose: bool
    parser: argparse.ArgumentParser
    command: CMD
    id: int | None


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)
    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")
    async with CMAPIClient(config.CM_HOST, auth) as c:
        match args.command:
            case CMD.START:
                rebalance = await c.rebalance_start()
                logger.info("started rebalance command with ID %s", rebalance.id)
            case CMD.STOP:
                if args.id is not None:
                    rebalance = await c.command(args.id)
                    if rebalance.name != "Rebalance":
                        args.parser.error(f"ID {args.id} is not a rebalance command")
                else:
                    last_status = APICommand.decode_json(
                        Path(config.HDFS_REBALANCE_STATUS).read_bytes()
                    )
                    rebalance = await c.command(last_status.id)
                if not rebalance.active:
                    logger.info(
                        "rebalance command with ID %s is already stoppped", rebalance.id
                    )
                else:
                    rebalance = await c.rebalance_stop(rebalance.id)
                    logger.info("rebalance command with ID %s stopped", rebalance.id)
        rebalance.dump(config.HDFS_REBALANCE_STATUS)


def parse_args(args: "Sequence[str] | None" = None):
    parser = argparse.ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
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
    subparser = parser.add_subparsers(required=True, metavar="command")
    start = subparser.add_parser("start", help="start rebalance command")
    start.set_defaults(command=CMD.START, parser=start)
    stop = subparser.add_parser("stop", help="stop running rebalance command")
    stop.set_defaults(command=CMD.STOP, parser=stop)
    stop.add_argument(
        "id",
        action="store",
        metavar="ID",
        help="stop rebalance command with ID",
        type=int,
        nargs="?",
    )
    return parser.parse_args(args, Arguments())
