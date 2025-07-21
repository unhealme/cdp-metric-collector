__version__ = "u2025.06.26-0"


import argparse
import logging
from collections.abc import Sequence
from datetime import datetime
from enum import Enum
from pathlib import Path

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAPIClientBase, CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs import Decodable
from cdp_metric_collector.cm_lib.utils import (
    JSON_ENC,
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
)

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


class APICommand(Decodable):
    id: int
    name: str
    startTime: datetime
    endTime: datetime
    active: bool
    success: bool
    resultMessage: str

    def dump(self, fp: Path | str):
        Path(fp).write_bytes(JSON_ENC.encode(self))


class CommandClient(CMAPIClientBase):
    async def get_command(self, id: int):
        async with self._client.get(
            f"/api/v{config.CM_API_VER}/commands/{id}",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return APICommand.decode_json(await resp.read())

    async def rebalance_start(self):
        async with self._client.post(
            f"/api/v{config.CM_API_VER}/clusters/{config.CM_CLUSTER_NAME}"
            "/services/hdfs/commands/Rebalance",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return APICommand.decode_json(await resp.read())

    async def rebalance_stop(self, id: int):
        async with self._client.post(
            f"/api/v{config.CM_API_VER}/commands/{id}/abort",
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return APICommand.decode_json(await resp.read())


async def main(_args: Sequence[str] | None = None):
    args = parse_args(_args)
    setup_logging((logger, "cm_lib"), debug=args.verbose)
    logger.debug("got args %s", args)
    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")
    async with CommandClient(config.CM_HOST, auth) as client:
        match args.command:
            case CMD.START:
                rebalance = await client.rebalance_start()
                logger.info("started rebalance command with ID %s", rebalance.id)
            case CMD.STOP:
                if args.id is not None:
                    rebalance = await client.get_command(args.id)
                    if rebalance.name != "Rebalance":
                        args.parser.error(f"ID {args.id} is not a rebalance command")
                else:
                    last_status = APICommand.decode_json(
                        Path(config.HDFS_REBALANCE_STATUS).read_bytes()
                    )
                    rebalance = await client.get_command(last_status.id)
                if not rebalance.active:
                    logger.info(
                        "rebalance command with ID %s is already stoppped", rebalance.id
                    )
                rebalance = await client.rebalance_stop(rebalance.id)
                logger.info("rebalance command with ID %s stopped", rebalance.id)
        rebalance.dump(config.HDFS_REBALANCE_STATUS)


def parse_args(args: Sequence[str] | None = None):
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
