__version__ = "r2025.06.26-0"


import logging
from argparse import ArgumentParser, RawTextHelpFormatter
from datetime import datetime
from enum import Enum
from pathlib import Path

from msgspec import Struct

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import CMAuth
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.structs.yqm import YQMConfigPayload, YQMConfigProp
from cdp_metric_collector.cm_lib.utils import (
    ARGSWithAuthBase,
    parse_auth,
    setup_logging,
)

from .export_yarn_qm import YQMCLient as _YQMCLient

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
    users: list["QueueACL"]
    groups: list["QueueACL"]


class Operator(Enum):
    ADD = 0
    REM = 1


class QueueACL(Struct):
    name: str
    op: Operator


class YQMCLient(_YQMCLient):
    async def put_config(
        self,
        pool: str,
        last_state: str,
        users: list[QueueACL],
        groups: list[QueueACL],
    ):
        user, _, group = last_state.partition(" ")
        acls = "%s %s" % (
            ",".join(parse_acl([u for u in user.split(",") if u], users)),
            ",".join(parse_acl([g for g in group.split(",") if g], groups)),
        )
        if acls == last_state:
            logger.info("no changes to update from last state")
            return
        logger.info("setting acls %r to pool %s", acls, pool)
        payload = YQMConfigPayload(
            [
                YQMConfigProp("acl_submit_applications", acls),
                YQMConfigProp("acl_administer_queue", acls),
            ],
            f"Changed properties of {pool} by automation",
        )
        logger.debug("sending payload %s", payload)
        async with self._client.put(
            f"/cmf/clusters/{config.CM_CLUSTER_NAME}/queue-manager-api/api/v1/environments/dev"
            f"/clusters/{config.CM_CLUSTER_NAME}/resources/scheduler/partitions/default/queues/{pool}",
            json=payload,
            headers={"Content-Type": "application/json"},
            ssl=False,
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())


def parse_acl(last: list[str], acls: list[QueueACL]):
    for acl in acls:
        match acl:
            case QueueACL(op=Operator.ADD, name=name):
                if name not in last:
                    last.append(name)
            case QueueACL(op=Operator.REM, name=name):
                try:
                    last.remove(name)
                except ValueError:
                    pass
    return last


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging(("cdp_metric_collector",), debug=args.verbose)
    logger.debug("got args %s", args)

    config.load_all()
    auth = args.get_auth()
    if not auth:
        args.parser.error("No auth mechanism is passed")

    async with YQMCLient(config.CM_HOST, auth) as client:
        snapshot = await client.get_data()
        snapshot.serialize_to_csv(
            args.snapshot_path / f"yqm_snapshot_{datetime.now().timestamp()}.csv"
        )
        last_state = ""
        for q in snapshot.queues:
            if q.queuePath == args.pool:
                last_state = q.properties.aclSubmit
                logger.info("using last state: %r", last_state)
        await client.put_config(args.pool, last_state, args.users, args.groups)


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
        type=lambda s: [QueueACL(x, Operator.ADD) for x in s.split(",")],
        default=[],
        dest="users",
    )
    parser.add_argument(
        "--remove-users",
        action="extend",
        metavar="USER",
        type=lambda s: [QueueACL(x, Operator.REM) for x in s.split(",")],
        default=[],
        dest="users",
    )
    parser.add_argument(
        "--add-groups",
        action="extend",
        metavar="GROUP",
        type=lambda s: [QueueACL(x, Operator.ADD) for x in s.split(",")],
        default=[],
        dest="groups",
    )
    parser.add_argument(
        "--remove-groups",
        action="extend",
        metavar="GROUP",
        type=lambda s: [QueueACL(x, Operator.REM) for x in s.split(",")],
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
