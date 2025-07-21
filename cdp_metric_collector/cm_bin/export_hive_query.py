__version__ = "b2025.06.26-0"


import argparse
import asyncio
import csv
import logging
import sys
from datetime import datetime
from enum import Enum
from io import TextIOWrapper
from pathlib import Path

from aiohttp import ClientSession

from cdp_metric_collector.cm_lib import config
from cdp_metric_collector.cm_lib.cm import APIClientBase
from cdp_metric_collector.cm_lib.errors import HTTPNotOK
from cdp_metric_collector.cm_lib.kerberos import KerberosClientBase
from cdp_metric_collector.cm_lib.structs.hive import (
    DagInfoData,
    QueryExtendedInfo,
    QueryInfo,
    QuerySearchResult,
)
from cdp_metric_collector.cm_lib.structs.yrm import YARNApplicationResponse
from cdp_metric_collector.cm_lib.utils import (
    ARGSBase,
    encode_json_str,
    pretty_size,
    setup_logging,
    strfdelta,
    wrap_async,
)

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Awaitable, Iterable, Sequence
    from typing import Any

logger = logging.getLogger(__name__)
prog: str | None = None


class CMD(Enum):
    DETAIL = 0
    HISTORY = 1


class Arguments(ARGSBase):
    command: CMD
    verbose: bool
    output: Path | int
    start_time: datetime
    end_time: datetime
    all_history: bool
    query_id: str


class HUEQPClient(APIClientBase):
    base_url: str

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self._client = ClientSession(
            base_url,
            json_serialize=encode_json_str,
        )

    async def initialize(self):
        self._client.headers.update({"x-do-as": config.HUE_USER})

    async def query_detail(self, query_id: str):
        async with self._client.get(
            "/api/hive/query",
            ssl=False,
            params={
                "queryId": query_id,
                "extended": "true",
            },
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(QueryExtendedInfo.decode_json, await resp.read())

    async def search_query(
        self,
        starttime: int,
        endtime: int,
        limit: int = 100,
        offset: int = 0,
        text: str = "",
    ):
        async with self._client.post(
            "/api/query/search",
            ssl=False,
            json={
                "search": {
                    "endTime": endtime,
                    "limit": limit,
                    "offset": offset,
                    "facets": [],
                    "text": text,
                    "sortText": "startTime:DESC",
                    "startTime": starttime,
                    "type": "BASIC",
                }
            },
        ) as resp:
            if resp.status >= 400:
                logger.error(
                    "got response code %s with header: %s",
                    resp.status,
                    resp.headers,
                )
                raise HTTPNotOK(await resp.text())
            return await wrap_async(QuerySearchResult.decode_json, await resp.read())


class YARNRMClient(KerberosClientBase):
    rm_hosts: list[str]

    def __init__(self, urls: list[str]):
        super().__init__("")
        self.rm_hosts = urls

    async def get_application(self, appid: str):
        body = b""
        for n, host in enumerate(self.rm_hosts):
            async with self._client.stream(
                "GET", f"{host}/ws/v1/cluster/apps/{appid}"
            ) as resp:
                body = await resp.aread()
                if resp.status_code >= 400:
                    logger.error(
                        "got response code %s with header: %s using host: %s",
                        resp.status_code,
                        resp.headers,
                        host,
                    )
                    continue
                if n > 0:
                    self.rm_hosts.insert(0, self.rm_hosts.pop(n))
                return await wrap_async(YARNApplicationResponse.decode_json, body)
        raise HTTPNotOK(body.decode())


async def get_row(
    client: HUEQPClient,
    q: QueryInfo,
    dag_info: DagInfoData | None,
    expand: bool = True,
) -> tuple["Any", ...]:
    if dag_info:
        if expand:
            qe = (await client.query_detail(q.queryId)).query
            start, end, elapsed = qe.duration()
            return (
                q.queryId,
                dag_info.applicationId,
                start.isoformat(" "),
                end.isoformat(" ") if end else "-",
                qe.requestUser,
                qe.queueName,
                qe.status,
                qe.details.get_config(),
                strfdelta(
                    elapsed,
                    "%(days)dd %(hours)02dh:%(minutes)02dm:%(seconds)02ds",
                ),
                qe.query,
                len(qe.query or ""),
                "" if qe.dataRead is None else pretty_size(qe.dataRead),
                "" if qe.dataWritten is None else pretty_size(qe.dataWritten),
                ", ".join(map(str, qe.tablesRead)),
                ", ".join(map(str, qe.tablesWritten)),
                qe.usedCBO,
            )
        else:
            start, end, elapsed = q.duration()
            return (
                q.queryId,
                dag_info.applicationId,
                start.isoformat(" "),
                end.isoformat(" ") if end else "-",
                "",
                dag_info.queueName,
                dag_info.status,
                "",
                strfdelta(
                    elapsed,
                    "%(days)dd %(hours)02dh:%(minutes)02dm:%(seconds)02ds",
                ),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )
    else:
        if expand:
            qe = (await client.query_detail(q.queryId)).query
            start, end, elapsed = qe.duration()
            return (
                q.queryId,
                "",
                start.isoformat(" "),
                end.isoformat(" ") if end else "-",
                qe.requestUser,
                qe.queueName,
                qe.status,
                qe.details.get_config(),
                strfdelta(
                    elapsed,
                    "%(days)dd %(hours)02dh:%(minutes)02dm:%(seconds)02ds",
                ),
                qe.query,
                len(qe.query or ""),
                "" if qe.dataRead is None else pretty_size(qe.dataRead),
                "" if qe.dataWritten is None else pretty_size(qe.dataWritten),
                ", ".join(map(str, qe.tablesRead)),
                ", ".join(map(str, qe.tablesWritten)),
                qe.usedCBO,
            )
        else:
            start, end, elapsed = q.duration()
            return (
                q.queryId,
                "",
                start.isoformat(" "),
                end.isoformat(" ") if end else "-",
                "",
                "",
                "",
                "",
                strfdelta(
                    elapsed,
                    "%(days)dd %(hours)02dh:%(minutes)02dm:%(seconds)02ds",
                ),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            )


async def export_data(
    client: HUEQPClient,
    start_time: datetime,
    end_time: datetime,
    all_history: bool,
):
    async def fetch_data():
        offset = 0
        qlen = -1
        while qlen != 0:
            q = await client.search_query(stq, etq, offset=max(0, offset + qlen))
            qlen = len(q.queries)
            logger.debug("fetched %s queries", qlen)
            offset = offset + qlen
            yield q.queries

    stq = int(start_time.timestamp() * 1000)
    etq = int(end_time.timestamp() * 1000)
    tasks: list[Awaitable[Iterable[Any]]] = []
    async for queries in fetch_data():
        for q in queries:
            try:
                dag_info = next(x.dagInfo for x in q.dags)
                tasks.append(get_row(client, q, dag_info))
            except StopIteration:
                if all_history:
                    tasks.append(get_row(client, q, None))
                else:
                    continue
        yield await asyncio.gather(*tasks)
        tasks.clear()


async def main(_args: "Sequence[str] | None" = None):
    args = parse_args(_args)
    setup_logging((logger,), debug=args.verbose)
    logger.debug("got args %s", args)
    config.load_all()
    async with HUEQPClient(config.HUEQP_HOST) as client:
        match args.command:
            case CMD.DETAIL:
                if args.query_id.startswith("application_"):
                    logger.debug(
                        "trying to get query id for application id %s", args.query_id
                    )
                    async with YARNRMClient(config.YARN_RM_HOST) as yarn:
                        query_id = (
                            await yarn.get_application(args.query_id)
                        ).app.hive_query_id
                else:
                    query_id = args.query_id
                q = (await client.query_detail(query_id)).query
                start, end, elapsed = q.duration()
                appid = next((x.dagInfo.applicationId for x in q.dags), "")
                print(
                    "\n".join(
                        (
                            f"Application ID: {appid}",
                            f"Query ID: {query_id}",
                            "Start: %s" % start.isoformat(" "),
                            "End: %s" % (end.isoformat(" ") if end else "-"),
                            f"User: {q.requestUser}",
                            f"Queue: {q.queueName}",
                            f"Status: {q.status}",
                            "Elapsed: %s"
                            % (
                                strfdelta(
                                    elapsed,
                                    "%(days)dd %(hours)02dh:%(minutes)02dm:%(seconds)02ds",
                                )
                            ),
                            "Data Read: %s"
                            % ("" if q.dataRead is None else pretty_size(q.dataRead)),
                            "Data Written: %s"
                            % (
                                ""
                                if q.dataWritten is None
                                else pretty_size(q.dataWritten)
                            ),
                            "Tables Read: %s"
                            % ", ".join([str(t) for t in q.tablesRead]),
                            "Tables Written: %s"
                            % ", ".join([str(t) for t in q.tablesWritten]),
                            f"CBO Enabled: {q.usedCBO}",
                            "Query Length: %s" % len(q.query or ""),
                            f"Query: {q.query}",
                        )
                    )
                )
            case CMD.HISTORY:
                csv.field_size_limit(sys.maxsize)
                with TextIOWrapper(
                    open(args.output, "wb", 0),
                    encoding="utf-8",
                    newline="",
                    write_through=True,
                ) as f:
                    fw = csv.writer(f)
                    fw.writerow(
                        (
                            "Query ID",
                            "Application ID",
                            "Start Time",
                            "End Time",
                            "User",
                            "Queue",
                            "Status",
                            "Config",
                            "Elapsed Time",
                            "Query",
                            "Query Length",
                            "Data Read",
                            "Data Written",
                            "Tables Read",
                            "Tables Written",
                            "CBO Enabled",
                        )
                    )
                    async for rows in export_data(
                        client,
                        args.start_time,
                        args.end_time,
                        args.all_history,
                    ):
                        fw.writerows(rows)


def parse_args(args: "Sequence[str] | None" = None):
    parser = argparse.ArgumentParser(
        prog=prog,
        add_help=False,
        formatter_class=argparse.RawTextHelpFormatter,
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
    subparser = parser.add_subparsers(required=True, metavar="command")
    history = subparser.add_parser("history", help="export hive query history")
    history.set_defaults(command=CMD.HISTORY)
    history.add_argument(
        "-s",
        "--start-time",
        action="store",
        help="export data from TIME (ISO format, today if unset)",
        metavar="TIME",
        type=datetime.fromisoformat,
        default=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
        dest="start_time",
    )
    history.add_argument(
        "-e",
        "--end-time",
        action="store",
        help="export data to TIME (ISO format, now if unset)",
        metavar="TIME",
        type=datetime.fromisoformat,
        default=datetime.now(),
        dest="end_time",
    )
    history.add_argument(
        "--all",
        action="store_true",
        help="export all query even if no application id found",
        dest="all_history",
    )
    history.add_argument(
        "-o",
        action="store",
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        type=Path,
        default=sys.stdout.fileno(),
        dest="output",
    )
    detail = subparser.add_parser("detail", help="get query detail for hive query id")
    detail.set_defaults(command=CMD.DETAIL)
    detail.add_argument(
        "query_id", action="store", metavar="APPLICATION_ID|QUERY_ID", type=str
    )
    return parser.parse_args(args, Arguments())
