import argparse
import importlib
import sys

from . import __version__
from .cm_lib.utils import ABC

TYPE_CHECKING = False
if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import Any, TypeAlias

ModulesType: "TypeAlias" = "dict[str, Module] | dict[str, ModulesType]"


class Arguments(argparse.Namespace):
    cmdtree: list[str]

    def __setattr__(self, name: str, value: "Any", /):
        if name.endswith("commands"):
            try:
                self.cmdtree.append(value)
            except AttributeError:
                super().__setattr__("cmdtree", [value])
        else:
            super().__setattr__(name, value)

    def __getattr__(self, name: str) -> "Any":
        if name.endswith("commands"):
            return None
        return super().__getattribute__(name)


class Module(ABC):
    async_main: bool
    name: str
    prog: str

    def __init__(self, name: str, async_main: bool = True):
        self.name = name
        self.async_main = async_main


def create_parsers(
    sub: "argparse._SubParsersAction[argparse.ArgumentParser]",
    mods: ModulesType,
):
    for k, v in mods.items():
        if isinstance(v, Module):
            parser = sub.add_parser(k, add_help=False)
            v.prog = parser.prog
        else:
            parser = sub.add_parser(k)
            subparser = parser.add_subparsers(
                title="commands", dest=f"{k}_commands", required=True
            )
            create_parsers(subparser, v)


def parse(_args: "Sequence[str] | None" = None):
    modules: dict[str, Any] = {
        "auto": {
            "hdfs-rebalance": Module(".auto_hdfs_rebalance"),
            "yqm-config": Module(".auto_yqm_config"),
        },
        "export": {
            "cm": {
                "health-issues": Module(".export_cm_health_issues"),
                "hosts": Module(".export_cm_hosts"),
                "metrics": Module(".export_cm_metrics"),
                "users": Module(".export_cm_users"),
            },
            "hdfs": {
                "disk-failures": Module(".export_hdfs_disk_failures"),
                "usage-report": Module(".export_hdfs_usage_report"),
                "utilization": Module(".export_hdfs_utilization", async_main=False),
            },
            "hive": {
                "queries": Module(".export_hive_query"),
            },
            "ranger": {
                "audit-log": Module(".export_ranger_audit_log"),
                "last-access": Module(".export_ranger_audit_log"),
                "mappings": Module(".export_ranger_mapping"),
                "users": Module(".export_ranger_user"),
            },
            "spark-history": Module(".export_spark_history"),
            "yarn": {
                "queues": Module(".export_yarn_qm"),
                "pool-stats": Module(".export_yarn_pool_stats"),
            },
        },
    }
    main = argparse.ArgumentParser("cdp-metric-collector")
    main.add_argument(
        "-V",
        "--version",
        action="version",
        help="print version",
        version=f"%(prog)s {__version__}",
    )
    sub = main.add_subparsers(title="commands", dest="commands", required=True)
    create_parsers(sub, modules)
    args, rest = main.parse_known_args(_args, Arguments())
    mod = modules[args.cmdtree.pop(0)]
    for t in args.cmdtree:
        mod = mod[t]
    sys.exit(run(mod, rest))


def run(mod: Module, args: list[str]):
    module = importlib.import_module(mod.name, "cdp_metric_collector.cm_bin")
    setattr(module, "prog", mod.prog)
    main = getattr(module, "main")
    if mod.async_main:
        try:
            import uvloop as aio  # type: ignore
        except ImportError:
            import asyncio as aio

        aio.run(main(args))
    else:
        main(args)
