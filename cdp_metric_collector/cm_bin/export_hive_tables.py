__version__ = "u2025.04.14-0"


import sys
from argparse import ArgumentParser, RawTextHelpFormatter
from enum import Enum
from pathlib import Path

from cdp_metric_collector.cm_lib.utils import ARGSBase, setup_logging


class Columns(Enum):
    DB = "Database"
    DDL = "DDL"
    DESC = "Desc"
    FILES = "Files"
    FP = "Partitions"
    L1P = "L1 Partitions"
    LOC = "Location"
    PCOLS = "Partition Columns"
    PCOUNT = "Partition Count"
    PSIZE = "PSize"
    SIZE = "Size"
    TABLE = "Table"
    TYPE = "Type"

    def __str__(self):
        return f"{self.__class__}.{self.name}"


class Arguments(ARGSBase):
    verbose: bool
    hive_url: str | None
    output: Path | int


def parse_args():
    def parse_columns(cols: str):
        return [Columns[x.strip().upper()] for x in cols.split(",")]

    parser = ArgumentParser(
        add_help=False,
        formatter_class=RawTextHelpFormatter,
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

    parser.add_argument(
        "files",
        action="store",
        help="input file",
        metavar="FILE",
        type=Path,
    )
    parser.add_argument(
        "-F",
        "--fields",
        type=parse_columns,
        help="""specifies the output format
FORMAT is comma-separated list of NAME with available NAMEs:
- db      : database name
- ddl     : table ddl
- desc    : table desc
- fp      : full partition of table
- l1p     : level 1 partition of table
- loc     : hdfs path of table
- numfile : count of table files
- pcols   : partition columns if any
- pcount  : partition count
- size    : table data size in bytes
- table   : table name
- type    : table type
default: 'db,table,ddl,desc'
""",
        metavar="FORMAT",
        default=[Columns.DB, Columns.TABLE, Columns.DDL, Columns.DESC],
        dest="columns",
    )
    parser.add_argument(
        "-o",
        type=Path,
        help="dump result to FILE instead of stdout",
        metavar="FILE",
        default=sys.stdout.fileno(),
        dest="output",
    )
    parser.add_argument(
        "-u",
        action="store",
        metavar="HIVE_URL",
        default=None,
        dest="hive_url",
    )
    parser.add_argument(
        "--no-pattern",
        action="store_true",
        dest="no_pattern",
        help="parse table as is without pattern matching",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="dump_json",
        help="dump result as json (this enables all output columns)",
    )
    return parser.parse_args(namespace=Arguments())
