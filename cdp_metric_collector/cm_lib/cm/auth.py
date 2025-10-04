from pathlib import Path

from msgspec import Struct

from cdp_metric_collector.cm_lib.structs import Decodable


class Creds(Decodable):
    session: str | None = None
    username: str = ""
    password: str = ""
    header: str | None = None

    @classmethod
    def from_path(cls, path: str):
        return cls.decode_yaml(Path(path).read_bytes())


class CMAuth(Struct):
    creds: Creds
    path: str | None = None

    @classmethod
    def from_path(cls, path: str):
        creds = Creds.from_path(path)
        return cls(creds, path)
