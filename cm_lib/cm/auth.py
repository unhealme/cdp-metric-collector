from pathlib import Path

from cm_lib.structs import Decodable


class CMAuth(Decodable):
    session: str | None = None
    username: str = ""
    password: str = ""
    header: str | None = None

    @classmethod
    def from_path(cls, path: Path | str):
        return cls.decode_yaml(Path(path).read_bytes())
