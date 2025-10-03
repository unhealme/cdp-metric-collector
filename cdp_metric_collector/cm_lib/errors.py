TYPE_CHECKING = False
if TYPE_CHECKING:
    from typing import Any


class HTTPNotOK(ValueError):
    def __init__(self, status: int, header: "Any", page: str) -> None:
        super().__init__(page)
        self.status = status
        self.header = header
