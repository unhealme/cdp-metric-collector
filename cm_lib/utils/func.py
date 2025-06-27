import csv

from msgspec import json

TYPE_CHECKING = False
if TYPE_CHECKING:
    from datetime import timedelta
    from typing import Any

JSON_ENC = json.Encoder()


def parse_auth(s: str) -> tuple[str, str]:
    user, passw, *_ = next(csv.reader((s,), delimiter=":")) + [""]
    return user, passw


def pretty_size(s: float):
    for u in ("Bytes", "KB", "MB", "GB", "TB", "PB", "EB", "ZB"):
        if s < 1024.0:
            return "%3.1f %s" % (s, u)
        s /= 1024.0
    return "%.1f YB" % s


def strfdelta(tdelta: "timedelta", fmt: str) -> str:
    """
    usage: strfdelta(timedelta_obj, "%(days)sd %(hours)sh%(minutes)sm%(seconds)ss")"""
    d: dict[str, int] = {"days": tdelta.days}
    d["hours"], rem = divmod(tdelta.seconds, 3600)
    d["minutes"], d["seconds"] = divmod(rem, 60)
    return fmt % d


def calc_perc(value: float, other: float | None):
    if other is None:
        return ""
    perc = value / max(other, 1)
    return f"{perc:.2%}"


def encode_json_str(data: "Any"):
    return JSON_ENC.encode(data).decode()


def ensure_api_ver(minimum: int, current: int):
    if current < minimum:
        err = f"CM api ver {current} is lower than minimum api ver {minimum}"
        raise RuntimeError(err)


def join_url(*paths: str):
    return "/".join([p.strip("/") for p in paths])
