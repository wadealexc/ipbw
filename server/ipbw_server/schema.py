from typing import Sequence

from pydantic import BaseModel


class Peer(BaseModel):
    pid: str
    ips: Sequence[str]
    neighbors: Sequence[str]


class Connection(BaseModel):
    origin_peer: str
    destination_peer: str


class CrawlResult(BaseModel):
    peers: Sequence[Peer]


class WebsocketStats(BaseModel):
    peer_count: int
    conn_count: int
