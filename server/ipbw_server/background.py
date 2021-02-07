import os

from loguru import logger

from .broadcast import broadcaster
from .database import Repository
from .schema import CrawlResult, WebsocketStats
from .util import remove_port_from_ips, remove_private_ips

IPBW_DSN = os.environ.get(
    "IPBW_DATABASE", "postgres://postgres:postgres@localhost:5432/ipbw"
)


async def batch_insert_worker(result: CrawlResult):
    logger.info("Starting batch insert background task")

    for peer in result.peers:
        peer.ips = remove_port_from_ips(peer.ips)
        peer.ips = remove_private_ips(peer.ips)

    repo = Repository(IPBW_DSN)
    repo.insert_batch(result)
    logger.info("Broadcasting new DB stats")

    # TODO: Add websocket publish and DB counts
    peer_count, conn_count = repo.get_statistics()
    ws_message = WebsocketStats(peer_count=peer_count, conn_count=conn_count)
    await broadcaster.push(ws_message)

    logger.info("Finished batch insert background task")
