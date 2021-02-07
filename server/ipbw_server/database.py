import psycopg2
import psycopg2.extensions
from loguru import logger

from .schema import CrawlResult

PEER_TABLE = """
CREATE TABLE IF NOT EXISTS peers (
    id          VARCHAR(100) PRIMARY KEY,
    ips         VARCHAR(100)[],
    inserted    TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated     TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);"""
PEER_INDEX = """
CREATE INDEX IF NOT EXISTS peers_updated ON peers USING BTREE (updated);
CREATE INDEX IF NOT EXISTS peers_inserted ON peers USING BTREE (inserted);
"""
CONN_TABLE = """
CREATE TABLE IF NOT EXISTS connections (
    origin      VARCHAR(100),
    destination VARCHAR(100),
    inserted    TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated     TIMESTAMP WITHOUT TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),

    PRIMARY KEY (origin,destination)
);"""
CONN_INDEX = """
CREATE INDEX IF NOT EXISTS connections_updated ON connections USING BTREE (updated);
CREATE INDEX IF NOT EXISTS connections_inserted ON connections USING BTREE (inserted);
CREATE INDEX IF NOT EXISTS connections_origin ON connections USING BTREE (origin);
CREATE INDEX IF NOT EXISTS connections_origin ON connections USING BTREE (destination);
"""
PEER_INSERT = """
    INSERT INTO peers (id, ips) VALUES (%s, %s)
    ON CONFLICT (id) DO UPDATE
    SET
        ips = ARRAY(SELECT DISTINCT UNNEST(peers.ips || EXCLUDED.ips)),
        updated = NOW() AT TIME ZONE 'utc'
"""
CONN_INSERT = """
    INSERT INTO connections (origin,destination) VALUES (%s, %s)
    ON CONFLICT (origin,destination) DO UPDATE
    SET
        updated = NOW() AT TIME ZONE 'utc'
"""

PEER_COUNT = "SELECT COUNT(*) FROM peers"
CONN_COUNT = "SELECT COUNT(*) FROM connections"


class Repository:
    def __init__(self, dsn: str):
        self.dsn = dsn

    def _get_connection(self):
        logger.info("Connecting to the database")
        return psycopg2.connect(self.dsn)

    def get_statistics(self):
        logger.info("Fetching database statistics")
        connection = self._get_connection()
        cursor = connection.cursor()

        cursor.execute(PEER_COUNT, [])
        peer_result = cursor.fetchone()[0]

        cursor.execute(CONN_COUNT, [])
        conn_result = cursor.fetchone()[0]

        logger.info(f"peer_count={peer_result}, conn_stats={conn_result}")
        connection.close()
        cursor.close()

        return peer_result, conn_result

    def insert_batch(self, batch: CrawlResult):
        logger.info(f"Inserting a new crawler batch with {len(batch.peers)} peers")
        connection = self._get_connection()
        cursor = connection.cursor()
        peer_list, conn_list = [], []

        for peer in batch.peers:
            peer_list.append((peer.pid, peer.ips))
            for neighbour in peer.neighbors:
                conn_list.append((peer.pid, neighbour))

        logger.info(
            f"Inserting {len(peer_list)} peers with {len(conn_list)} connections"
        )

        cursor.executemany(PEER_INSERT, peer_list)
        cursor.executemany(CONN_INSERT, conn_list)
        connection.commit()
        connection.close()
        cursor.close()

    def initialize_tables(self):
        logger.info("Initializing database tables")
        connection = self._get_connection()
        cursor = connection.cursor()
        logger.info("Creating peer and connection tables")
        cursor.execute(PEER_TABLE, [])
        cursor.execute(CONN_TABLE, [])
        logger.info("Applying indices to peer and connection tables")
        cursor.execute(PEER_INDEX, [])
        cursor.execute(CONN_INDEX, [])
        connection.commit()
        cursor.close()
        connection.close()
