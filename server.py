#!/usr/bin/env python3.9

from typing import Dict, Sequence
from fastapi import FastAPI
from pydantic import BaseModel
import motor.motor_asyncio
from pymongo.collection import Collection


# Connect to MongoDB and get our DB
client = motor.motor_asyncio.AsyncIOMotorClient()
db = client.ipfs_db
coll: Collection = db.peers

app = FastAPI()


class CrawlResult(BaseModel):
    IDs: Dict[str, Sequence[str]]
    IPs: Dict[str, Sequence[str]]


@app.get("/")
def read_index():
    return "Hello, crawlers!"


@app.post("/crawl")
async def post_crawl_results(result: CrawlResult):
    # res_ids = await db.peer_ids.insert_one(result.IDs)
    # res_ips = await db.peer_ips.insert_one(result.IPs)

    db_payload = []
    for origin_peer, destination_peers in result.IDs.items():
        for destination_peer in destination_peers:
            db_payload.append({origin_peer: destination_peer})

    await coll.insert(db_payload)
    return "Inserted %d IDs and %d IPs" % (len(result.IDs), len(result.IPs))
