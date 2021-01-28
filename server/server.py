#!/usr/bin/env python3.9

from typing import Dict, Sequence
from fastapi import FastAPI
from pydantic import BaseModel
import motor.motor_asyncio

DB_NAME = "server-test"

# Connect to MongoDB and get our DB
client = motor.motor_asyncio.AsyncIOMotorClient()
db = client[DB_NAME]

app = FastAPI()

class Peer(BaseModel):
    pid: str
    ips: Sequence[str]
    neighbors: Sequence[str]
    timestamp: str

class CrawlResult(BaseModel):
    peers: Sequence[Peer]

@app.get("/ping")
def read_index():
    return "Hello, crawlers!"

@app.post("/crawl")
async def post_crawl_results(result: CrawlResult):
    num_reachable = 0
    num_targets = 0
    for peer in result.peers:
        if len(peer.ips) != 0:
            num_reachable += 1
            num_targets += len(peer.ips)

    return "%d reachable with %d total unique targets!" % (num_reachable, num_targets)

    # res_ids = await db.peer_ids.insert_one(result.IDs)
    # res_ips = await db.peer_ips.insert_one(result.IPs)
    # return "Inserted %d IDs and %d IPs" % (len(result.IDs) - 1, len(result.IPs) - 1)