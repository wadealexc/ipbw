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

class CrawlResult(BaseModel):
    IDs: Dict[str, Sequence[str]]
    IPs: Dict[str, Sequence[str]]

@app.get("/ping")
def read_index():
    return "Hello, crawlers!"

@app.post("/crawl")
async def post_crawl_results(result: CrawlResult):
    res_ids = await db.peer_ids.insert_one(result.IDs)
    res_ips = await db.peer_ips.insert_one(result.IPs)
    return "Inserted %d IDs and %d IPs" % (len(result.IDs) - 1, len(result.IPs) - 1)