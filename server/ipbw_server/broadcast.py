from typing import List

from fastapi import WebSocket

from .schema import WebsocketStats


class Broadcaster:
    def __init__(self):
        self.subscribers: List[WebSocket] = []
        self.broadcaster = self._get_broadcaster()

    async def _get_broadcaster(self):
        while True:
            message = yield
            await self._publish(message)

    async def _publish(self, message: str):
        alive = []
        while len(self.subscribers) > 0:
            subscriber: WebSocket = self.subscribers.pop()
            await subscriber.send_json(message)
            alive.append(subscriber)
        self.subscribers = alive

    async def push(self, message: WebsocketStats):
        await self.broadcaster.asend(message.dict())

    async def connect(self, subscriber: WebSocket):
        await subscriber.accept()
        self.subscribers.append(subscriber)

    def remove(self, subscriber: WebSocket):
        self.subscribers.remove(subscriber)


broadcaster = Broadcaster()
