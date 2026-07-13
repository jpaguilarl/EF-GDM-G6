from typing import Awaitable, Callable

import asyncio

from app.speed.schema import EnrichedRide


class EventBus:
    def __init__(self):
        self._subscribers: list[Callable[[EnrichedRide], Awaitable[None]]] = []

    def subscribe(self, callback: Callable[[EnrichedRide], Awaitable[None]]):
        self._subscribers.append(callback)

    async def publish(self, event: EnrichedRide):
        await asyncio.gather(*[sub(event) for sub in self._subscribers])
