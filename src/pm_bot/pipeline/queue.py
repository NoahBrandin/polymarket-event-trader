"""Zentrale asynchrone Event-Queue."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from src.pm_bot.configuration.logger_config import get_logger
from src.pm_bot.pipeline.events import EventEnvelope

logger = get_logger()


@dataclass(slots=True, frozen=True)
class EventQueueStats:
    """Beschreibt den aktuellen Zustand und Durchsatz der begrenzten EventQueue."""
    current_size: int
    maximum_size: int
    published_messages: int
    consumed_messages: int


class EventQueue:
    """
    Dünne, typisierte Abstraktion um ``asyncio.Queue``.

    Ein begrenztes ``maxsize`` erzeugt Backpressure: Ist die Engine langsamer als
    der Producer, wartet der Producer beim Einfügen, statt unbegrenzt Speicher zu
    verbrauchen.
    """

    def __init__(self, maxsize: int = 10_000) -> None:
        if maxsize < 0:#
            logger.error("EventQueue maxsize < 0")
            raise ValueError("maxsize darf nicht negativ sein")

        self._queue: asyncio.Queue[EventEnvelope] = asyncio.Queue(maxsize=maxsize)
        self._published_messages = 0
        self._consumed_messages = 0

    async def put(self, message: EventEnvelope) -> None:
        await self._queue.put(message)
        self._published_messages += 1

    async def get(self) -> EventEnvelope:
        message = await self._queue.get()
        self._consumed_messages += 1
        return message

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    @property
    def empty(self) -> bool:
        return self._queue.empty()

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def stats(self) -> EventQueueStats:
        return EventQueueStats(
            current_size=self._queue.qsize(),
            maximum_size=self._queue.maxsize,
            published_messages=self._published_messages,
            consumed_messages=self._consumed_messages,
        )
