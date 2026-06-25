import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Callable

import sys

from pm_bot.configuration.logger_config import get_logger
from pm_bot.configuration.selection import SubscriptionSelection
from pm_bot.configuration.trading import StrategyDecision
from pm_bot.consumers.execution.bass import Execution
from pm_bot.consumers.strategy.base import Strategy
from pm_bot.locel_types import StrategyName, ExecutionMode
from pm_bot.pipeline.events import EventEnvelope, EventType
from pm_bot.pipeline.queue import EventQueueStats, EventQueue
from pm_bot.producer.base import Producer

logger = get_logger()

@dataclass(slots=True, frozen=True)
class EngineStats:
    processed_events: int
    submitted_orders: int
    execution_reports: int
    selection_updates: int
    queue: EventQueueStats


@dataclass(slots=True, frozen=True)
class EngineConfig:
    queue_size: int = 10_000

    testing:bool = False #only use for tests

    tick_hz: int = 60 #schläge die Sekunde

    print_lifecycle: bool = True
    print_events: bool = True
    print_execution: bool = True
    stop_on_producer_error: bool = True
    stop_on_strategy_error: bool = True
    stop_on_execution_error: bool = True

    def __post_init__(self) -> None:
        if self.queue_size < 0:
            logger.error(f"queue_size {self.queue_size} is negative")
            raise ValueError("queue_size darf nicht negativ sein")


class Engine:
    def __init__(
            self,
            producer: Producer,
            strategy: Strategy,
            execution: Execution,
            config: EngineConfig,
            ) -> None:
        self.producer: Producer = producer
        self.strategy: Strategy = strategy
        self.execution: Execution = execution
        self.config = config

        self._running = False
        self._stopping = False

        self._selection_updates = 0
        self._processed_events = 0
        self._submitted_orders = 0
        self._execution_reports = 0

        self.event_handler:Callable[[EventEnvelope], None] | None = None
        self._producer_task = None
        self.event_queue = EventQueue()

    async def run(self) -> EngineStats:
        """
        Öffentlicher Lifecycle: Komponenten starten, konsumieren und kontrolliert
        herunterfahren. Fehlerzähler bleiben auch bei abgefangenen Fehlern erhalten.
        """
        if self._running:
            logger.error(f"Engine.run() is already running")
            raise RuntimeError("Die Engine läuft bereits")

        self._running = True
        self._stopping = False
        logger.info(f"Engine.run() is starting. Whit producer: {self.producer.config.name}, "
                    f"strategy: {self.strategy.config.strategy_name if self.strategy else StrategyName.NONE }, "
                    f"execution: {self.execution.config.execution_name if self.execution else ExecutionMode.NONE}")

        try:
            await self._start()

            if not self._stopping:
                self._producer_task = asyncio.create_task(
                        self.producer.run(self.event_queue),
                        name="producer",
                    )

                await self._consume_until_producers_finish() # Start main_loop
        except SystemExit as error:
            raise
        except Exception as error:
            logger.error(f"Engine.run() is failed with error: {error}")
            raise Exception(f"Engine.run() is failed with error: {error}")

        finally:
            if self._stopping:
                logger.info(f"Initiating GRACEFUL shutdown...")
            else:
                logger.info(f"Initiating shutdown...")

            await self._shutdown_producer()
            logger.debug(f"Producer shut down")
            if self.strategy is not None:
                try:
                    await self.strategy.on_stop()
                except Exception as error:
                    logger.error(f"Strategy stop failed: {error}")
                    raise Exception(f"Strategy stop failed: {error}")

            if self.execution is not None:
                try:
                    await self.execution.stop()
                except Exception as error:
                    logger.error(f"Execution stop failed: {error}")
                    raise Exception(f"Execution-Stop fehlgeschlagen: {error}")

            self._running = False
            logger.info(f"Engine.run() is stopped")

        return self.stats

    async def _start(self):
        if self.strategy is not None:
            selection = await self.strategy.get_subscription_selection()
            if selection is None:
                selection = await self.producer.get_default_subscription_selection()
            await self._apply_subscription_selection(selection)

            try:
                initial_decision = await self.strategy.on_start()
            except Exception as error:
                logger.error(f"Strategy start failed: {error}")
                raise Exception(f"Strategy-Start fehlgeschlagen: {error}")

            if self.execution is not None:
                await self.strategy.add_account_interface(self.execution.account_interface)
                if initial_decision is not None:
                    try:
                        report = await self._execution_handel_decisions(initial_decision)
                    except Exception as error:
                        logger.error(f"Execution-Start fehlgeschlagen: {error}")
                        raise Exception(f"Execution-Start fehlgeschlagen: {error}")

        else:
            await self._apply_subscription_selection(await self.producer.get_default_subscription_selection())

    async def _consume_until_producers_finish(self) -> None:
        """
        Queue-Konsum: Producer-Lifecycle und Markt-Events werden sequenziell
        verarbeitet. Dadurch bleibt ein Backtest deterministisch.
        """
        tick_hz = 1.0 / self.config.tick_hz

        while (self._producer_task or not self.event_queue.empty) and not self._stopping: # Solang Events in Queue oder noch produziert werden
            queue_get = asyncio.create_task( #Task die das Event aus Queue pulled
                self.event_queue.get(),
                name="event-engine:queue-get",
            )

            wait_set: set[asyncio.Task[object]] = {queue_get}
            if self._producer_task:
                wait_set.add(self._producer_task)

            done, _ = await asyncio.wait( # Wartet auf neu Events
                wait_set,
                timeout=tick_hz,
                return_when=asyncio.FIRST_COMPLETED,
            )

            # Fall 1: Timeout abgelaufen
            if not done: # done Leer wenn während Timeout kein Event und Producer fertig wurde
                queue_get.cancel()
                await asyncio.gather(queue_get, return_exceptions=True)
                continue

            if self._producer_task in done:
                task = self._producer_task
                self._producer_task = None  # WICHTIG: Auf None setzen, damit die Schleife das Ende erkennt

                try:
                    task.result()
                except asyncio.CancelledError:
                    pass
                except Exception as error:
                    logger.error(f"Producer_task failed unexpectedly: {error}")
                    raise Exception(f"Producer-Task unerwartet fehlgeschlagen: {error}")

            if queue_get in done: #Bearbeite task aus queue
                message = queue_get.result()
                try:
                    await self._process_message(message)
                finally:
                    self._processed_events += 1
                    self.event_queue.task_done()
            else:
                queue_get.cancel()
                await asyncio.gather(queue_get, return_exceptions=True)


    async def _process_message(self, envelope: EventEnvelope) -> None:
        """
        Event-Pipeline: WebSocket-Marktdaten aktualisieren zusätzlich State und
        Execution; API-Daten werden direkt an Handler und Strategie weitergereicht.
        """
        object.__setattr__(envelope, "received_at", datetime.now(timezone.utc)) #sehr vorsichtig umgeht (frozen=True) bei EventEnvelop ist aber schneller als replcat()
        object.__setattr__(envelope, "sequence", self._processed_events)

        logger.info(f"Event: {envelope}")

        if envelope.event_type == EventType.ERROR or envelope.event_type == EventType.HEARTBEAT: # Error wird nicht bearbeite (später vilt mit risk_manger)
            return

        if self.event_handler is not None: #Führe ein on_event Ereignis aus
            self.event_handler(envelope)

        if self.strategy is None: # für test nur von Producer
            if self.config.testing:
                self._stopping = True
            return

        decision = await self._strategy_handel_envelope(envelope)

        if decision is None:
            return

        if self.execution is None:
            if self.config.testing:
                self._stopping = True
            return

        await self._execution_handel_decisions(decision)

        if self.config.testing:
            self._stopping = True

    async def _strategy_handel_envelope(self, envelope: EventEnvelope) -> StrategyDecision | None:
        if not envelope.producer_type == self.strategy.config.producer_type:
            logger.error(
                f"Envelope type {envelope.producer_type} does not match strategy type {self.strategy.config.producer_type}")
            raise Exception(
                f"Event type {envelope.producer_type} passt nicht zum strategy type {self.strategy.config.producer_type}")

        try:
            decision = await self.strategy.on_event(envelope)
            if decision is None: return None
        except Exception as error:
            logger.error(f"Strategy handling of event failed: {error}")
            raise Exception(f"Strategie-Verarbeitung eines Marktevents fehlgeschlagen: {error}")

        logger.info(f"Decision: {decision}")
        return decision

    async def _execution_handel_decisions(self, decision:StrategyDecision) -> None:
        if decision.subscription_selection is not None:
            await self._apply_subscription_selection(decision.subscription_selection)

        for order in decision.orders:
            self._submitted_orders += 1
            logger.debug(f"Submitted order: {order}")
            try:
                report = await self.execution.execute(order)
            except Exception as error:
                logger.error(f"Execution order-submission failed unexpectedly: {error}")
                raise Exception(f"Execution Oder-Bearbeitung fehlgeschlagen: {error}")

            self._execution_reports += 1
            logger.info(f"Executed order report: {report}")

    async def _apply_subscription_selection(self, selection: SubscriptionSelection) -> None:
        await self.producer.set_subscription_selection(selection)
        self._selection_updates += 1
        if self.config.print_lifecycle:
            logger.info(f"[MARKET SELECTION] ids={sorted(selection.ids)}")

    async def _shutdown_producer(self) -> None:
        logger.debug("Shutting down producer...")
        await self.producer.stop()

        task = self._producer_task
        self._producer_task = None

        if task is None:
            return
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except TimeoutError:
            logger.warning("Producer did not stop gracefully; cancelling task")
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def stats(self) -> EngineStats:
        return EngineStats(
            processed_events=self._processed_events,
            submitted_orders=self._submitted_orders,
            execution_reports=self._execution_reports,
            selection_updates=self._selection_updates,
            queue=self.event_queue.stats,
        )