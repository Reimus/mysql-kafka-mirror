from __future__ import annotations

import logging
import queue
import threading
import time
from typing import List

from ..config.settings import Settings
from ..events.models import SqlLogMessage
from .publisher import Publisher

logger = logging.getLogger(__name__)


class QueueingPublisher:
    def __init__(self, *, inner: Publisher, settings: Settings) -> None:
        self._inner = inner
        self._settings = settings
        self._q: "queue.Queue[SqlLogMessage]" = queue.Queue(maxsize=settings.publish_queue_maxsize)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="mysql-interceptor-publisher", daemon=True)
        self._thread.start()

    def publish(self, event: SqlLogMessage) -> None:
        if self._settings.backpressure == "drop":
            try:
                self._q.put_nowait(event)
            except queue.Full:
                return
        else:
            self._q.put(event)

    def publish_batch(self, events: List[SqlLogMessage]) -> None:
        for e in events:
            self.publish(e)

    def flush(self) -> None:
        while not self._q.empty():
            time.sleep(0.05)
        self._inner.flush()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2.0)
        self.flush()
        self._inner.close()

    def _run(self) -> None:
        batch: List[SqlLogMessage] = []
        last_flush = time.time()

        while not self._stop.is_set():
            timeout = max(0.0, self._settings.publish_flush_interval_s - (time.time() - last_flush))
            try:
                evt = self._q.get(timeout=timeout)
                batch.append(evt)
                if len(batch) >= self._settings.publish_batch_size:
                    try:
                        self._inner.publish_batch(batch)
                    except Exception:
                        logger.exception("Error in background publisher thread")
                    batch.clear()
                    last_flush = time.time()
            except queue.Empty:
                if batch:
                    try:
                        self._inner.publish_batch(batch)
                    except Exception:
                        logger.exception("Error in background publisher thread during flush")
                    batch.clear()
                last_flush = time.time()

        while True:
            try:
                evt = self._q.get_nowait()
                batch.append(evt)
                if len(batch) >= self._settings.publish_batch_size:
                    self._inner.publish_batch(batch)
                    batch.clear()
            except queue.Empty:
                break
        if batch:
            self._inner.publish_batch(batch)
