#!/usr/bin/env python3
"""
Async RabbitMQ → MongoDB consumer (no threads, no metrics, no health).

Dependencies  (add exact versions to requirements.txt):
  aio-pika~=9.4
  motor~=3.4
  pymongo[srv]~=4.7          # Motor re-exports PyMongo’s errors
  python-dotenv~=1.0         # dev-only, for local .env files

Environment variables
──────────────────────────────────────────────────────────────
# RabbitMQ
RABBITMQ_HOST              (required)
RABBITMQ_PORT              defaults 5672, or 5671 if RABBITMQ_TLS=1
RABBITMQ_USERNAME          (required)
RABBITMQ_PASSWORD          (required)
RABBITMQ_QUEUE             defaults telemetry
RABBITMQ_PREFETCH          defaults 100
RABBITMQ_TLS               0/1 – enable TLS
RABBITMQ_CA_CERT           path to CA cert  (if TLS)
RABBITMQ_CLIENT_CERT       path to client cert (optional)
RABBITMQ_CLIENT_KEY        path to client key  (optional)

# MongoDB
MONGO_URI                  (required)
MONGO_DB                   (required)
MONGO_COLLECTION           (required)

# Misc
LOG_LEVEL                  DEBUG / INFO / WARNING / ERROR (default INFO)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import ssl
import sys
import time
from typing import Any, Optional

import aio_pika
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError




class ConfigError(RuntimeError):
    """Raised when required env vars are missing."""


class Settings:
    """Read & validate environment variables."""

    def __init__(self) -> None:
        load_dotenv(override=False)  # only effective for local development

        # RabbitMQ
        self.rabbitmq_nodes: list[tuple[str, int]] = [
            (host.strip().split(":")[0], int(host.strip().split(":")[1]))
            for host in os.getenv("RABBITMQ_NODES", "").split(",")
            if host.strip()
        ]
        self.rabbitmq_username: str = os.getenv("RABBITMQ_USERNAME", "")
        self.rabbitmq_password: str = os.getenv("RABBITMQ_PASSWORD", "")
        self.rabbitmq_queue: str = os.getenv("RABBITMQ_QUEUE", "telemetry")
        self.rabbitmq_prefetch: int = int(os.getenv("RABBITMQ_PREFETCH", 100))
        self.rabbitmq_tls: bool = os.getenv("RABBITMQ_TLS", "0") == "1"
        # self.rabbitmq_port: int = int(
        #     os.getenv("RABBITMQ_PORT", 5671 if os.getenv("RABBITMQ_TLS") else 5672)
        # )
        # self.rabbitmq_ca: Optional[str] = os.getenv("RABBITMQ_CA_CERT")
        # self.rabbitmq_cert: Optional[str] = os.getenv("RABBITMQ_CLIENT_CERT")
        # self.rabbitmq_key: Optional[str] = os.getenv("RABBITMQ_CLIENT_KEY")

        # Mongo
        self.mongo_uri: str = os.getenv("MONGO_URI", "")
        self.mongo_db: str = os.getenv("MONGO_DB", "")
        self.mongo_coll: str = os.getenv("MONGO_COLLECTION", "")

        # Misc
        self.log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

        self._validate()

    def _validate(self) -> None:
        missing = [
            name.upper()
            for name, value in vars(self).items()
            if not name.startswith("_")
            and "password" not in name
            and value in ("", None)
        ]
        if missing:
            raise ConfigError(
                f"Required environment variables missing: {', '.join(missing)}"
            )

CFG = Settings()

logging.basicConfig(
    level=getattr(logging, CFG.log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
LOG = logging.getLogger("consumer")


class MongoClientWrapper:
    def __init__(self, uri: str, db: str, coll: str) -> None:
        self._uri = uri
        self._db_name = db
        self._coll_name = coll

        self._client: Optional[AsyncIOMotorClient] = None
        self._collection = None

    async def connect(self, stop: asyncio.Event) -> None:
        delay = 1
        while not stop.is_set():
            try:
                LOG.info("Connecting to MongoDB %s ...", self._uri)
                self._client = AsyncIOMotorClient(
                    self._uri,
                    connectTimeoutMS=5_000,
                    serverSelectionTimeoutMS=5_000,
                    retryWrites=True,
                    retryReads=True,
                )
                # simple ping
                await self._client.admin.command("ping")
                self._collection = self._client[self._db_name][self._coll_name]
                LOG.info("MongoDB connected.")
                return
            except Exception as exc:
                LOG.warning("MongoDB connection failed: %s – retrying in %ss", exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def insert(self, doc: dict) -> None:
        if self._collection is None:
            raise RuntimeError("MongoDB not connected")
        await self._collection.insert_one(doc)

    async def close(self) -> None:
        if self._client:
            self._client.close()
            LOG.info("MongoDB connection closed.")



class RabbitConsumer:
    def __init__(self, mongo: MongoClientWrapper, stop_event: asyncio.Event) -> None:
        self.mongo = mongo
        self._stop_event = stop_event

        self._connection: Optional[aio_pika.RobustConnection] = None
        self._channel: Optional[aio_pika.RobustChannel] = None
        self._consumer_tag: Optional[str] = None

    async def _connect(self) -> None:
        delay = 1
        while not self._stop_event.is_set():
            print(CFG.rabbitmq_nodes)
            for host, port in CFG.rabbitmq_nodes:
                try:
                    LOG.info("Trying RabbitMQ node %s:%s ...", host, port)
                    self._connection = await aio_pika.connect_robust(
                        host=host,
                        port=port,
                        login=CFG.rabbitmq_username,
                        password=CFG.rabbitmq_password,
                        heartbeat=30,
                        client_properties={"connection_name": "keda-mongo-consumer"},
                    )

                    self._channel = await self._connection.channel()
                    await self._channel.set_qos(prefetch_count=CFG.rabbitmq_prefetch)

                    queue = await self._channel.declare_queue(
                        CFG.rabbitmq_queue,
                        durable=True,
                        arguments={"x-queue-type": "quorum"},
                    )

                    self._consumer_tag = await queue.consume(self._on_message, no_ack=False)
                    LOG.info("✅ Connected and consuming from RabbitMQ node: %s:%s", host, port)
                    return
                except Exception as exc:
                    LOG.warning("❌ Failed to connect to %s:%s: %s", host, port, exc)
                    continue

            LOG.warning("⚠️ All RabbitMQ nodes failed – retrying in %ss", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _on_message(self, message: aio_pika.IncomingMessage) -> None:
        try:
            if not message.body.strip():
                LOG.warning("Empty message – skipping.")
                await message.ack()
                return

            doc = json.loads(message.body)
            print(doc)
            await self.mongo.insert(doc)
            await message.ack()
            LOG.debug("Message processed and ACKed.")
        except PyMongoError:
            LOG.exception("Transient Mongo error – NACK requeue")
            await message.nack(requeue=True)
        except Exception:
            LOG.exception("Permanent failure – message sent to DLQ / dropped")
            await message.reject(requeue=False)


    async def run(self) -> None:
        """Main lifecycle – keeps reconnecting until stop event is set."""
        while not self._stop_event.is_set():
            await self._connect()
            # wait until stop or connection is closed
            while (
                not self._stop_event.is_set()
                and self._connection
                and not self._connection.is_closed
            ):
                await asyncio.sleep(1)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._channel and self._consumer_tag:
            try:
                await self._channel.cancel(self._consumer_tag)
            except Exception:
                pass
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
        LOG.info("Rabbit consumer stopped.")


async def main_async() -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    # Mongo
    mongo = MongoClientWrapper(CFG.mongo_uri, CFG.mongo_db, CFG.mongo_coll)
    await mongo.connect(stop_event)

    # Rabbit consumer
    consumer = RabbitConsumer(mongo, stop_event)
    consumer_task = asyncio.create_task(consumer.run())

    # Wait for shutdown
    await stop_event.wait()
    LOG.info("Shutdown requested … waiting for tasks to finish")

    await consumer.stop()
    await mongo.close()
    await asyncio.wait_for(consumer_task, timeout=10)
    LOG.info("Service exited cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except ConfigError as ce:
        LOG.error("Configuration error: %s", ce)
        sys.exit(1)
    except Exception as exc:
        LOG.exception("Fatal error: %s", exc)
        sys.exit(2)

