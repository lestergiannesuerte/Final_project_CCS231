import argparse
import asyncio
import json
import sqlite3
from typing import Dict, Optional, Set, Tuple

from protocol import colors_enabled, encode_frame, paint, time_tag, topic_matches, utc_now_iso


class DurableStore:
    """SQLite-backed durable subscription registry and per-subscriber message queue."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS subscriptions (
                    client_id TEXT NOT NULL,
                    pattern TEXT NOT NULL,
                    PRIMARY KEY (client_id, pattern)
                );
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS durable_queue (
                    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id TEXT NOT NULL,
                    msg_id TEXT NOT NULL,
                    topic TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_queue_client
                ON durable_queue(client_id, queue_id);
                """
            )
            self.conn.execute(
                """
                UPDATE durable_queue
                SET status = 'pending'
                WHERE status = 'in_flight';
                """
            )

    def add_subscription(self, client_id: str, pattern: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO subscriptions (client_id, pattern)
                VALUES (?, ?);
                """,
                (client_id, pattern),
            )

    def remove_subscription(self, client_id: str, pattern: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                DELETE FROM subscriptions
                WHERE client_id = ? AND pattern = ?;
                """,
                (client_id, pattern),
            )

    def get_all_subscriptions(self) -> list[Tuple[str, str]]:
        cur = self.conn.execute("SELECT client_id, pattern FROM subscriptions;")
        return [(row["client_id"], row["pattern"]) for row in cur.fetchall()]

    def enqueue_for_matching_subscribers(
        self, msg_id: str, topic: str, payload: dict, timestamp: str
    ) -> list[dict]:
        """Fan-out a published message into durable_queue once per matching subscriber."""
        subs = self.get_all_subscriptions()
        matched_clients: Set[str] = set()
        for client_id, pattern in subs:
            if topic_matches(pattern, topic):
                matched_clients.add(client_id)

        payload_str = json.dumps(payload, separators=(",", ":"))
        enqueued_rows: list[dict] = []

        with self.conn:
            for client_id in sorted(matched_clients):
                cur = self.conn.execute(
                    """
                    INSERT INTO durable_queue
                        (client_id, msg_id, topic, payload_json, created_at, status, attempts)
                    VALUES (?, ?, ?, ?, ?, 'pending', 0);
                    """,
                    (client_id, msg_id, topic, payload_str, timestamp),
                )
                enqueued_rows.append(
                    {
                        "queue_id": cur.lastrowid,
                        "client_id": client_id,
                        "msg_id": msg_id,
                        "topic": topic,
                        "payload": payload,
                        "timestamp": timestamp,
                        "attempt": 0,
                    }
                )
        return enqueued_rows

    def get_undelivered_for_client(self, client_id: str) -> list[dict]:
        cur = self.conn.execute(
            """
            SELECT queue_id, client_id, msg_id, topic, payload_json, created_at, status, attempts
            FROM durable_queue
            WHERE client_id = ? AND status IN ('pending', 'in_flight')
            ORDER BY queue_id ASC;
            """,
            (client_id,),
        )
        return [
            {
                "queue_id": row["queue_id"],
                "client_id": row["client_id"],
                "msg_id": row["msg_id"],
                "topic": row["topic"],
                "payload": json.loads(row["payload_json"]),
                "timestamp": row["created_at"],
                "attempt": row["attempts"],
            }
            for row in cur.fetchall()
        ]

    def mark_in_flight(self, queue_id: int) -> int:
        with self.conn:
            self.conn.execute(
                """
                UPDATE durable_queue
                SET status = 'in_flight', attempts = attempts + 1
                WHERE queue_id = ?;
                """,
                (queue_id,),
            )
            cur = self.conn.execute(
                "SELECT attempts FROM durable_queue WHERE queue_id = ?;", (queue_id,)
            )
            row = cur.fetchone()
            return int(row["attempts"]) if row else 1

    def mark_pending_on_send_fail(self, queue_id: int) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE durable_queue
                SET status = 'pending'
                WHERE queue_id = ? AND status = 'in_flight';
                """,
                (queue_id,),
            )

    def acknowledge(self, client_id: str, queue_id: int, msg_id: str) -> bool:
        with self.conn:
            cur = self.conn.execute(
                """
                DELETE FROM durable_queue
                WHERE queue_id = ? AND client_id = ? AND msg_id = ?;
                """,
                (queue_id, client_id, msg_id),
            )
            return cur.rowcount > 0

    def close(self) -> None:
        self.conn.close()


class PubSubBroker:
    def __init__(
        self,
        host: str,
        port: int,
        db_path: str,
        ack_timeout: float = 8.0,
        use_color: Optional[bool] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.store = DurableStore(db_path)
        self.ack_timeout = ack_timeout
        self.color = colors_enabled() if use_color is None else use_color
        self.active_subscribers: Dict[str, asyncio.StreamWriter] = {}
        self.io_lock = asyncio.Lock()

    def _log(self, msg: str, style: str = "cyan") -> None:
        ts = paint(f"[{time_tag()}]", "dim", enabled=self.color)
        tag = paint("[STORE HUB]", "bold", style, enabled=self.color)
        print(f"{ts} {tag} {msg}")

    async def _send_json(self, writer: asyncio.StreamWriter, obj: dict) -> bool:
        try:
            async with self.io_lock:
                writer.write(encode_frame(obj))
                await writer.drain()
            return True
        except Exception:
            return False

    async def _deliver_item(self, item: dict) -> None:
        client_id = item["client_id"]
        queue_id = int(item["queue_id"])
        writer = self.active_subscribers.get(client_id)
        if not writer or writer.is_closing():
            return

        attempt_num = self.store.mark_in_flight(queue_id)
        frame = {
            "type": "DELIVER",
            "queue_id": queue_id,
            "msg_id": item["msg_id"],
            "topic": item["topic"],
            "payload": item["payload"],
            "timestamp": item["timestamp"],
            "attempt": attempt_num,
        }
        ok = await self._send_json(writer, frame)
        if not ok:
            self.store.mark_pending_on_send_fail(queue_id)
            if self.active_subscribers.get(client_id) is writer:
                del self.active_subscribers[client_id]

    async def _flush_subscriber_queue(self, client_id: str) -> int:
        pending_items = self.store.get_undelivered_for_client(client_id)
        delivered_count = 0
        for item in pending_items:
            if client_id not in self.active_subscribers:
                break
            await self._deliver_item(item)
            delivered_count += 1
        return delivered_count

    async def _retry_unacked_loop(self) -> None:
        while True:
            await asyncio.sleep(self.ack_timeout)
            for client_id in list(self.active_subscribers.keys()):
                items = self.store.get_undelivered_for_client(client_id)
                for item in items:
                    if client_id not in self.active_subscribers:
                        break
                    self._log(
                        f"No reply yet from {paint(client_id, 'bold', enabled=self.color)} "
                        f"— sending message {item['msg_id']} again (try #{item['attempt'] + 1})",
                        style="yellow",
                    )
                    await self._deliver_item(item)

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        bound_sub_id: Optional[str] = None

        try:
            while not reader.at_eof():
                line = await reader.readline()
                if not line:
                    break
                raw = line.decode("utf-8").strip()
                if not raw:
                    continue

                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "HELLO":
                    client_id = msg["client_id"]
                    role = msg.get("role", "sub")
                    if role == "sub":
                        bound_sub_id = client_id
                        self.active_subscribers[client_id] = writer
                        replayed = await self._flush_subscriber_queue(client_id)
                        await self._send_json(
                            writer,
                            {
                                "type": "HELLO_ACK",
                                "client_id": client_id,
                                "replayed_messages": replayed,
                            },
                        )
                        if replayed > 0:
                            missed_txt = paint(
                                f"sending {replayed} missed message(s)",
                                "bold",
                                "green",
                                enabled=self.color,
                            )
                        else:
                            missed_txt = paint("no missed messages", "dim", enabled=self.color)
                        self._log(
                            f"{paint(client_id, 'bold', enabled=self.color)} is now online ({missed_txt})"
                        )
                    else:
                        await self._send_json(
                            writer, {"type": "HELLO_ACK", "client_id": client_id, "role": "pub"}
                        )
                        self._log(
                            f"Sender connected: {paint(client_id, 'bold', enabled=self.color)}"
                        )

                elif msg_type == "SUB":
                    client_id = msg["client_id"]
                    pattern = msg["pattern"]
                    bound_sub_id = client_id
                    self.active_subscribers[client_id] = writer
                    self.store.add_subscription(client_id, pattern)
                    self._log(
                        f"{paint(client_id, 'bold', enabled=self.color)} is now watching "
                        f"{paint(pattern, 'green', enabled=self.color)}"
                    )
                    await self._send_json(
                        writer,
                        {"type": "SUBACK", "client_id": client_id, "pattern": pattern, "status": "ok"},
                    )

                elif msg_type == "UNSUB":
                    client_id = msg["client_id"]
                    pattern = msg["pattern"]
                    self.store.remove_subscription(client_id, pattern)
                    self._log(
                        f"{paint(client_id, 'bold', enabled=self.color)} stopped watching "
                        f"{paint(pattern, 'red', enabled=self.color)}"
                    )
                    await self._send_json(
                        writer,
                        {"type": "UNSUBACK", "client_id": client_id, "pattern": pattern, "status": "ok"},
                    )

                elif msg_type == "PUB":
                    msg_id = msg["msg_id"]
                    topic = msg["topic"]
                    payload = msg.get("payload", {})
                    ts = msg.get("timestamp") or utc_now_iso()

                    enqueued = self.store.enqueue_for_matching_subscribers(
                        msg_id=msg_id, topic=topic, payload=payload, timestamp=ts
                    )
                    matched_names = [row["client_id"] for row in enqueued]
                    if matched_names:
                        targets_txt = ", ".join(matched_names)
                    else:
                        targets_txt = paint("nobody watching this yet", "dim", enabled=self.color)
                    self._log(
                        f"New event {paint(topic, 'bold', 'yellow', enabled=self.color)} "
                        f"(id: {msg_id}) -> {targets_txt}"
                    )

                    for item in enqueued:
                        if item["client_id"] in self.active_subscribers:
                            await self._deliver_item(item)

                    await self._send_json(
                        writer,
                        {
                            "type": "PUBACK",
                            "msg_id": msg_id,
                            "status": "queued",
                            "matched_queues": len(enqueued),
                        },
                    )

                elif msg_type == "ACK":
                    client_id = msg["client_id"]
                    queue_id = int(msg["queue_id"])
                    msg_id = msg["msg_id"]
                    removed = self.store.acknowledge(client_id, queue_id, msg_id)
                    if removed:
                        self._log(
                            f"{paint('Confirmed:', 'green', enabled=self.color)} "
                            f"{client_id} got message {msg_id} (cleared from waiting list)"
                        )

                elif msg_type == "PING":
                    await self._send_json(writer, {"type": "PONG", "timestamp": utc_now_iso()})

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"Connection issue with {peer}: {exc}", style="red")
        finally:
            if bound_sub_id and self.active_subscribers.get(bound_sub_id) is writer:
                del self.active_subscribers[bound_sub_id]
                self._log(
                    f"{paint(bound_sub_id, 'bold', 'red', enabled=self.color)} went offline "
                    f"(will save missed messages until they return)",
                    style="red",
                )
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def start(self) -> None:
        retry_task = asyncio.create_task(self._retry_unacked_loop())
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
        self._log(f"Ready and listening at {addrs} | saving data in {self.store.db_path}")
        try:
            async with server:
                await server.serve_forever()
        finally:
            retry_task.cancel()
            self.store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sari-Sari Store Durable PubSub Broker")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--db", default="sari_sari_broker.db")
    parser.add_argument("--ack-timeout", type=float, default=8.0)
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI terminal colors")
    args = parser.parse_args()

    broker = PubSubBroker(
        args.host,
        args.port,
        args.db,
        ack_timeout=args.ack_timeout,
        use_color=False if args.no_color else None,
    )
    asyncio.run(broker.start())


if __name__ == "__main__":
    main()