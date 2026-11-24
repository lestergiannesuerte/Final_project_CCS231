import argparse
import asyncio
import json
import shlex
import sqlite3
import uuid
from collections import deque
from typing import Callable, Deque, List, Optional

from protocol import colors_enabled, encode_frame, paint, time_tag, utc_now_iso, validate_pattern

BEVERAGE_KEYWORDS = (
    "beverage",
    "beverages",
    "drink",
    "drinks",
    "c2",
    "coke",
    "pepsi",
    "sprite",
    "water",
    "juice",
    "soda",
    "tea",
    "coffee",
    "milk",
)


def _classify_kind(topic: str, payload: dict) -> str:
    text = f"{topic} {payload.get('item', '')}".lower()
    if any(word in text for word in BEVERAGE_KEYWORDS):
        return "BEVERAGE"
    return "ITEM"


def _infer_restock_category(first_arg: Optional[str], item_or_rest: str) -> str:
    if first_arg:
        low = first_arg.strip().lower().replace(" ", "_")
        if low in ("beverages", "beverage", "drinks", "drink"):
            return "beverages"
        if low in ("snacks", "snack", "canned_goods", "items", "item"):
            return low
    combined = f"{first_arg or ''} {item_or_rest}".lower()
    if any(word in combined for word in BEVERAGE_KEYWORDS):
        return "beverages"
    return "snacks"


class IdempotencyStore:
    """Tracks processed msg_ids in SQLite so duplicate redeliveries are skipped safely."""

    def __init__(self, db_path: str) -> None:
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_messages (
                    msg_id TEXT PRIMARY KEY,
                    processed_at TEXT NOT NULL
                );
                """
            )

    def already_processed(self, msg_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM processed_messages WHERE msg_id = ?;", (msg_id,)
        )
        return cur.fetchone() is not None

    def mark_processed(self, msg_id: str) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO processed_messages (msg_id, processed_at)
                VALUES (?, ?);
                """,
                (msg_id, utc_now_iso()),
            )

    def close(self) -> None:
        self.conn.close()


class SubscriberClient:
    def __init__(
        self,
        client_id: str,
        patterns: List[str],
        host: str,
        port: int,
        on_message: Optional[Callable[[dict], None]] = None,
        auto_ack: bool = True,
        state_db: Optional[str] = None,
        use_color: Optional[bool] = None,
    ) -> None:
        for pat in patterns:
            if not validate_pattern(pat):
                raise ValueError(f"Invalid topic filter pattern: {pat}")
        self.client_id = client_id
        self.patterns = list(patterns)
        self.host = host
        self.port = port
        self.on_message = on_message
        self.auto_ack = auto_ack
        self.color = colors_enabled() if use_color is None else use_color
        self.dedup_store = IdempotencyStore(state_db or f"{client_id}_seen.db")
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self._pub_reader: Optional[asyncio.StreamReader] = None
        self._pub_writer: Optional[asyncio.StreamWriter] = None
        self._stop_event = asyncio.Event()
        self._listen_task: Optional[asyncio.Task] = None
        self._pending_frames: Deque[dict] = deque()
        self._suback_event = asyncio.Event()
        self._io_lock = asyncio.Lock()
        self._pub_lock = asyncio.Lock()

    def _log(self, msg: str, style: str = "green") -> None:
        ts = paint(f"[{time_tag()}]", "dim", enabled=self.color)
        tag = paint(f"[LISTENER:{self.client_id}]", "bold", style, enabled=self.color)
        print(f"{ts} {tag} {msg}")

    async def _send(self, obj: dict) -> None:
        assert self.writer is not None
        async with self._io_lock:
            self.writer.write(encode_frame(obj))
            await self.writer.drain()

    async def _ensure_pub_connection(self) -> None:
        if self._pub_writer is not None and not self._pub_writer.is_closing():
            return
        self._pub_reader, self._pub_writer = await asyncio.open_connection(self.host, self.port)
        self._pub_writer.write(
            encode_frame({"type": "HELLO", "client_id": f"{self.client_id}_pub", "role": "pub"})
        )
        await self._pub_writer.drain()
        line = await self._pub_reader.readline()
        if not line:
            raise ConnectionError("Broker closed connection during runner publish HELLO")

    async def publish_restocked(self, category: str, item: str, cases: int = 1) -> None:
        clean_cat = category.strip().lower().replace(" ", "_") or "snacks"
        topic = f"store/{clean_cat}/restocked"
        kind = _classify_kind(topic, {"item": item})
        qty_key = "cases_added" if (kind == "BEVERAGE" or clean_cat == "canned_goods") else "packs_added"
        payload = {"item": item, qty_key: cases}
        kind_badge = paint(f"[{kind}]", "bold", "blue" if kind == "BEVERAGE" else "yellow", enabled=self.color)
        mid = str(uuid.uuid4())[:8]

        async with self._pub_lock:
            await self._ensure_pub_connection()
            assert self._pub_writer is not None and self._pub_reader is not None
            frame = {
                "type": "PUB",
                "client_id": f"{self.client_id}_pub",
                "msg_id": mid,
                "topic": topic,
                "payload": payload,
            }
            self._pub_writer.write(encode_frame(frame))
            await self._pub_writer.drain()

            line = await self._pub_reader.readline()
            if not line:
                raise ConnectionError("Broker closed connection while waiting for PUBACK")
            reply = json.loads(line.decode("utf-8").strip())
            matched = reply.get("matched_subscribers", 0)
            self._log(
                f"Reported restocked {kind_badge}: {paint(topic, 'bold', 'cyan', enabled=self.color)} "
                f"{paint(json.dumps(payload, separators=(',', ':')), 'dim', enabled=self.color)} "
                f"-> saved for {matched} listener(s)",
                style="cyan",
            )

    async def _wait_for_frame(self, expected_type: str) -> dict:
        assert self.reader is not None
        while True:
            line = await self.reader.readline()
            if not line:
                raise ConnectionError(f"Connection closed while waiting for {expected_type}")
            raw = line.decode("utf-8").strip()
            if not raw:
                continue
            frame = json.loads(raw)
            if frame.get("type") == expected_type:
                return frame
            self._pending_frames.append(frame)

    async def send_ack(self, queue_id: int, msg_id: str) -> None:
        await self._send(
            {
                "type": "ACK",
                "client_id": self.client_id,
                "queue_id": int(queue_id),
                "msg_id": msg_id,
            }
        )

    async def add_subscription(self, pattern: str) -> None:
        if not validate_pattern(pattern):
            raise ValueError(f"Invalid topic filter pattern: {pattern}")
        if pattern in self.patterns:
            self._log(f"Already watching: {pattern}", style="yellow")
            return
        self._suback_event.clear()
        await self._send({"type": "SUB", "client_id": self.client_id, "pattern": pattern})
        await asyncio.wait_for(self._suback_event.wait(), timeout=5.0)
        self.patterns.append(pattern)
        self._log(f"Now also watching: {paint(pattern, 'cyan', enabled=self.color)}")

    async def _handle_frame(self, frame: dict) -> None:
        ftype = frame.get("type")
        if ftype == "SUBACK":
            self._suback_event.set()
            return

        if ftype == "DELIVER":
            msg_id = frame["msg_id"]
            queue_id = int(frame["queue_id"])
            topic = frame["topic"]
            payload = frame["payload"]
            attempt = frame["attempt"]
            payload_str = json.dumps(payload, separators=(",", ":"))
            kind = _classify_kind(topic, payload)
            kind_badge = paint(f"[{kind}]", "bold", "blue" if kind == "BEVERAGE" else "yellow", enabled=self.color)

            if self.dedup_store.already_processed(msg_id):
                self._log(
                    f"{paint('Already saw this earlier — skipped duplicate', 'bold', 'magenta', enabled=self.color)} "
                    f"(id: {msg_id}) -> confirming receipt",
                    style="magenta",
                )
            else:
                is_urgent = any(k in topic for k in ("out_of_stock", "limit_reached"))
                topic_style = ("bold", "red") if is_urgent else ("bold", "green")
                attempt_note = (
                    paint(f" (resent #{attempt})", "yellow", enabled=self.color)
                    if attempt > 1
                    else ""
                )
                self._log(
                    f"Got {kind_badge} {paint(topic, *topic_style, enabled=self.color)} "
                    f"{paint(payload_str, 'dim', enabled=self.color)}{attempt_note}"
                )
                if self.on_message:
                    self.on_message(frame)
                self.dedup_store.mark_processed(msg_id)

            if self.auto_ack:
                await self.send_ack(queue_id, msg_id)

    async def connect_and_subscribe(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        await self._send({"type": "HELLO", "client_id": self.client_id, "role": "sub"})
        hello_ack = await self._wait_for_frame("HELLO_ACK")
        replayed = hello_ack.get("replayed_messages", 0)
        pats_str = ", ".join(self.patterns)
        self._log(
            f"Online ({replayed} missed message(s) coming) | watching: {paint(pats_str, 'cyan', enabled=self.color)}"
        )

        for pat in self.patterns:
            await self._send({"type": "SUB", "client_id": self.client_id, "pattern": pat})
            await self._wait_for_frame("SUBACK")

    async def _listen_loop(self) -> None:
        assert self.reader is not None
        try:
            while self._pending_frames and not self._stop_event.is_set():
                buffered = self._pending_frames.popleft()
                await self._handle_frame(buffered)

            while not self._stop_event.is_set():
                line = await self.reader.readline()
                if not line:
                    self._log("Lost connection to the store hub", style="red")
                    break
                raw = line.decode("utf-8").strip()
                if not raw:
                    continue
                frame = json.loads(raw)
                await self._handle_frame(frame)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"Listener problem: {exc}", style="red")

    async def start(self) -> None:
        self._stop_event.clear()
        await self.connect_and_subscribe()
        self._listen_task = asyncio.create_task(self._listen_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        if self._pub_writer:
            try:
                self._pub_writer.close()
                await self._pub_writer.wait_closed()
            except Exception:
                pass
        self.dedup_store.close()
        self._log("Turned off cleanly", style="red")


async def _live_command_loop(sub: SubscriberClient) -> None:
    is_runner = sub.client_id.startswith("restock_runner")
    if sub.client_id == "lista_watch_sub":
        print("Tip: Type 'lista <name>' (example: lista kuya_marco) or 'sub <topic>' to add a topic live.")
    elif is_runner:
        print('Tip: Type restocked <category> "<item>" <amount> (auto-detects [BEVERAGE] vs [ITEM]).')

    while not sub._stop_event.is_set():
        raw = await asyncio.to_thread(input, "")
        cmd = raw.strip()
        if not cmd:
            continue

        parts = shlex.split(cmd)
        head = parts[0].lower()

        if head == "restocked" and is_runner:
            arg1 = parts[1] if len(parts) >= 2 else None
            arg2 = parts[2] if len(parts) >= 3 else None
            arg3 = parts[3] if len(parts) >= 4 else None

            if arg1 and arg1.lower() in ("beverages", "beverage", "drinks", "drink", "snacks", "snack", "canned_goods", "items", "item"):
                category = _infer_restock_category(arg1, arg2 or "")
                item = arg2 or "Restocked Item"
                cases = int(arg3) if arg3 and arg3.isdigit() else 1
            else:
                item = arg1 or "Restocked Item"
                category = _infer_restock_category(None, item)
                cases = int(arg2) if arg2 and arg2.isdigit() else 1

            try:
                await sub.publish_restocked(category, item, cases)
            except Exception as exc:
                print(f"Could not report restock: {exc}")
            continue

        if sub.client_id == "lista_watch_sub":
            if head == "lista" and len(parts) == 2:
                person = parts[1].strip().lower().replace(" ", "_")
                pattern = f"store/lista_{person}/#"
            elif head == "sub" and len(parts) == 2:
                pattern = parts[1].strip()
            else:
                pattern = cmd

            try:
                await sub.add_subscription(pattern)
            except Exception as exc:
                print(f"Could not add subscription '{pattern}': {exc}")


async def _cli_main(args: argparse.Namespace) -> None:
    use_color = False if args.no_color else None
    sub = SubscriberClient(
        client_id=args.client_id,
        patterns=args.topic,
        host=args.host,
        port=args.port,
        auto_ack=not args.no_ack,
        state_db=args.state_db,
        use_color=use_color,
    )
    await sub.start()
    cmd_task = asyncio.create_task(_live_command_loop(sub))
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        cmd_task.cancel()
        try:
            await cmd_task
        except asyncio.CancelledError:
            pass
        await sub.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sari-Sari Store Subscriber Client")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--topic", action="append", required=True, help="Repeatable topic filter")
    parser.add_argument("--state-db", default=None, help="Optional path for idempotency SQLite DB")
    parser.add_argument("--no-ack", action="store_true", help="Disable auto-ACK (for retry tests)")
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI terminal colors")
    args = parser.parse_args()
    try:
        asyncio.run(_cli_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()