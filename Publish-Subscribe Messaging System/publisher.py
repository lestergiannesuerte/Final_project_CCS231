import argparse
import asyncio
import json
import uuid
from typing import Any, Dict, Optional

from protocol import colors_enabled, encode_frame, paint, time_tag


class PublisherClient:
    def __init__(
        self,
        client_id: str,
        host: str,
        port: int,
        use_color: Optional[bool] = None,
    ) -> None:
        self.client_id = client_id
        self.host = host
        self.port = port
        self.color = colors_enabled() if use_color is None else use_color
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    def _log(self, msg: str, style: str = "cyan") -> None:
        ts = paint(f"[{time_tag()}]", "dim", enabled=self.color)
        tag = paint(f"[SENDER:{self.client_id}]", "bold", style, enabled=self.color)
        print(f"{ts} {tag} {msg}")

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.writer.write(
            encode_frame({"type": "HELLO", "client_id": self.client_id, "role": "pub"})
        )
        await self.writer.drain()

        line = await self.reader.readline()
        if not line:
            raise ConnectionError("Broker closed connection during HELLO")
        ack = json.loads(line.decode("utf-8").strip())
        if ack.get("type") != "HELLO_ACK":
            raise ConnectionError(f"Unexpected handshake reply: {ack}")

        self._log("Connected to the store hub")

    async def publish(
        self,
        topic: str,
        payload: Dict[str, Any],
        msg_id: Optional[str] = None,
    ) -> dict:
        if not topic or not isinstance(topic, str) or "+" in topic or "#" in topic:
            raise ValueError(f"Invalid topic: {topic}")
        assert self.writer is not None and self.reader is not None

        mid = msg_id or str(uuid.uuid4())[:8]
        frame = {
            "type": "PUB",
            "client_id": self.client_id,
            "msg_id": mid,
            "topic": topic,
            "payload": payload,
        }
        self.writer.write(encode_frame(frame))
        await self.writer.drain()

        line = await self.reader.readline()
        if not line:
            raise ConnectionError("Broker closed connection while waiting for PUBACK")
        reply = json.loads(line.decode("utf-8").strip())

        matched = reply.get("matched_subscribers", 0)
        payload_str = json.dumps(payload, separators=(",", ":"))
        self._log(
            f"Sent {paint(topic, 'bold', 'cyan', enabled=self.color)} "
            f"{paint(payload_str, 'dim', enabled=self.color)} "
            f"(id: {mid}) -> saved for {matched} listener(s)"
        )
        return reply

    async def close(self) -> None:
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass
        self._log("Disconnected cleanly", style="yellow")


def _build_payload_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    if args.item is not None:
        payload["item"] = args.item
    if args.packs is not None:
        payload["packs_left"] = args.packs
    if args.cases is not None:
        payload["cases_added"] = args.cases
    if args.amount is not None:
        payload["amount_php"] = args.amount
    if args.note:
        payload["note"] = args.note
    if not payload:
        payload["status"] = "event_recorded"
    return payload


async def _interactive_loop(pub: PublisherClient) -> None:
    print("\n--- Manual Lista / Store Event Mode (type 'quit' or press Ctrl+C to exit) ---")
    while True:
        person = await asyncio.to_thread(
            input, "Person name (e.g. ate_mia, or press Enter for general topic): "
        )
        person = person.strip().lower().replace(" ", "_")
        if person in ("quit", "exit", "q"):
            break

        if person:
            action = await asyncio.to_thread(
                input, "Action [credit_added / payment_logged / limit_reached] (default: credit_added): "
            )
            action = action.strip() or "credit_added"
            topic = f"store/lista_{person}/{action}"
        else:
            topic = await asyncio.to_thread(
                input, "Topic (e.g. store/snacks/low_stock): "
            )
            topic = topic.strip()
            if not topic:
                print("Topic cannot be empty.")
                continue

        amount_raw = await asyncio.to_thread(input, "Amount in PHP (or press Enter to skip): ")
        amount_raw = amount_raw.strip()
        note = (await asyncio.to_thread(input, "Short note (optional): ")).strip()

        payload: Dict[str, Any] = {}
        if amount_raw:
            try:
                payload["amount_php"] = float(amount_raw)
            except ValueError:
                print("Amount must be a number (like 100 or 150.50).")
                continue
        if note:
            payload["note"] = note
        if not payload:
            payload["status"] = "recorded"

        try:
            await pub.publish(topic, payload)
        except Exception as exc:
            print(f"Could not send: {exc}")


async def _cli_main(args: argparse.Namespace) -> None:
    use_color = False if args.no_color else None
    pub = PublisherClient(
        client_id=args.client_id,
        host=args.host,
        port=args.port,
        use_color=use_color,
    )
    await pub.connect()
    try:
        if args.interactive:
            await _interactive_loop(pub)
        else:
            if not args.topic:
                raise ValueError("--topic is required unless --interactive is used")
            payload = _build_payload_from_args(args)
            await pub.publish(args.topic, payload, msg_id=args.msg_id)
    finally:
        await pub.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Sari-Sari Store Publisher Client")
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--item", default=None)
    parser.add_argument("--packs", type=int, default=None)
    parser.add_argument("--cases", type=int, default=None)
    parser.add_argument("--amount", type=float, default=None)
    parser.add_argument("--note", default=None)
    parser.add_argument("--msg-id", default=None)
    parser.add_argument(
        "--interactive",
        action="store_true",
        help="Manually type lista person, action, and amount one by one",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI terminal colors")
    args = parser.parse_args()
    try:
        asyncio.run(_cli_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()