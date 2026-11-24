import asyncio
import os
from pathlib import Path

from broker import PubSubBroker
from publisher import PublisherClient
from protocol import colors_enabled, paint
from subscriber import SubscriberClient


def _cleanup_db(path_str: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = Path(path_str + suffix)
        if p.exists():
            os.remove(p)


def _banner(title: str, use_color: bool) -> None:
    print(f"\n{paint(f'=== {title} ===', 'bold', 'blue', enabled=use_color)}")


async def run_demo() -> None:
    use_color = colors_enabled()
    host = "127.0.0.1"
    port = 5577
    broker_db = "demo_sari_sari_broker.db"
    _cleanup_db(broker_db)

    sub_ids = ["restock_runner_sub", "lista_watch_sub", "store_audit_sub"]
    for sid in sub_ids:
        _cleanup_db(f"{sid}_seen.db")

    broker = PubSubBroker(host=host, port=port, db_path=broker_db, ack_timeout=8.0, use_color=use_color)
    broker_task = asyncio.create_task(broker.start())
    await asyncio.sleep(0.3)

    received_by_sub = {sid: [] for sid in sub_ids}

    def make_handler(sub_name: str):
        def _handler(frame: dict) -> None:
            received_by_sub[sub_name].append((frame["topic"], frame["msg_id"]))
        return _handler

    restock_runner = SubscriberClient(
        "restock_runner_sub",
        ["store/+/low_stock", "store/+/out_of_stock"],
        host,
        port,
        on_message=make_handler("restock_runner_sub"),
        use_color=use_color,
    )
    lista_watch = SubscriberClient(
        "lista_watch_sub",
        ["store/lista_tita_rosa/#", "store/lista_kuya_ben/#"],
        host,
        port,
        on_message=make_handler("lista_watch_sub"),
        use_color=use_color,
    )
    store_audit = SubscriberClient(
        "store_audit_sub",
        ["store/#"],
        host,
        port,
        on_message=make_handler("store_audit_sub"),
        use_color=use_color,
    )

    shelf_pub = PublisherClient("shelf_scanner_pub", host, port, use_color=use_color)
    lista_pub = PublisherClient("counter_lista_pub", host, port, use_color=use_color)

    await restock_runner.start()
    await lista_watch.start()
    await store_audit.start()
    await shelf_pub.connect()
    await lista_pub.connect()
    await asyncio.sleep(0.2)

    _banner("PHASE 1: Topic Filtering & Routing", use_color)
    await shelf_pub.publish(
        "store/snacks/low_stock",
        {"item": "Boy Bawang Cornick", "packs_left": 3},
        msg_id="msg-01",
    )
    await shelf_pub.publish(
        "store/beverages/restocked",
        {"item": "C2 Apple 500ml", "cases_added": 2},
        msg_id="msg-02",
    )
    await lista_pub.publish(
        "store/lista_tita_rosa/payment_logged",
        {"amount_php": 150.0, "remaining_balance_php": 120.0},
        msg_id="msg-03",
    )
    await asyncio.sleep(0.3)

    _banner("PHASE 2: restock_runner_sub Goes Offline", use_color)
    await restock_runner.stop()
    await asyncio.sleep(0.2)

    _banner("PHASE 3: Events Published While Offline", use_color)
    await shelf_pub.publish(
        "store/snacks/out_of_stock",
        {"item": "SkyFlakes Crackers", "packs_left": 0},
        msg_id="msg-04",
    )
    await shelf_pub.publish(
        "store/canned_goods/low_stock",
        {"item": "Ligo Sardines Red", "cans_left": 4},
        msg_id="msg-05",
    )
    await lista_pub.publish(
        "store/lista_kuya_ben/limit_reached",
        {"balance_php": 500.0, "credit_limit_php": 500.0},
        msg_id="msg-06",
    )
    await asyncio.sleep(0.3)

    _banner("PHASE 4: restock_runner_sub Reconnects & Catches Up", use_color)
    restock_runner = SubscriberClient(
        "restock_runner_sub",
        ["store/+/low_stock", "store/+/out_of_stock"],
        host,
        port,
        on_message=make_handler("restock_runner_sub"),
        use_color=use_color,
    )
    await restock_runner.start()
    await asyncio.sleep(0.5)

    _banner("DELIVERY SUMMARY", use_color)
    for sub_name, events in received_by_sub.items():
        print(f"  {paint(sub_name, 'bold', enabled=use_color)}: {events}")

    assert received_by_sub["restock_runner_sub"] == [
        ("store/snacks/low_stock", "msg-01"),
        ("store/snacks/out_of_stock", "msg-04"),
        ("store/canned_goods/low_stock", "msg-05"),
    ]
    assert received_by_sub["lista_watch_sub"] == [
        ("store/lista_tita_rosa/payment_logged", "msg-03"),
        ("store/lista_kuya_ben/limit_reached", "msg-06"),
    ]
    assert received_by_sub["store_audit_sub"] == [
        ("store/snacks/low_stock", "msg-01"),
        ("store/beverages/restocked", "msg-02"),
        ("store/lista_tita_rosa/payment_logged", "msg-03"),
        ("store/snacks/out_of_stock", "msg-04"),
        ("store/canned_goods/low_stock", "msg-05"),
        ("store/lista_kuya_ben/limit_reached", "msg-06"),
    ]
    print(f"\n{paint('All Sari-Sari Store assertions passed!', 'bold', 'green', enabled=use_color)}")

    await shelf_pub.close()
    await lista_pub.close()
    await restock_runner.stop()
    await lista_watch.stop()
    await store_audit.stop()
    broker_task.cancel()
    try:
        await broker_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(run_demo())