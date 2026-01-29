import json
import os
import time
import uuid
import random
import itertools
from datetime import datetime, timezone

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
QUEUE = os.getenv("RABBIT_QUEUE", "events_q")

SCENARIO = os.getenv("SCENARIO", "normal")
EVENTS_PER_MIN = float(os.getenv("EVENTS_PER_MIN", "1800"))
SEED = int(os.getenv("SEED", "42"))
random.seed(SEED)

DEVICES = ["web", "ios", "android"]
CHANNELS = ["organic", "ads", "email"]
COUNTRIES = ["RU", "KZ", "BY", "AM", "GE"]
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "kids"]

# Каталог: 500 товаров
PRODUCTS = []
for pid in range(1, 501):
    cat = random.choice(CATEGORIES)
    base = {"electronics": 25000, "fashion": 4000, "home": 7000, "beauty": 2500, "sports": 9000, "kids": 6000}[cat]
    price = max(199, int(random.lognormvariate(8.5, 0.35) / 1000 * base))
    PRODUCTS.append((pid, cat, float(price)))

def ts_to_str(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def diurnal_multiplier() -> float:
    h = datetime.now().hour
    if 2 <= h <= 6:
        return 0.35
    if 7 <= h <= 10:
        return 0.9
    if 11 <= h <= 14:
        return 1.25
    if 18 <= h <= 22:
        return 1.55
    return 1.0

def scenario_mult() -> float:
    if SCENARIO == "traffic_spike":
        return 3.0
    if SCENARIO == "bot_attack":
        return 2.5
    return 1.0

def payment_error_prob() -> float:
    return 0.35 if SCENARIO == "payment_incident" else 0.03

def latency_ms() -> int:
    base = 80
    if SCENARIO == "slow_app":
        base = 250
    if SCENARIO == "traffic_spike":
        base = 160
    return int(random.lognormvariate(4.5, 0.35)) + base

def connect():
    params = pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=30)
    return pika.BlockingConnection(params)

def publish(ch, ev: dict):
    body = json.dumps(ev).encode("utf-8")
    ch.basic_publish(
        exchange="",
        routing_key=QUEUE,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2),
    )

def pick_product():
    return random.choice(PRODUCTS)

def main():
    conn = connect()
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)

    print(f"[generator] connected: queue={QUEUE} scenario={SCENARIO} events/min={EVENTS_PER_MIN}", flush=True)

    user_seq = itertools.count(1)

    while True:
        # сколько "сессий" в секунду
        eps = (EVENTS_PER_MIN * diurnal_multiplier() * scenario_mult()) / 60.0
        n = int(eps)
        if random.random() < (eps - n):
            n += 1

        for _ in range(n):
            now = time.time()
            user_id = random.randint(1, 80000)
            session_id = str(uuid.uuid4())

            device = random.choices(DEVICES, weights=[55, 25, 20], k=1)[0]
            channel = random.choices(CHANNELS, weights=[55, 30, 15], k=1)[0]
            country = random.choices(COUNTRIES, weights=[70, 10, 8, 6, 6], k=1)[0]

            # bot_attack: много просмотров без покупок
            is_bot = (SCENARIO == "bot_attack" and random.random() < 0.35)

            # page_view (почти всегда)
            ev = dict(
                event_time=ts_to_str(now),
                event_id=str(uuid.uuid4()),
                event_type="page_view",
                user_id=user_id,
                session_id=session_id,
                order_id=str(uuid.UUID(int=0)),
                product_id=0,
                category="",
                price=0.0,
                quantity=0,
                device=device,
                channel=channel,
                country=country,
                status="ok",
                error_code="",
                latency_ms=latency_ms(),
            )
            publish(ch, ev)

            # product_view
            if random.random() < (0.55 if not is_bot else 0.85):
                pid, cat, price = pick_product()
                ev2 = ev | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "product_view",
                    "product_id": int(pid),
                    "category": cat,
                    "price": float(price),
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev2)
            else:
                continue

            # add_to_cart
            if random.random() < (0.18 if not is_bot else 0.01):
                qty = 1 if random.random() < 0.8 else 2
                ev3 = ev2 | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "add_to_cart",
                    "quantity": int(qty),
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev3)
            else:
                continue

            # checkout_start
            if random.random() < 0.6:
                ev4 = ev3 | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "checkout_start",
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev4)
            else:
                continue

            # payment_attempt
            if random.random() < 0.92:
                status = "error" if random.random() < payment_error_prob() else "ok"
                ev5 = ev4 | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "payment_attempt",
                    "status": status,
                    "error_code": (random.choice(["PAYMENT_TIMEOUT", "DECLINED", "PROVIDER_5XX"]) if status == "error" else ""),
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev5)
                if status == "error":
                    continue
            else:
                continue

            # order_created
            order_id = str(uuid.uuid4())
            ev6 = ev5 | {
                "event_id": str(uuid.uuid4()),
                "event_type": "order_created",
                "order_id": order_id,
                "latency_ms": latency_ms(),
            }
            publish(ch, ev6)

            # order_paid
            ev7 = ev6 | {
                "event_id": str(uuid.uuid4()),
                "event_type": "order_paid",
                "latency_ms": latency_ms(),
            }
            publish(ch, ev7)

            # delivered (не все доходят)
            if random.random() < 0.93:
                ev8 = ev7 | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "order_delivered",
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev8)
            else:
                ev9 = ev7 | {
                    "event_id": str(uuid.uuid4()),
                    "event_type": "order_cancelled",
                    "error_code": random.choice(["NO_STOCK", "USER_CANCEL", "DELIVERY_FAIL"]),
                    "latency_ms": latency_ms(),
                }
                publish(ch, ev9)

        time.sleep(1)

if __name__ == "__main__":
    main()
