import os
import time
import requests
import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
QUEUE = os.getenv("RABBIT_QUEUE", "events_q")

CH_URL = os.getenv("CH_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1500"))
FLUSH_SECS = float(os.getenv("FLUSH_SECS", "2.0"))

INSERT_QUERY = "INSERT INTO shop.events_raw SETTINGS input_format_skip_unknown_fields=1 FORMAT JSONEachRow"

def post_batch(lines: list[bytes]):
    data = b"\n".join(lines) + b"\n"
    r = requests.post(
        CH_URL,
        params={"query": INSERT_QUERY},
        data=data,
        auth=(CH_USER, CH_PASSWORD),
        timeout=15,
    )
    r.raise_for_status()

def main():
    buf = []
    last_flush = time.time()

    while True:
        try:
            params = pika.ConnectionParameters(host=RABBIT_HOST, heartbeat=30)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            ch.queue_declare(queue=QUEUE, durable=True)
            ch.basic_qos(prefetch_count=BATCH_SIZE)

            print(f"[collector] connected: {RABBIT_HOST} queue={QUEUE} -> {CH_URL} as {CH_USER}", flush=True)

            def flush():
                nonlocal buf, last_flush
                if not buf:
                    return
                try:
                    post_batch([b for (b, _) in buf])
                    for (_, tag) in buf:
                        ch.basic_ack(delivery_tag=tag)
                    buf = []
                    last_flush = time.time()
                except Exception as e:
                    print(f"[collector] insert failed: {e} (retry, not ack)", flush=True)
                    time.sleep(2)

            def on_msg(channel, method, properties, body):
                nonlocal buf
                buf.append((body, method.delivery_tag))
                if len(buf) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECS:
                    flush()

            ch.basic_consume(queue=QUEUE, on_message_callback=on_msg, auto_ack=False)

            while True:
                ch.connection.process_data_events(time_limit=1)
                if (time.time() - last_flush) >= FLUSH_SECS:
                    flush()

        except Exception as e:
            print(f"[collector] error: {e}. reconnecting in 3s...", flush=True)
            time.sleep(3)

if __name__ == "__main__":
    main()
