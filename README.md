# shop-project — Monitoring pipeline for e-commerce (Big Data Methods)

End-to-end локальная система генерации и обработки данных, имитирующая production data pipeline интернет-магазина.

## Архитектура
**Generator (Python)** → **RabbitMQ** → **Collector (Python)** → **ClickHouse** → **Grafana**

- Generator: генерирует поток событий (page_view → product_view → add_to_cart → checkout_start → payment_attempt → order_paid)
- RabbitMQ: очередь/буфер
- Collector: читает из очереди, батчит, валидирует и пишет в ClickHouse
- ClickHouse: хранение + аналитические запросы
- Grafana: дашборд

## Быстрый старт
```bash
docker compose up -d --build



