"""Datos enteramente sintéticos y semillas fijas; sin datos de clientes."""
import math
import random

BASE_MS = 1793260800000


def risk_history(n=1600, seed=42):
    rng = random.Random(seed)
    result = []
    for i in range(n):
        amount, risk, attempts = rng.uniform(5, 650), rng.random(), rng.randint(0, 12)
        z = -4 + 1.1 * amount / 500 + 4.2 * risk + 1.8 * attempts / 10
        y = int(rng.random() < 1 / (1 + math.exp(-z)))
        result.append({"event_id": f"history-{i:05}", "entity_id": f"account-{i % 20}",
                       "event_type": "risk.observed.v1", "event_time_ms": BASE_MS + i * 1000,
                       "payload": {"amount": amount, "device_risk": risk, "attempts_1m": attempts},
                       "label": y})
    return result


def risk_stream(n=240, seed=91):
    rng = random.Random(seed)
    delayed, result = [], []
    for i in range(n):
        amount, risk, attempts = rng.uniform(5, 650), rng.random(), rng.randint(0, 12)
        phase = "normal" if i < n // 2 else "drift"
        z = (-4 + 1.1 * amount / 500 + 4.2 * risk + 1.8 * attempts / 10 if phase == "normal"
             else -0.5 + 1.1 * amount / 500 - 3.5 * risk + 1.8 * attempts / 10)
        y = int(rng.random() < 1 / (1 + math.exp(-z)))
        observation = {"event_id": f"live-{i:05}", "entity_id": f"account-{i % 20}",
                       "event_type": "risk.observed.v1", "event_time_ms": BASE_MS + (2000+i) * 1000,
                       "phase": phase,
                       "payload": {"amount": round(amount, 2), "device_risk": round(risk, 4),
                                   "attempts_1m": attempts}}
        result.append(observation)
        if i % 17 == 0:
            result.append(dict(observation))
        delayed.append({"event_id": f"label-{i:05}", "observation_id": observation["event_id"],
                        "entity_id": observation["entity_id"], "event_type": "risk.label_arrived.v1",
                        "event_time_ms": observation["event_time_ms"] + 8000, "label": y})
        if len(delayed) > 8:
            result.append(delayed.pop(0))
    result.extend(delayed)
    return result


def marketing_events():
    def row(i, session, name, sec, **kwargs):
        return {"event_id": i, "client_id": f"synthetic-{session}", "session_id": session,
                "event": name, "timestamp_ms": BASE_MS + sec * 1000, "currency": "USD",
                "analytics_storage": "granted", "ad_personalization": "granted",
                "source": "web", "campaign": "demo-search", **kwargs}
    a = row("m1", "s1", "page_view", 1)
    return [a, row("m2", "s1", "view_item", 2), row("m3", "s1", "add_to_cart", 3),
            row("m4", "s1", "begin_checkout", 4), dict(a),
            row("m5", "s2", "page_view", 5), row("m6", "s2", "begin_checkout", 6),
            row("m7", "s1", "purchase", 15, source="orders-service", transaction_id="order-1", value=120),
            row("m8", "s1", "purchase", 16, source="orders-service", transaction_id="order-1", value=120),
            row("m9", "s3", "page_view", 18, analytics_storage="denied"),
            row("m10", "s4", "begin_checkout", 20, ad_personalization="denied"),
            row("m11", "s5", "begin_checkout", 21), row("m12", "s6", "page_view", 65),
            # Compra que llega después del watermark: compensación de acción, sin reabrir métricas.
            row("m13", "s5", "purchase", 30, source="orders-service", transaction_id="order-5", value=75),
            row("m14", "s7", "purchase", 70, source="orders-service", transaction_id="bad", value=-1),
            row("m15", "s8", "page_view", 80, email="never-retained@example.invalid")]
