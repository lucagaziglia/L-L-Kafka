"""Normalización allowlist y simulador finito de event time (no es Apache Flink)."""
import math

ALLOWED_EVENTS = {"page_view", "view_item", "add_to_cart", "begin_checkout", "purchase"}
ALLOWED_FIELDS = {"event_id", "client_id", "session_id", "event", "timestamp_ms", "currency", "value",
                  "transaction_id", "campaign", "analytics_storage", "ad_personalization", "source"}


def normalize(raw):
    if raw.get("analytics_storage") != "granted":
        return None
    if raw.get("event") not in ALLOWED_EVENTS:
        raise ValueError("event no permitido")
    for key in ("event_id", "client_id", "session_id", "source"):
        if not isinstance(raw.get(key), str) or not raw[key] or len(raw[key]) > 120:
            raise ValueError(f"{key} inválido")
    if not isinstance(raw.get("timestamp_ms"), int) or isinstance(raw["timestamp_ms"], bool):
        raise ValueError("timestamp_ms debe ser entero")
    out = {k: v for k, v in raw.items() if k in ALLOWED_FIELDS}
    if raw["event"] == "purchase":
        if not isinstance(raw.get("transaction_id"), str) or not raw["transaction_id"]:
            raise ValueError("purchase requiere transaction_id")
        value = raw.get("value")
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError("purchase requiere value no negativo y finito")
        if raw.get("currency") not in {"USD", "ARS"}:
            raise ValueError("currency no soportada en el lab")
    return out


class MarketingEngine:
    def __init__(self, lateness_ms=10000, wait_ms=30000):
        self.lateness_ms, self.wait_ms = lateness_ms, wait_ms
        self.max_time = -1
        self.watermark = -1
        self.seen, self.transactions = set(), set()
        self.sessions, self.windows, self.actions, self.dlq = {}, {}, [], []
        self.stats = {"accepted": 0, "duplicates": 0, "consent_dropped": 0,
                      "late": 0, "invalid": 0, "duplicate_transactions": 0}

    def _timers(self):
        for identity, session in self.sessions.items():
            session_key = ':'.join(identity)
            checkout = session.get("checkout")
            if (checkout is not None and self.watermark >= checkout + self.wait_ms
                    and not session.get("purchased") and not session.get("offered")
                    and session.get("ads") == "granted"):
                action = {"action_id": f"assist:{session_key}", "session_id": session["session_id"],
                          "session_key": session_key,
                          "action": "assistance_eligible", "reason": "checkout_without_known_purchase"}
                session["offered"] = True
                self.actions.append(action)

    def process(self, raw):
        before = len(self.actions)
        try:
            e = normalize(raw)
        except (ValueError, TypeError, KeyError) as exc:
            self.stats["invalid"] += 1
            # La DLQ educativa guarda solamente el motivo; no copia el payload ni PII.
            self.dlq.append({"reason": str(exc)})
            return []
        if e is None:
            self.stats["consent_dropped"] += 1
            return []
        if e["event_id"] in self.seen:
            self.stats["duplicates"] += 1
            return []
        self.seen.add(e["event_id"])
        sid, ts, name = e["session_id"], e["timestamp_ms"], e["event"]
        identity = (e["client_id"], sid)
        session_key = ':'.join(identity)
        s = self.sessions.setdefault(identity, {"session_id": sid, "events": set(),
                                               "ads": e.get("ad_personalization", "denied")})
        s["ads"] = e.get("ad_personalization", "denied")
        # En este laboratorio sólo el origen backend representa una compra autoritativa.
        confirmed = name == "purchase" and e["source"] == "orders-service"
        if confirmed:
            tx = e["transaction_id"]
            if tx in self.transactions:
                self.stats["duplicate_transactions"] += 1
                return []
            self.transactions.add(tx)
            s["purchased"] = True
            if s.get("offered") and not s.get("cancelled"):
                s["cancelled"] = True
                self.actions.append({"action_id": f"cancel:{session_key}", "cancels": f"assist:{session_key}",
                                     "session_id": sid, "session_key": session_key,
                                     "action": "cancel_assistance"})
        if ts <= self.watermark:
            self.stats["late"] += 1
            return self.actions[before:]
        self.stats["accepted"] += 1
        s["events"].add(name)
        if name == "begin_checkout":
            s["checkout"] = min(ts, s.get("checkout", ts))
        # Moneda forma parte de la clave: nunca sumar ARS con USD.
        key = (ts // 60000 * 60000, e.get("currency", "NA"), e.get("campaign", "unknown"))
        w = self.windows.setdefault(key, {"events": 0, "sessions": set(), "revenue": 0.0, "purchases": 0})
        w["events"] += 1
        w["sessions"].add(identity)
        if confirmed:
            w["revenue"] += e["value"]
            w["purchases"] += 1
        self.max_time = max(self.max_time, ts)
        self.watermark = self.max_time - self.lateness_ms
        self._timers()
        return self.actions[before:]

    def finish(self):
        """Sólo para input finito: representa end-of-input, no un reloj para producción."""
        before = len(self.actions)
        self.watermark = max(self.max_time + self.wait_ms + 1, self.watermark)
        self._timers()
        return self.actions[before:]

    def report(self):
        return {"stats": self.stats, "actions": self.actions, "dlq": self.dlq,
                "windows": [{"start_ms": k[0], "currency": k[1], "campaign": k[2],
                             "events": v["events"], "sessions": len(v["sessions"]),
                             "revenue": round(v["revenue"], 2), "purchases": v["purchases"]}
                            for k, v in sorted(self.windows.items())],
                "funnel_reach": {name: sum(name in s["events"] for s in self.sessions.values())
                                 for name in sorted(ALLOWED_EVENTS)}}
