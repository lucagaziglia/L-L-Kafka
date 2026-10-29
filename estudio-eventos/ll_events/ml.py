"""Regresión logística transparente; inferencia separada de etiquetas demoradas."""
import math
from dataclasses import dataclass, field

FEATURES = ["amount / 500", "device_risk", "attempts_1m / 10"]


def features(event):
    p = event["payload"]
    values = [float(p["amount"]) / 500, float(p["device_risk"]), float(p["attempts_1m"]) / 10]
    if not all(math.isfinite(x) and x >= 0 for x in values):
        raise ValueError("Features no finitas o negativas")
    if values[1] > 1:
        raise ValueError("device_risk fuera de [0,1]")
    return [1.0] + [min(5.0, x) for x in values]


@dataclass
class Logistic:
    weights: list = field(default_factory=lambda: [0.0] * 4)
    version: str = "logistic-v1"

    def predict(self, x):
        if len(x) != len(self.weights):
            raise ValueError("Dimensiones incompatibles")
        z = max(-35.0, min(35.0, sum(w * v for w, v in zip(self.weights, x))))
        return 1.0 / (1.0 + math.exp(-z))

    def learn(self, x, y, rate=0.08):
        if y not in (0, 1):
            raise ValueError("Etiqueta debe ser 0 o 1")
        error = self.predict(x) - y
        self.weights = [w - rate * (error * v + (0.001 * w if i else 0))
                        for i, (w, v) in enumerate(zip(self.weights, x))]

    def fit(self, events, epochs=18):
        for _ in range(epochs):
            for event in events:
                self.learn(features(event), event["label"])
        return self


def metrics(rows, threshold=0.25):
    if not rows:
        return {"n": 0}
    tp = fp = tn = fn = 0
    for p, y in rows:
        if p >= threshold:
            tp += y
            fp += 1 - y
        else:
            tn += 1 - y
            fn += y
    n = len(rows)
    return {"n": n, "precision": tp / max(1, tp + fp), "recall": tp / max(1, tp + fn),
            "brier": sum((p - y) ** 2 for p, y in rows) / n,
            "log_loss": -sum(y * math.log(max(p, 1e-12)) + (1-y) * math.log(max(1-p, 1e-12))
                             for p, y in rows) / n,
            "expected_cost_per_event": (30 * fp + 90 * fn) / n,
            "confusion": {"tp": tp, "fp": fp, "tn": tn, "fn": fn}}


class DecisionEngine:
    """Estado en memoria para un replay finito. No ofrece recuperación transaccional."""
    def __init__(self, model, online=False, threshold=0.25):
        self.model, self.online, self.threshold = model, online, threshold
        self.seen, self.pending, self.labeled = set(), {}, set()
        self.evaluated = {"normal": [], "drift": []}
        self.duplicates = 0

    def process(self, event):
        if event["event_id"] in self.seen:
            self.duplicates += 1
            return None
        self.seen.add(event["event_id"])
        if event["event_type"] == "risk.observed.v1":
            x = features(event)
            probability = self.model.predict(x)  # Antes de conocer y.
            self.pending[event["event_id"]] = (x, probability, event.get("phase", "normal"))
            return {"decision_id": f"decision:{event['event_id']}:{self.model.version}",
                    "observation_id": event["event_id"], "entity_id": event["entity_id"],
                    "event_time_ms": event["event_time_ms"], "model_version": self.model.version,
                    "probability": probability, "threshold": self.threshold,
                    "action": "review" if probability >= self.threshold else "allow",
                    "learning_mode": "online" if self.online else "frozen"}
        if event["event_type"] != "risk.label_arrived.v1":
            raise ValueError("Tipo de evento desconocido")
        oid = event["observation_id"]
        if oid in self.labeled:
            return None
        if oid not in self.pending:
            raise ValueError("Etiqueta sin observación previa; preservar orden por entidad")
        y = event["label"]
        if y not in (0, 1):
            raise ValueError("Etiqueta inválida")
        x, probability, phase = self.pending.pop(oid)
        self.labeled.add(oid)
        self.evaluated.setdefault(phase, []).append((probability, y))
        if self.online:
            self.model.learn(x, y)  # Nunca recalcular la predicción histórica.
        return None

    def report(self):
        return {"mode": "online" if self.online else "frozen", "duplicates": self.duplicates,
                "pending_labels": len(self.pending),
                "phases": {phase: metrics(rows, self.threshold) for phase, rows in self.evaluated.items()}}
