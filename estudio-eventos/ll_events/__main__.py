import argparse
import json
from pathlib import Path
from .generate import risk_history, risk_stream, marketing_events
from .ml import Logistic, DecisionEngine, features, metrics
from .marketing import MarketingEngine, normalize


def write_json(path, obj):
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


def write_jsonl(path, rows):
    path.write_text("".join(json.dumps(x, ensure_ascii=False) + "\n" for x in rows))


def run_ml(out, online=False):
    history = risk_history()
    train, validation, test = history[:1000], history[1000:1200], history[1200:]
    model = Logistic().fit(train)
    train_rate = sum(e["label"] for e in train) / len(train)
    report = {"split": {"train": len(train), "validation": len(validation), "test": len(test)},
              "threshold_source": "C_FP/(C_FP+C_FN)=30/(30+90); no optimizado sobre test",
              "validation": metrics([(model.predict(features(e)), e["label"]) for e in validation]),
              "test": metrics([(model.predict(features(e)), e["label"]) for e in test]),
              "constant_baseline_test": metrics([(train_rate, e["label"]) for e in test])}
    write_json(out / "model.json", {"weights": model.weights, "version": model.version,
                                   "feature_order": ["bias", "amount/500", "device_risk", "attempts_1m/10"]})
    engine = DecisionEngine(model, online=online)
    stream, decisions = risk_stream(), []
    for event in stream:
        decision = engine.process(event)
        if decision:
            decisions.append(decision)
    report["stream"] = engine.report()
    write_jsonl(out / "risk-events.jsonl", stream)
    write_jsonl(out / "decisions.jsonl", decisions)
    write_json(out / "ml-report.json", report)
    return report


def run_marketing(out):
    raw = marketing_events()
    engine = MarketingEngine()
    for event in raw:
        engine.process(event)
    engine.finish()
    write_jsonl(out / "marketing-raw.jsonl", raw)
    canonical = []
    for event in raw:
        try:
            value = normalize(event)
            if value is not None:
                canonical.append(value)
        except ValueError:
            pass
    write_jsonl(out / "marketing-canonical.jsonl", canonical)
    write_json(out / "marketing-report.json", engine.report())
    return engine.report()


def main():
    parser = argparse.ArgumentParser(description="L&L: laboratorio sintético reproducible")
    parser.add_argument("command", choices=["demo-ml", "demo-marketing", "demo-all"])
    parser.add_argument("--out", default="artifacts")
    parser.add_argument("--online-learning", action="store_true")
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    results = {}
    if args.command in ("demo-ml", "demo-all"):
        results["ml"] = run_ml(out, args.online_learning)
    if args.command in ("demo-marketing", "demo-all"):
        results["marketing"] = run_marketing(out)
    print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
