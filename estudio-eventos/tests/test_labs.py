import json
import unittest
from ll_events.generate import risk_history, risk_stream, marketing_events
from ll_events.ml import Logistic, DecisionEngine, features, metrics
from ll_events.marketing import MarketingEngine, normalize


class MachineLearningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.history = risk_history()
        cls.model = Logistic().fit(cls.history[:1000])

    def test_model_improves_brier_against_constant_on_temporal_holdout(self):
        test = self.history[1200:]
        prior = sum(e['label'] for e in self.history[:1000]) / 1000
        learned = metrics([(self.model.predict(features(e)), e['label']) for e in test])
        baseline = metrics([(prior, e['label']) for e in test])
        self.assertLess(learned['brier'], baseline['brier'])

    def test_labels_arrive_after_prediction_and_update_only_then(self):
        stream = risk_stream(10)
        model = Logistic(list(self.model.weights))
        engine = DecisionEngine(model, online=True)
        before = list(model.weights)
        first = engine.process(stream[0])
        self.assertEqual(before, model.weights)
        self.assertNotIn('label', first)
        label = next(e for e in stream if e['event_type'] == 'risk.label_arrived.v1')
        engine.process(label)
        self.assertNotEqual(before, model.weights)
        self.assertEqual(engine.evaluated['normal'][0][0], first['probability'])

    def test_replay_is_deterministic_with_frozen_model(self):
        def replay():
            engine = DecisionEngine(Logistic(list(self.model.weights)))
            return [d for e in risk_stream() if (d := engine.process(e)) is not None], engine.report()
        a, b = replay(), replay()
        self.assertEqual(a, b)
        self.assertEqual(len(a[0]), 240)
        self.assertEqual(a[1]['pending_labels'], 0)
        self.assertGreater(a[1]['duplicates'], 0)

    def test_duplicate_label_does_not_update_twice(self):
        engine = DecisionEngine(Logistic(list(self.model.weights)), online=True)
        stream = risk_stream(2)
        for e in stream:
            engine.process(e)
        weights = list(engine.model.weights)
        label = dict(stream[-1], event_id='redelivery-with-new-id')
        engine.process(label)
        self.assertEqual(weights, engine.model.weights)

    def test_future_label_rejected(self):
        with self.assertRaises(ValueError):
            DecisionEngine(Logistic()).process(next(e for e in risk_stream(2) if 'label' in e))

    def test_feature_validation(self):
        event = risk_history(1)[0]
        event['payload']['amount'] = float('nan')
        with self.assertRaises(ValueError):
            features(event)

    def test_cost_threshold_and_matrix_dimensions(self):
        model = Logistic([0, 0, 0, 0])
        self.assertEqual(model.predict([1, 0, 0, 0]), 0.5)
        with self.assertRaises(ValueError):
            model.predict([1, 2])
        row = DecisionEngine(model).process(risk_stream(1)[0])
        self.assertEqual(row['action'], 'review')
        self.assertEqual(row['threshold'], 30 / (30+90))


class MarketingTests(unittest.TestCase):
    def engine(self):
        engine = MarketingEngine()
        for event in marketing_events():
            engine.process(event)
        engine.finish()
        return engine

    def test_dedup_events_and_transactions(self):
        engine = self.engine()
        self.assertEqual(engine.stats['duplicates'], 1)
        self.assertEqual(engine.stats['duplicate_transactions'], 1)
        self.assertEqual(sum(w['revenue'] for w in engine.windows.values()), 120)

    def test_consent_and_allowlist(self):
        raw = marketing_events()
        self.assertIsNone(normalize(next(e for e in raw if e['analytics_storage'] == 'denied')))
        self.assertNotIn('email', normalize(raw[-1]))
        serialized = json.dumps(self.engine().report())
        self.assertNotIn('never-retained', serialized)
        self.assertNotIn('s3', serialized)

    def test_no_activation_without_ads_consent_or_after_purchase(self):
        actions = self.engine().actions
        sessions = {a['session_id'] for a in actions}
        self.assertNotIn('s1', sessions)
        self.assertNotIn('s4', sessions)
        self.assertIn('s2', sessions)

    def test_late_purchase_compensates_action_without_reopening_revenue(self):
        engine = self.engine()
        self.assertEqual(engine.stats['late'], 1)
        self.assertIn({'action_id': 'cancel:synthetic-s5:s5', 'cancels': 'assist:synthetic-s5:s5',
                       'session_id': 's5', 'session_key': 'synthetic-s5:s5',
                       'action': 'cancel_assistance'}, engine.actions)
        self.assertEqual(sum(w['purchases'] for w in engine.windows.values()), 1)

    def test_invalid_purchase_goes_to_redacted_dlq(self):
        engine = self.engine()
        self.assertEqual(engine.stats['invalid'], 1)
        self.assertEqual(len(engine.dlq), 1)
        self.assertNotIn('client_id', engine.dlq[0])

    def test_currency_is_part_of_group_key(self):
        engine = MarketingEngine()
        raw = marketing_events()[6]
        engine.process(raw)
        engine.process(dict(raw, event_id='other', transaction_id='order-ars', currency='ARS'))
        self.assertEqual({k[1] for k in engine.windows}, {'USD', 'ARS'})

    def test_timer_waits_for_watermark_and_is_idempotent(self):
        engine = MarketingEngine()
        checkout = next(e for e in marketing_events() if e['event'] == 'begin_checkout')
        engine.process(checkout)
        self.assertEqual(engine.actions, [])
        engine.finish()
        engine.finish()
        self.assertEqual(len(engine.actions), 1)

    def test_negative_and_infinite_revenue_rejected(self):
        raw = next(e for e in marketing_events() if e['event'] == 'purchase')
        for value in [-1, float('inf'), True]:
            with self.assertRaises(ValueError):
                normalize(dict(raw, value=value))

    def test_session_id_is_not_global_across_clients(self):
        engine = MarketingEngine()
        checkout = next(e for e in marketing_events() if e['event'] == 'begin_checkout')
        engine.process(checkout)
        purchase = next(e for e in marketing_events() if e['event'] == 'purchase')
        engine.process(dict(purchase, session_id=checkout['session_id'], client_id='another-client'))
        engine.finish()
        self.assertEqual(len(engine.sessions), 2)
        self.assertEqual(engine.actions[0]['action'], 'assistance_eligible')


if __name__ == '__main__':
    unittest.main()
