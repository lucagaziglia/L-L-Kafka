#!/usr/bin/env python3
"""Publica fixtures a Kafka local para consultar desde Flink. No llama servicios cloud."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ll_events.generate import risk_stream, marketing_events
from ll_events.marketing import normalize


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', choices=['ml', 'marketing'], required=True)
    parser.add_argument('--bootstrap', default='127.0.0.1:19092')
    args = parser.parse_args()
    env = dict(os.environ)
    paths = sorted(ROOT.parent.glob('kafka_2.13-*'))
    kafka = Path(env.get('KAFKA_HOME', str(paths[-1]) if paths else ''))
    if not (kafka / 'bin/kafka-topics.sh').is_file():
        raise SystemExit('Configurar KAFKA_HOME')
    jdks = sorted((ROOT.parent / '.tools').glob('jdk-*/Contents/Home'))
    if not env.get('JAVA_HOME') and jdks:
        env['JAVA_HOME'] = str(jdks[-1])
    topic = 'll.risk.raw.v1' if args.case == 'ml' else 'll.marketing.canonical.v1'
    subprocess.run([str(kafka / 'bin/kafka-topics.sh'), '--bootstrap-server', args.bootstrap,
                    '--create', '--if-not-exists', '--topic', topic, '--partitions', '1',
                    '--replication-factor', '1'], env=env, check=True)
    if args.case == 'ml':
        # Este ejercicio aísla ventanas: observations únicas, sin labels ni redeliveries.
        seen = set()
        rows = []
        for e in risk_stream(80):
            if e['event_type'] == 'risk.observed.v1' and e['event_id'] not in seen:
                seen.add(e['event_id'])
                rows.append(e)
        rows.append({'event_id': 'fixture-marker', 'entity_id': '__fixture__',
                     'event_type': 'fixture.end_marker',
                     'event_time_ms': max(e['event_time_ms'] for e in rows) + 120000,
                     'payload': {'amount': 0, 'device_risk': 0, 'attempts_1m': 0}})
    else:
        rows = []
        for e in marketing_events():
            try:
                canonical = normalize(e)
                if canonical:
                    rows.append(canonical)
            except ValueError:
                pass
        rows.append({'event_id': 'fixture-marker', 'client_id': '__fixture__', 'session_id': '__fixture__',
                     'event': '_watermark_marker',
                     'timestamp_ms': max(e['timestamp_ms'] for e in rows) + 120000})
    data = ''.join(e.get('entity_id', e.get('client_id', 'fixture')) + '\t' + json.dumps(e) + '\n' for e in rows)
    subprocess.run([str(kafka / 'bin/kafka-console-producer.sh'), '--bootstrap-server', args.bootstrap,
                    '--topic', topic, '--property', 'parse.key=true', '--property', 'key.separator=\t',
                    '--producer-property', 'enable.idempotence=true'], input=data, text=True, env=env, check=True)
    print(f'{len(rows)} registros publicados en {topic}, incluido marcador sintético de cierre de ventanas.')
    print('Usar un sandbox nuevo para repetir sin acumular entradas. Ver docs/04_FLINK_LOCAL.md.')


if __name__ == '__main__':
    main()
