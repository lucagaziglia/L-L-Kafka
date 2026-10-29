#!/usr/bin/env python3
"""Verifica Kafka -> Flink con snapshots finitos; no mide latencia streaming."""
import json
import math
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ll_events.generate import risk_stream


def main():
    env = dict(os.environ, TERM='dumb')
    flink = Path(env.get('FLINK_HOME', ROOT / '.runtime/flink-2.1.3'))
    client = flink / 'bin/sql-client.sh'
    if not client.exists():
        raise SystemExit('Instalar Flink con scripts/setup_flink.py')
    with socket.socket() as sock:
        if sock.connect_ex(('127.0.0.1', 19092)) == 0:
            raise SystemExit('Puerto 19092 ocupado. Detener tu sandbox anterior antes de esta prueba.')
    run = ROOT / 'artifacts' / ('flink-kafka-' + uuid.uuid4().hex[:8])
    run.mkdir(parents=True)
    with (run / 'sandbox.log').open('w') as broker_log:
        broker = subprocess.Popen([sys.executable, str(ROOT / 'scripts/kafka_demo.py'), '--serve-only'],
                                  env=env, cwd=ROOT, text=True, stdout=broker_log, stderr=broker_log)
        try:
            for _ in range(90):
                if broker.poll() is not None:
                    raise RuntimeError('Falló sandbox; revisar sandbox.log')
                with socket.socket() as sock:
                    if sock.connect_ex(('127.0.0.1', 19092)) == 0:
                        break
                time.sleep(0.5)
            else:
                raise RuntimeError('No arrancó broker')
            reports = {}
            for case in ['ml', 'marketing']:
                publish = subprocess.run([sys.executable, str(ROOT / 'scripts/publish_fixture.py'), '--case', case],
                                         cwd=ROOT, env=env, text=True, capture_output=True, timeout=90)
                (run / f'publish-{case}.log').write_text(publish.stdout + publish.stderr)
                publish.check_returncode()
                filename = '02_risk_windows.sql' if case == 'ml' else '03_marketing_windows.sql'
                sql = (ROOT / 'sql/local' / filename).read_text()
                marker = '-- Consulta continua;' if case == 'ml' else 'SELECT window_start, window_end, campaign, currency,'
                at = sql.index(marker)
                if case == 'ml':
                    query = sql[sql.index('SELECT entity_id, window_start', at):]
                else:
                    query = sql[at:]
                prefix = sql[:at].replace("'scan.startup.mode' = 'earliest-offset',",
                                         "'scan.startup.mode' = 'earliest-offset',\n  'scan.bounded.mode' = 'latest-offset',")
                output = run / f'output-{case}'
                fields = ('entity_id STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3), '
                          'observations BIGINT, total_amount DOUBLE' if case == 'ml' else
                          'window_start TIMESTAMP(3), window_end TIMESTAMP(3), campaign STRING, '
                          'currency STRING, events BIGINT, observed_sessions BIGINT, backend_revenue DOUBLE')
                setup = "SET 'execution.target' = 'local';\nSET 'execution.runtime-mode' = 'batch';\nSET 'table.dml-sync' = 'true';\n"
                sink = f"CREATE TABLE checked_output ({fields}) WITH ('connector'='filesystem','format'='json','path'='{output}');\n"
                jobfile = run / f'{case}.sql'
                jobfile.write_text(setup + prefix + sink + 'INSERT INTO checked_output\n' + query)
                result = subprocess.run([str(client), '-f', str(jobfile)], cwd=ROOT, env=env,
                                        capture_output=True, text=True, timeout=180)
                (run / f'flink-{case}.log').write_text(result.stdout + result.stderr)
                rows = []
                for path in output.rglob('*') if output.exists() else []:
                    if path.is_file() and not path.name.startswith('.'):
                        rows.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
                if result.returncode != 0 or not rows:
                    raise RuntimeError(f'Flink falló: revisar {run}/flink-{case}.log')
                if case == 'ml':
                    unique = {e['event_id']: e for e in risk_stream(80) if e['event_type'] == 'risk.observed.v1'}
                    count = sum(r['observations'] for r in rows)
                    amount = sum(r['total_amount'] for r in rows)
                    expected = sum(e['payload']['amount'] for e in unique.values())
                    verified = count == 80 and math.isclose(amount, expected, abs_tol=1e-6)
                else:
                    count = sum(r['events'] for r in rows)
                    amount = sum(r['backend_revenue'] for r in rows)
                    # En snapshot batch se incluye la compra tardía; no aplica descarte por watermark.
                    verified = count == 12 and math.isclose(amount, 195.0, abs_tol=1e-6)
                reports[case] = {'verified': verified, 'count': count, 'amount': amount, 'windows': rows}
                if not verified:
                    raise RuntimeError(f'Resultados inesperados: {reports[case]}')
            report = {'engine': 'Apache Flink 2.1.3 + Kafka connector 5.0.0-2.1',
                      'mode': 'batch sobre snapshot acotado de Kafka; no prueba tardanza streaming',
                      'verified': True, 'cases': reports}
            (run / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps(report, indent=2))
            print('Evidencia:', run)
        finally:
            if broker.poll() is None:
                broker.send_signal(signal.SIGINT)
                try:
                    broker.wait(timeout=40)
                except subprocess.TimeoutExpired:
                    broker.terminate()
                    broker.wait(timeout=20)


if __name__ == '__main__':
    main()
