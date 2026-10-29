#!/usr/bin/env python3
"""Ejecuta una ventana en Flink real y comprueba sus dos filas de resultado."""
import json
import os
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]


def main():
    flink = Path(os.environ.get('FLINK_HOME', ROOT / '.runtime' / 'flink-2.1.3'))
    client = flink / 'bin' / 'sql-client.sh'
    if not client.is_file():
        raise SystemExit('Primero ejecutar python3 scripts/setup_flink.py o configurar FLINK_HOME')
    run = ROOT / 'artifacts' / ('flink-bounded-' + uuid.uuid4().hex[:8])
    run.mkdir(parents=True)
    base = 1793260800000
    rows = [{'event_id': 'e1', 'entity_id': 'a', 'event_time_ms': base+1000, 'amount': 10.0},
            {'event_id': 'e2', 'entity_id': 'a', 'event_time_ms': base+30000, 'amount': 20.0},
            {'event_id': 'e3', 'entity_id': 'b', 'event_time_ms': base+61000, 'amount': 50.0}]
    input_file, output = run / 'input.jsonl', run / 'output'
    input_file.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    sql = (ROOT / 'sql/local/01_bounded_windows.sql').read_text()
    sql = sql.replace('__INPUT__', str(input_file).replace("'", "''")).replace('__OUTPUT__', str(output).replace("'", "''"))
    sqlfile = run / 'job.sql'
    sqlfile.write_text(sql)
    env = dict(os.environ, TERM='dumb')
    result = subprocess.run([str(client), '-f', str(sqlfile)], env=env, text=True,
                            capture_output=True, timeout=180)
    (run / 'sql-client.log').write_text(result.stdout + '\n' + result.stderr)
    actual = []
    for path in output.rglob('*') if output.exists() else []:
        if path.is_file() and not path.name.startswith('.'):
            actual.extend(json.loads(line) for line in path.read_text().splitlines() if line.strip())
    summary = sorted((r['entity_id'], r['observations'], r['total_amount']) for r in actual)
    if result.returncode != 0 or summary != [('a', 2, 30.0), ('b', 1, 50.0)]:
        raise SystemExit(f'Flink no verificó el resultado. Ver {run}/sql-client.log; obtenido={summary}')
    report = {'verified': True, 'input_count': 3, 'output': actual, 'engine': 'Apache Flink 2.1.3'}
    (run / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    print('Evidencia:', run)


if __name__ == '__main__':
    main()
