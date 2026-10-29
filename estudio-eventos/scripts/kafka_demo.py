#!/usr/bin/env python3
"""Prueba real Kafka -> consumidor Python -> Kafka. Broker efímero y aislado."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ll_events.generate import risk_history, risk_stream, marketing_events
from ll_events.ml import Logistic, DecisionEngine
from ll_events.marketing import MarketingEngine


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', choices=['ml', 'marketing'], default='ml')
    parser.add_argument('--port', type=int, default=19092)
    parser.add_argument('--controller-port', type=int, default=19093)
    parser.add_argument('--serve-only', action='store_true', help='Mantener broker sandbox hasta Ctrl+C')
    args = parser.parse_args()
    if args.port == args.controller_port:
        raise SystemExit('Broker y controller requieren puertos diferentes')
    for port in [args.port, args.controller_port]:
        with socket.socket() as sock:
            if sock.connect_ex(('127.0.0.1', port)) == 0:
                raise SystemExit(f'Puerto {port} ocupado; elegir otro. No se detuvo ningún proceso.')
    project = ROOT.parent
    kafka = os.environ.get('KAFKA_HOME')
    if not kafka:
        paths = sorted(project.glob('kafka_2.13-*'))
        if not paths:
            raise SystemExit('Configurar KAFKA_HOME con una distribución instalada de Kafka')
        kafka = str(paths[-1])
    kafka = Path(kafka).resolve()
    env = dict(os.environ)
    if not env.get('JAVA_HOME'):
        jdks = sorted((project / '.tools').glob('jdk-*/Contents/Home'))
        if jdks:
            env['JAVA_HOME'] = str(jdks[-1])
    env['KAFKA_HEAP_OPTS'] = '-Xms256m -Xmx512m'
    run_id = uuid.uuid4().hex[:10]
    runtime = ROOT / 'artifacts' / f'kafka-{args.case}-{run_id}'
    runtime.mkdir(parents=True)
    bootstrap = f'127.0.0.1:{args.port}'

    def command(tool, *values):
        return [str(kafka / 'bin' / (tool + '.sh')), *map(str, values)]

    def run(tool, *values, **kwargs):
        return subprocess.run(command(tool, *values), env=env, text=True, check=True,
                              capture_output=True, timeout=45, **kwargs)

    config = runtime / 'server.properties'
    config.write_text(f'''process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:{args.controller_port}
listeners=PLAINTEXT://127.0.0.1:{args.port},CONTROLLER://127.0.0.1:{args.controller_port}
advertised.listeners=PLAINTEXT://127.0.0.1:{args.port}
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs={runtime / 'data'}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
''')
    cluster_id = run('kafka-storage', 'random-uuid').stdout.strip()
    run('kafka-storage', 'format', '--standalone', '-t', cluster_id, '-c', config)
    logfile = (runtime / 'broker.log').open('w')
    broker = subprocess.Popen(command('kafka-server-start', config), env=env, stdout=logfile, stderr=logfile)
    children = []
    try:
        for _ in range(60):
            if broker.poll() is not None:
                raise RuntimeError(f'Kafka no arrancó. Revisar {runtime}/broker.log')
            with socket.socket() as sock:
                if sock.connect_ex(('127.0.0.1', args.port)) == 0:
                    break
            time.sleep(0.5)
        else:
            raise RuntimeError('Timeout esperando Kafka')
        if args.serve_only:
            print(f'Kafka sandbox en {bootstrap}; datos en {runtime}. Ctrl+C para detener.', flush=True)
            try:
                while broker.poll() is None:
                    time.sleep(1)
            except KeyboardInterrupt:
                pass
            return
        topics = [f'll.{args.case}.input.{run_id}', f'll.{args.case}.decisions.{run_id}']
        for topic in topics:
            run('kafka-topics', '--bootstrap-server', bootstrap, '--create', '--topic', topic,
                '--partitions', 1, '--replication-factor', 1)
        events = risk_stream(80) if args.case == 'ml' else marketing_events()
        engine = (DecisionEngine(Logistic().fit(risk_history()[:1000]))
                  if args.case == 'ml' else MarketingEngine())
        diagnostics = (runtime / 'clients.log').open('w')

        def producer(topic):
            p = subprocess.Popen(command('kafka-console-producer', '--bootstrap-server', bootstrap,
                 '--topic', topic, '--property', 'parse.key=true', '--property', 'key.separator=\t',
                 '--producer-property', 'enable.idempotence=true', '--producer-property', 'acks=all'),
                 env=env, text=True, stdin=subprocess.PIPE, stdout=diagnostics, stderr=diagnostics)
            children.append(p)
            return p

        output = producer(topics[1])
        consumer = subprocess.Popen(command('kafka-console-consumer', '--bootstrap-server', bootstrap,
            '--topic', topics[0], '--from-beginning', '--group', f'll-{run_id}',
            '--max-messages', len(events), '--timeout-ms', 30000), env=env, text=True,
            stdout=subprocess.PIPE, stderr=diagnostics, bufsize=1)
        children.append(consumer)
        errors = []

        def publish():
            try:
                p = producer(topics[0])
                for event in events:
                    key = event.get('entity_id', event.get('session_id', 'unknown'))
                    p.stdin.write(key + '\t' + json.dumps(event) + '\n')
                    p.stdin.flush()
                    time.sleep(0.015)
                p.stdin.close()
                if p.wait(timeout=45) != 0:
                    raise RuntimeError('Falló el productor; revisar clients.log')
            except Exception as exc:
                errors.append(str(exc))

        thread = threading.Thread(target=publish, daemon=True)
        thread.start()
        decisions, received = [], 0
        for line in consumer.stdout:
            event = json.loads(line)
            received += 1
            result = engine.process(event)
            values = ([result] if result else []) if args.case == 'ml' else result
            for decision in values:
                decisions.append(decision)
                key = decision.get('entity_id', decision.get('session_id'))
                output.stdin.write(key + '\t' + json.dumps(decision) + '\n')
                output.stdin.flush()
        if args.case == 'marketing':
            for decision in engine.finish():
                decisions.append(decision)
                output.stdin.write(decision['session_id'] + '\t' + json.dumps(decision) + '\n')
        output.stdin.close()
        if output.wait(timeout=45) != 0 or consumer.wait(timeout=10) != 0:
            raise RuntimeError('Falló un cliente; revisar clients.log')
        thread.join(timeout=50)
        if thread.is_alive() or errors or received != len(events):
            raise RuntimeError(f'Publicación incompleta: {received}/{len(events)}, {errors}')
        if not decisions:
            raise RuntimeError('No se generaron decisiones')
        replay = run('kafka-console-consumer', '--bootstrap-server', bootstrap, '--topic', topics[1],
                     '--from-beginning', '--max-messages', len(decisions), '--timeout-ms', 15000)
        persisted = [json.loads(line) for line in replay.stdout.splitlines() if line.strip()]
        if persisted != decisions:
            raise RuntimeError('La lectura del topic no coincide con las decisiones emitidas')
        report = {'case': args.case, 'input_count': received, 'decision_count': len(decisions),
                  'persisted_count': len(persisted), 'topics': topics, 'broker_port': args.port,
                  'verified': persisted == decisions, 'engine': engine.report()}
        (runtime / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        (runtime / 'decisions.jsonl').write_text(''.join(json.dumps(d) + '\n' for d in decisions))
        print(json.dumps(report, indent=2))
        print(f'Evidencia: {runtime}')
        diagnostics.close()
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
        broker.terminate()
        try:
            broker.wait(timeout=20)
        except subprocess.TimeoutExpired:
            broker.kill()
            broker.wait(timeout=5)
        logfile.close()


if __name__ == '__main__':
    main()
