#!/usr/bin/env python3
"""Instala una distribución fijada y verifica SHA512 antes de extraer."""
import hashlib
from pathlib import Path
import re
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[1]
VERSION = '2.1.3'
CONNECTOR = '5.0.0-2.1'
RUNTIME = ROOT / '.runtime'


def download_verified(url, destination, checksum_url=None, algorithm='sha512'):
    checksum = destination.with_name(destination.name + '.' + algorithm)
    if not destination.exists():
        subprocess.run(['curl', '-fsSL', '--retry', '2', '-o', str(destination), url], check=True)
    subprocess.run(['curl', '-fsSL', '--retry', '2', '-o', str(checksum), checksum_url or url + '.' + algorithm], check=True)
    width = 128 if algorithm == 'sha512' else 40
    candidates = re.findall(r'\b[0-9a-fA-F]{' + str(width) + r'}\b', checksum.read_text())
    if not candidates:
        raise RuntimeError('Checksum no encontrado')
    with destination.open('rb') as handle:
        if hasattr(hashlib, 'file_digest'):
            digest = hashlib.file_digest(handle, algorithm).hexdigest()
        else:
            h = hashlib.new(algorithm)
            for block in iter(lambda: handle.read(1024 * 1024), b''):
                h.update(block)
            digest = h.hexdigest()
    if digest.lower() != candidates[0].lower():
        raise RuntimeError(f'Checksum incorrecto: {destination}; eliminar descarga y reintentar')
    print(algorithm.upper(), 'verificado:', destination.name)


def main():
    RUNTIME.mkdir(exist_ok=True)
    archive = RUNTIME / f'flink-{VERSION}.tgz'
    url = f'https://downloads.apache.org/flink/flink-{VERSION}/flink-{VERSION}-bin-scala_2.12.tgz'
    download_verified(url, archive)
    flink = RUNTIME / f'flink-{VERSION}'
    if not flink.exists():
        with tarfile.open(archive) as tar:
            # Archive oficial verificado; además, impedir paths fuera del runtime.
            for member in tar.getmembers():
                path = (RUNTIME / member.name).resolve()
                if not path.is_relative_to(RUNTIME.resolve()) or member.issym() or member.islnk():
                    raise RuntimeError('Miembro de archivo no permitido')
            tar.extractall(RUNTIME, filter='data') if hasattr(tarfile, 'data_filter') else tar.extractall(RUNTIME)
    jar = flink / 'lib' / f'flink-sql-connector-kafka-{CONNECTOR}.jar'
    download_verified(f'https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/{CONNECTOR}/{jar.name}', jar, algorithm='sha1')
    print(f'FLINK_HOME={flink}')
    print('Java 17 recomendado; Java 21 es experimental en esta rama. Ver docs/04_FLINK_LOCAL.md.')


if __name__ == '__main__':
    main()
