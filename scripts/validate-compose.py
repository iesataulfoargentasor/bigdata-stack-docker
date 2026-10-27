"""Validación sin arrancar contenedores: python scripts/validate-compose.py."""
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def config(file, profile=None):
    command = ['docker', 'compose', '-f', file]
    if profile:
        command += ['--profile', profile]
    result = subprocess.run(command + ['config', '--format', 'json'], cwd=ROOT,
                            check=True, capture_output=True, encoding='utf-8')
    model = json.loads(result.stdout)
    services = model['services']
    # Todas las dependencias obligatorias deben existir en el escenario elegido.
    for name, service in services.items():
        for dependency, options in service.get('depends_on', {}).items():
            if options.get('required', True):
                assert dependency in services, (name, dependency)
        for mount in service.get('volumes', []):
            if mount['type'] == 'bind':
                assert Path(mount['source']).exists(), mount['source']
    # Orden topológico: ningún ciclo de arranque.
    pending = set(services)
    while pending:
        ready = {name for name in pending if not
                 (set(services[name].get('depends_on', {})) & pending)}
        assert ready, f'Dependencias circulares: {pending}'
        pending -= ready
    print(f'OK {file} {profile or ""}: {len(services)} servicios')
    return model


models = [config('docker-compose.stack.yml', profile) for profile in
          ['hadoop', 'minio', 'hive', 'trino', 'lab', 'airflow', 'lake', 'elt', 'all']]
for file in ['docker-compose.yml', 'docker-compose.hub.yml']:
    config(file)
kafka = config('docker-compose.kafka.yml')
ports = set()
for model in [models[-1], kafka]:
    for service in model['services'].values():
        for port in service.get('ports', []):
            key = (port.get('host_ip', '0.0.0.0'), str(port['published']), port['protocol'])
            assert key not in ports, f'Colisión de puertos: {key}'
            ports.add(key)
print('OK: stack completo y Kafka no comparten puertos publicados')
