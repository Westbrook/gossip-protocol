"""Pure created-state proof for the trusted storage keeper, before any start.

The keeper is root with init; candidate adapters are separately launched as
65534:65534. This module admits observed Engine state, never command flags alone.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
from typing import Any

from . import candidate_client_process_v4 as process

PROTOCOL = 'candidate-storage-created-prestart-v2-desktop-inputs-v1'
MOUNT_POLICY = 'candidate-storage-desktop-inputs-source-v1'
RUNTIME_ORIGINAL_NAMES = ('runtime-version-request.bin', 'runtime-version-response.bin',
                          'runtime-info-request.bin', 'runtime-info-response.bin')
COMMAND = ('python', '-I', '-c', 'import time;time.sleep(1800)')
PROOF_FILE = 'container-prestart-proof.json'
ORDER = ('volume-created-dispatch.json', 'volume-created-stdout.bin', 'volume-created.json',
         'container-create-dispatch.json', 'container-create-stdout.bin', 'container-create.json', 'container-prestart-dispatch.json',
         'container-prestart-stdout.bin', 'container-prestart.json', PROOF_FILE,
         'container-start-dispatch.json', 'container-start.json', 'session-dispatch.json')
LOADED_SOURCE_SHA256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


class PrestartError(ValueError):
    pass


def require(ok: bool, message: str) -> None:
    if not ok:
        raise PrestartError(message)


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True, allow_nan=False).encode('ascii')


def exact(left: Any, right: Any) -> bool:
    return encoded(left) == encoded(right)


def definition() -> dict[str, Any]:
    return {'protocol': PROTOCOL, 'keeper_user': '0:0', 'candidate_exec_user': '65534:65534',
            'keeper_init': True, 'keeper_command': list(COMMAND),
            'inspection': 'complete raw docker inspect after create before start',
            'bind_source_policy': bind_source_policy(),
            'running_identity': 'all immutable fields exact; complete actual Mounts rows keyed by destination',
            'running_lineage': 'zero typed restart count; stable positive PID and nonzero StartedAt after first observed running state',
            'startup_exception': 'OomKillDisable false to null only for qualified retained runtime',
            'later_running_oom': 'exact first running representation; no repeated startup exception',
            'volume': 'original volume-created inspection must match owned local tmpfs driver options before container create',
            'startup_policy_sha256': process.startup_policy_sha256(),
            'chronology': list(ORDER), 'authority': 'authenticated originals and independently derived expectation'}


def _canonical_path(value: Any) -> bool:
    # Pure lexical check: retained staging paths may no longer exist at read time.
    return (type(value) is str and value.startswith('/') and '\\' not in value
            and '\x00' not in value and all(part not in ('', '.', '..') for part in value.split('/')[1:]))


def bind_source_policy() -> dict[str, Any]:
    return {'protocol': MOUNT_POLICY, 'destination': '/inputs', 'host_prefix': '/private/',
        'engine_prefix': '/host_mnt', 'rule': 'literal or exact engine_prefix + complete canonical host source',
        'declaration': 'actual and HostConfig Source must agree exactly',
        'other_destinations': 'literal only', 'running': 'complete observed representation remains exact',
        'runtime': {'protocol': process.PROTOCOL, 'api_version': process.API_VERSION,
            'os': 'linux', 'engine_version': '29.2.1', 'engine_git_commit': '6bc6209',
            'architecture': 'arm64', 'kernel_version': '6.12.72-linuxkit',
            'cgroup_driver': 'cgroupfs', 'cgroup_version': '2', 'oom_kill_disable_supported': False},
        'version': {'Platform': {'Name': 'Docker Desktop 4.64.0 (221278)'}, 'Version': '29.2.1',
            'GitCommit': '6bc6209', 'Os': 'linux', 'Arch': 'arm64', 'KernelVersion': '6.12.72-linuxkit',
            'ApiVersion': '1.53', 'MinAPIVersion': '1.44'},
        'info': {'OperatingSystem': 'Docker Desktop', 'Name': 'docker-desktop', 'OSType': 'linux',
            'Architecture': 'aarch64', 'KernelVersion': '6.12.72-linuxkit', 'ServerVersion': '29.2.1',
            'CgroupVersion': '2', 'CgroupDriver': 'cgroupfs', 'OomKillDisable': False},
        'originals': list(RUNTIME_ORIGINAL_NAMES),
        'original_parser': 'bounded retained HTTP using frozen process._Wire framing; no network',
        'unqualified': 'alias unavailable; never normalize arbitrary Linux paths'}


class _RetainedRuntimeWire(process._Wire):
    """Use the existing bounded HTTP parser over authenticated retained bytes."""
    def __init__(self, raw: bytes):
        require(type(raw) is bytes and len(raw) <= process.HEADER_LIMIT + process.CONTROL_LIMIT
                + process.FRAME_COUNT_LIMIT * 20, 'Bounded runtime HTTP original required')
        self.buffer = bytearray(raw)
        self.eof = False

    def receive(self) -> None:
        self.eof = True


def _desktop_runtime(runtime: dict[str, Any] | None, originals: dict[str, bytes] | None,
                     image_id: str) -> dict[str, str]:
    policy = bind_source_policy()
    require(type(runtime) is dict and process._startup_runtime_valid(runtime, image_id)
            and all(key in runtime and exact(runtime[key], value) for key, value in policy['runtime'].items()),
            'Unqualified Docker Desktop bind-source runtime')
    require(type(originals) is dict and set(originals) == set(RUNTIME_ORIGINAL_NAMES),
            'Complete original Docker Desktop runtime responses required')
    assert runtime is not None and originals is not None
    for kind in ('version', 'info'):
        request = originals['runtime-' + kind + '-request.bin']
        require(type(request) is bytes and request == process._request('GET', '/' + kind),
                'Original runtime request differs')
        wire = _RetainedRuntimeWire(originals['runtime-' + kind + '-response.bin'])
        status, headers = wire.headers()
        require(status == 200, 'Original runtime response failed')
        data = process.strict_json_loads(wire.body(status, headers))
        require(type(data) is dict and all(key in data and exact(data[key], value)
                for key, value in policy[kind].items()), 'Unqualified original Docker Desktop ' + kind)
        if kind == 'info':
            require(data.get('ID') == runtime['daemon_id'], 'Original runtime daemon differs')
    return {name: hashlib.sha256(originals[name]).hexdigest() for name in RUNTIME_ORIGINAL_NAMES}


def _mount_sources(by_target: dict[str, Any], expected: dict[str, str], image_id: str,
                   runtime: dict[str, Any] | None, originals: dict[str, bytes] | None
                   ) -> tuple[dict[str, str], dict[str, Any]]:
    observed: dict[str, str] = {}
    translated = False
    for target, source in expected.items():
        actual = by_target[target].get('Source')
        if actual != source:
            require(target == '/inputs' and source.startswith('/private/')
                    and actual == '/host_mnt' + source and _canonical_path(actual),
                    'Read-only source/helper/fixture mount differs')
            translated = True
        observed[target] = actual
    originals_sha = _desktop_runtime(runtime, originals, image_id) if translated else {}
    binding = {'protocol': MOUNT_POLICY, 'representation': 'docker-desktop-inputs-prefix' if translated else 'literal',
        'host_sources': expected, 'observed_sources': observed,
        'runtime_sha256': hashlib.sha256(encoded(runtime)).hexdigest() if translated else None,
        'runtime_original_sha256': originals_sha}
    return observed, binding


def _mounts(value: dict[str, Any], expected: dict[str, str], volume: str, image_id: str,
            runtime: dict[str, Any] | None, originals: dict[str, bytes] | None) -> dict[str, Any]:
    mounts, host = value['Mounts'], value['HostConfig']
    require(type(mounts) is list and len(mounts) == len(expected) + 1
            and all(type(row) is dict and type(row.get('Destination')) is str for row in mounts),
            'Malformed actual mount inventory')
    by_target = {row.get('Destination'): row for row in mounts}
    require(set(by_target) == set(expected) | {'/tmp'} and len(by_target) == len(mounts),
            'Actual mount inventory differs')
    admitted, binding = _mount_sources(by_target, expected, image_id, runtime, originals)
    for target, source in admitted.items():
        row = by_target[target]
        require(row.get('Type') == 'bind' and row.get('Source') == source
                and row.get('RW') is False and row.get('Propagation') == 'rprivate'
                and row.get('Mode', '') in ('', 'ro'), 'Read-only source/helper/fixture mount differs')
    row = by_target['/tmp']
    require(row.get('Type') == 'volume' and row.get('Name') == volume and row.get('Driver') == 'local'
            and row.get('RW') is True and row.get('Propagation', '') == ''
            and row.get('Mode', '') in ('', 'z') and _canonical_path(row.get('Source')),
            'Owned writable state volume differs')
    declarations = host.get('Mounts')
    require(type(declarations) is list and len(declarations) == len(mounts)
            and all(type(item) is dict and type(item.get('Target')) is str for item in declarations),
            'Complete mount declarations required')
    declared = {item.get('Target'): item for item in declarations}
    require(set(declared) == set(by_target) and len(declared) == len(declarations), 'Mount declarations differ')
    for target, source in admitted.items():
        item = declared[target]
        require(item.get('Type') == 'bind' and item.get('Source') == source
                and item.get('ReadOnly') is True and item.get('Consistency', '') in ('', 'default'),
                'Read-only bind declaration differs')
        options = item.get('BindOptions')
        require(type(options) is dict and options.get('Propagation') == 'rprivate'
                and all(value is False for key, value in options.items() if key != 'Propagation')
                and not item.get('VolumeOptions') and not item.get('TmpfsOptions'), 'Bind options differ')
    item = declared['/tmp']
    require(item.get('Type') == 'volume' and item.get('Source') == volume
            and item.get('ReadOnly', False) is False and not item.get('BindOptions')
            and not item.get('TmpfsOptions') and item.get('Consistency', '') in ('', 'default'),
            'Owned volume declaration differs')
    options = item.get('VolumeOptions')
    require(type(options) is dict and options.get('NoCopy') is True
            and all(key in ('NoCopy', 'Labels', 'DriverConfig', 'Subpath') for key in options)
            and not options.get('Labels') and not options.get('DriverConfig') and not options.get('Subpath'),
            'Owned volume options differ')
    return binding


def validate_created(value: dict[str, Any], *, container_id: str, name: str, image_id: str,
                     volume: str, labels: dict[str, str], mounts: dict[str, str],
                     runtime: dict[str, Any] | None = None,
                     runtime_originals: dict[str, bytes] | None = None) -> dict[str, Any]:
    """Validate observed created state against independent owner expectations."""
    require(type(container_id) is str and re.fullmatch('[0-9a-f]{64}', container_id) is not None
            and type(name) is str and bool(name) and type(volume) is str and bool(volume)
            and type(image_id) is str and re.fullmatch('sha256:[0-9a-f]{64}', image_id) is not None,
            'Exact container/image identities required')
    require(type(labels) is dict and set(labels) == {'gossip.execution', 'gossip.source', 'gossip.fixture'}
            and all(type(item) is str and bool(item) for item in labels.values()), 'Exact owner labels required')
    require(type(mounts) is dict and set(mounts) in ({'/workspace', '/checks'}, {'/workspace', '/checks', '/inputs'})
            and all(_canonical_path(item) for item in mounts.values())
            and len(set(mounts.values())) == len(mounts), 'Exact staging roots required')
    process.immutable_inspection(value)
    config, host = value['Config'], value['HostConfig']
    require(type(config) is dict and type(host) is dict, 'Malformed actual configuration')
    process.validate_oom_kill_default(host)
    require(value['Id'] == container_id and value['Name'] == '/' + name and value['Image'] == image_id
            and process._timestamp(value['Created']) is not None and value.get('RestartCount') == 0
            and type(value.get('RestartCount')) is int, 'Created container identity differs')
    expected_config = {'Image': image_id, 'User': '0:0', 'WorkingDir': '/workspace', 'Tty': False,
        'OpenStdin': False, 'StdinOnce': False, 'AttachStdout': True, 'AttachStderr': True,
        'Healthcheck': {'Test': ['NONE']}, 'Hostname': 'gossip-validator', 'Labels': labels,
        'Entrypoint': [COMMAND[0]], 'Cmd': list(COMMAND[1:])}
    require(all(key in config and exact(config[key], expected) for key, expected in expected_config.items())
            and value['Path'] == COMMAND[0] and exact(value['Args'], list(COMMAND[1:])),
            'Created keeper user/command/configuration differs')
    expected_host = {'NetworkMode': 'none', 'ReadonlyRootfs': True, 'Privileged': False,
        'AutoRemove': False, 'Memory': 256 * 1024 * 1024, 'MemorySwap': 256 * 1024 * 1024,
        'NanoCpus': 1000000000, 'PidsLimit': 64, 'CapDrop': ['ALL'],
        'SecurityOpt': ['no-new-privileges:true'], 'LogConfig': {'Type': 'none', 'Config': {}},
        'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0},
        'Ulimits': [{'Name': 'nofile', 'Hard': 256, 'Soft': 256}], 'Init': True}
    empty_values: tuple[Any, ...] = (None, '', False, [], {})
    require(all(key in host and exact(host[key], expected) for key, expected in expected_host.items())
            and host.get('IpcMode') in ('', 'private') and host.get('CgroupnsMode') in ('', 'private')
            and all(key in host and any(exact(host[key], allowed) for allowed in empty_values) for key in (
                'CapAdd', 'Devices', 'DeviceRequests', 'VolumesFrom', 'Links', 'PortBindings',
                'PublishAllPorts', 'PidMode', 'UTSMode', 'UsernsMode', 'Binds'))
            and not host.get('Tmpfs'), 'Created keeper sandbox restrictions differ')
    mount_binding = _mounts(value, mounts, volume, image_id, runtime, runtime_originals)
    environment = config.get('Env')
    require(type(environment) is list and all(type(item) is str and '=' in item for item in environment),
            'Malformed actual environment')
    env = dict(item.split('=', 1) for item in environment)
    require(len(env) == len(environment) and env.get('HOME') == '/tmp'
            and env.get('PYTHONDONTWRITEBYTECODE') == '1' and env.get('PYTHONNOUSERSITE') == '1'
            and all(env.get(key) == '' for key in ('HTTP_PROXY', 'HTTPS_PROXY', 'FTP_PROXY', 'NO_PROXY',
                'ALL_PROXY', 'http_proxy', 'https_proxy', 'ftp_proxy', 'no_proxy', 'all_proxy')),
            'Created keeper environment differs')
    state = value.get('State')
    expected_state = {'Status': 'created', 'Running': False, 'Paused': False, 'Restarting': False,
                      'OOMKilled': False, 'Dead': False, 'Pid': 0, 'ExitCode': 0, 'Error': ''}
    require(type(state) is dict and all(key in state and exact(state[key], expected)
            for key, expected in expected_state.items())
            and state.get('StartedAt') == '0001-01-01T00:00:00Z'
            and state.get('FinishedAt') == '0001-01-01T00:00:00Z', 'Container has already started or is not cleanly created')
    return mount_binding


def proof_for(raw: bytes, *, container_id: str, name: str, image_id: str,
              volume: str, labels: dict[str, str], mounts: dict[str, str],
              runtime: dict[str, Any] | None = None,
              runtime_originals: dict[str, bytes] | None = None) -> dict[str, Any]:
    require(type(raw) is bytes and len(raw) <= process.CONTROL_LIMIT, 'Bounded original inspection required')
    value = process.strict_json_loads(raw)
    mount_binding = validate_created(value, container_id=container_id, name=name, image_id=image_id,
                     volume=volume, labels=labels, mounts=mounts, runtime=runtime, runtime_originals=runtime_originals)
    return {'protocol': PROTOCOL, 'inspection_sha256': hashlib.sha256(raw).hexdigest(),
            'container_id': container_id, 'name': name, 'image_id': image_id, 'volume': volume,
            'labels': labels, 'mounts': mounts, 'mount_source_binding': mount_binding, 'definition': definition()}


def validate_order(positions: dict[str, int]) -> None:
    require(type(positions) is dict and set(positions) == set(ORDER), 'Complete original prestart chronology required')
    ordered = [positions[name] for name in ORDER]
    require(all(type(item) is int and item > 0 for item in ordered)
            and all(left < right for left, right in zip(ordered, ordered[1:])),
            'Created proof must precede start and candidate session')


def validate_continuity(created: dict[str, Any], running: dict[str, Any], runtime: dict[str, Any], *,
                        previous_running: dict[str, Any] | None = None) -> None:
    """Bind every observed running keeper to its fully admitted created state.

    Unlike the finite-process helper, this keeper has source/helper bind mounts.
    Only the already qualified OOM default transition and whole-row mount order
    are admitted; no command, resource or mount field is discarded.
    """
    baseline = created if previous_running is None else previous_running
    before = json.loads(encoded(process.immutable_inspection(baseline)))
    after = json.loads(encoded(process.immutable_inspection(running)))
    state = running.get('State')
    require(type(running.get('RestartCount')) is int and running['RestartCount'] == 0
            and type(state) is dict and state.get('Running') is True
            and state.get('Restarting') is False and state.get('Dead') is False and state.get('OOMKilled') is False
            and type(state.get('Pid')) is int and state['Pid'] > 0,
            'Running keeper has restarted or lacks a live lineage')
    assert type(state) is dict
    created_at, started_at = process._timestamp(created['Created']), process._timestamp(state.get('StartedAt'))
    require(created_at is not None and started_at is not None and created_at <= started_at,
            'Running keeper needs a nonzero start after creation')
    if previous_running is not None:
        previous_state = previous_running.get('State')
        require(type(previous_running.get('RestartCount')) is int and previous_running['RestartCount'] == 0
                and type(previous_state) is dict and type(previous_state.get('Pid')) is int
                and previous_state['Pid'] == state['Pid'] and previous_state.get('StartedAt') == state['StartedAt'],
                'Running keeper incarnation changed')
    for projection in (before, after):
        process.validate_oom_kill_default(projection['HostConfig'])
        rows = projection['Mounts']
        require(type(rows) is list and all(type(row) is dict and type(row.get('Destination')) is str for row in rows),
                'Complete running mount inventory required')
        by_target = {row['Destination']: row for row in rows}
        require(len(by_target) == len(rows), 'Duplicate running mount destination')
        projection['Mounts'] = by_target
    old, new = before['HostConfig']['OomKillDisable'], after['HostConfig']['OomKillDisable']
    if previous_running is None and old is False and new is None:
        require(process._startup_runtime_valid(runtime, created['Image']), 'Unqualified OOM startup transition')
        after['HostConfig']['OomKillDisable'] = False
    else:
        require(old is new, 'Unqualified OOM default change')
    require(exact(before, after), 'Running keeper differs from admitted created identity')
