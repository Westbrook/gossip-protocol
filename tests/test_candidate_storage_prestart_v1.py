"""Pure constructed inspection controls; these are not physical evidence."""
from copy import deepcopy
import hashlib
from typing import Any
import unittest

from gossip_harness import candidate_storage_prestart_v1 as prestart


def created_fixture(*, inputs: bool = False) -> tuple[dict[str, Any], dict[str, Any]]:
    """Complete synthetic keeper inspection for offline admission controls only."""
    expected: dict[str, Any] = {'container_id': 'a' * 64, 'name': 'fixture-container',
        'image_id': 'sha256:' + 'b' * 64, 'volume': 'fixture-volume',
        'labels': {'gossip.execution': 'fixture-execution', 'gossip.source': 'c' * 64, 'gossip.fixture': 'd' * 64},
        'mounts': {'/workspace': '/fixture/source', '/checks': '/fixture/checks'}}
    if inputs:
        expected['mounts']['/inputs'] = '/fixture/inputs'
    host = {'NetworkMode': 'none', 'ReadonlyRootfs': True, 'Privileged': False, 'AutoRemove': False,
        'Memory': 256 * 1024 * 1024, 'MemorySwap': 256 * 1024 * 1024,
        'NanoCpus': 1000000000, 'PidsLimit': 64, 'CapDrop': ['ALL'],
        'SecurityOpt': ['no-new-privileges:true'], 'LogConfig': {'Type': 'none', 'Config': {}},
        'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0},
        'Ulimits': [{'Name': 'nofile', 'Hard': 256, 'Soft': 256}], 'Init': True,
        'OomKillDisable': False, 'IpcMode': 'private', 'CgroupnsMode': 'private',
        'CapAdd': None, 'Devices': [], 'DeviceRequests': None, 'VolumesFrom': None,
        'Links': None, 'PortBindings': {}, 'PublishAllPorts': False, 'PidMode': '',
        'UTSMode': '', 'UsernsMode': '', 'Binds': None}
    host['Mounts'] = [{'Type': 'bind', 'Source': source, 'Target': target, 'ReadOnly': True,
        'BindOptions': {'Propagation': 'rprivate'}} for target, source in expected['mounts'].items()]
    host['Mounts'].append({'Type': 'volume', 'Source': expected['volume'], 'Target': '/tmp',
                         'VolumeOptions': {'NoCopy': True}})
    mounts = [{'Type': 'bind', 'Source': source, 'Destination': target, 'RW': False,
        'Propagation': 'rprivate', 'Mode': ''} for target, source in expected['mounts'].items()]
    mounts.append({'Type': 'volume', 'Source': '/var/lib/docker/volumes/fixture-volume/_data',
        'Name': expected['volume'], 'Driver': 'local', 'Destination': '/tmp', 'RW': True,
        'Propagation': '', 'Mode': 'z'})
    config = {'Image': expected['image_id'], 'User': '0:0', 'WorkingDir': '/workspace',
        'Tty': False, 'OpenStdin': False, 'StdinOnce': False, 'AttachStdout': True,
        'AttachStderr': True, 'Healthcheck': {'Test': ['NONE']}, 'Hostname': 'gossip-validator',
        'Labels': expected['labels'], 'Entrypoint': [prestart.COMMAND[0]], 'Cmd': list(prestart.COMMAND[1:]),
        'Env': ['HOME=/tmp', 'PYTHONDONTWRITEBYTECODE=1', 'PYTHONNOUSERSITE=1'] + [key + '=' for key in
            ('HTTP_PROXY', 'HTTPS_PROXY', 'FTP_PROXY', 'NO_PROXY', 'ALL_PROXY',
             'http_proxy', 'https_proxy', 'ftp_proxy', 'no_proxy', 'all_proxy')]}
    value = {'Id': expected['container_id'], 'Name': '/' + expected['name'], 'Image': expected['image_id'],
        'Created': '2026-10-04T00:00:00.000000001Z', 'Path': prestart.COMMAND[0],
        'Args': list(prestart.COMMAND[1:]), 'RestartCount': 0, 'Config': config, 'HostConfig': host,
        'Mounts': mounts, 'State': {'Status': 'created', 'Running': False, 'Paused': False,
            'Restarting': False, 'OOMKilled': False, 'Dead': False, 'Pid': 0, 'ExitCode': 0,
            'Error': '', 'StartedAt': '0001-01-01T00:00:00Z', 'FinishedAt': '0001-01-01T00:00:00Z'}}
    return value, deepcopy(expected)


def qualified_runtime(image_id: str) -> dict[str, Any]:
    gate = prestart.process.startup_policy()['runtime_gate']
    return {'protocol': prestart.process.PROTOCOL, 'image_id': image_id, 'image_inspect_sha256': 'e' * 64,
        **{key: gate[key] for key in ('os', 'engine_version', 'api_version', 'cgroup_version')},
        'engine_git_commit': gate['engine_git_commit_values'][0], 'oom_kill_disable_supported': False,
        'daemon_id': 'fixture-daemon', 'architecture': 'fixture', 'kernel_version': 'fixture',
        'cgroup_driver': 'fixture', 'endpoint': {'socket_path': '/fixture/docker.sock', 'device': 1, 'inode': 2}}


class CandidateStoragePrestartV1Tests(unittest.TestCase):
    def test_full_b01_b02_inspections_are_admitted_and_raw_digest_is_bound(self):
        for inputs in (False, True):
            with self.subTest(inputs=inputs):
                value, expected = created_fixture(inputs=inputs)
                raw = prestart.encoded(value) + b'\n'
                proof = prestart.proof_for(raw, **expected)
                self.assertEqual(proof['inspection_sha256'], hashlib.sha256(raw).hexdigest())
                self.assertEqual(proof['protocol'], prestart.PROTOCOL)
                self.assertEqual(proof['mounts'], expected['mounts'])

    def test_every_required_config_field_is_enforced_before_start(self):
        for key, wrong in {'Image': 'sha256:' + 'f' * 64, 'User': '65534:65534', 'WorkingDir': '/',
            'Tty': True, 'OpenStdin': True, 'StdinOnce': True, 'AttachStdout': False, 'AttachStderr': False,
            'Healthcheck': {}, 'Hostname': 'other', 'Labels': {}, 'Entrypoint': ['sh'], 'Cmd': ['different']}.items():
            for missing in (False, True):
                with self.subTest(key=key, missing=missing):
                    value, expected = created_fixture()
                    if missing:
                        del value['Config'][key]
                    else:
                        value['Config'][key] = wrong
                    with self.assertRaises(ValueError):
                        prestart.validate_created(value, **expected)

    def test_resource_and_security_fields_cannot_be_weakened_or_missing(self):
        changes = {'NetworkMode': 'host', 'ReadonlyRootfs': False, 'Privileged': True, 'AutoRemove': True,
            'Memory': 0, 'MemorySwap': -1, 'NanoCpus': 0, 'PidsLimit': -1, 'CapDrop': [], 'CapAdd': ['SYS_ADMIN'],
            'SecurityOpt': [], 'LogConfig': {'Type': 'json-file'}, 'RestartPolicy': {'Name': 'always'},
            'Ulimits': [], 'Init': False, 'Devices': [{'PathOnHost': '/dev/kvm'}], 'DeviceRequests': [{}],
            'VolumesFrom': ['foreign'], 'Links': ['foreign'], 'PortBindings': {'80/tcp': [{}]},
            'PublishAllPorts': True, 'PidMode': 'host', 'UTSMode': 'host', 'UsernsMode': 'host', 'Binds': ['/a:/b']}
        for key, wrong in changes.items():
            for missing in (False, True):
                with self.subTest(key=key, missing=missing):
                    value, expected = created_fixture()
                    if missing:
                        del value['HostConfig'][key]
                    else:
                        value['HostConfig'][key] = wrong
                    with self.assertRaises(ValueError):
                        prestart.validate_created(value, **expected)

    def test_created_lifecycle_and_exact_json_types_are_required(self):
        for key, wrong in {'Status': 'running', 'Running': True, 'Paused': True, 'Restarting': True,
            'OOMKilled': True, 'Dead': True, 'Pid': 1, 'ExitCode': 1, 'Error': 'failed',
            'StartedAt': '2026-10-04T00:00:00Z', 'FinishedAt': '2026-10-04T00:00:01Z'}.items():
            with self.subTest(key=key):
                value, expected = created_fixture()
                value['State'][key] = wrong
                with self.assertRaises(ValueError):
                    prestart.validate_created(value, **expected)
        for group, key, wrong in (('State', 'Pid', False), ('HostConfig', 'ReadonlyRootfs', 1),
                ('HostConfig', 'PublishAllPorts', 0), ('Config', 'Tty', 0)):
            value, expected = created_fixture()
            value[group][key] = wrong
            with self.assertRaises(ValueError):
                prestart.validate_created(value, **expected)
        for key, wrong in {'Id': 'f' * 64, 'Name': '/other', 'Image': 'sha256:' + 'f' * 64,
            'Created': 'not-a-time', 'RestartCount': True, 'Path': 'sh', 'Args': ['-c', 'evil']}.items():
            value, expected = created_fixture()
            value[key] = wrong
            with self.assertRaises(ValueError):
                prestart.validate_created(value, **expected)

    def test_complete_mount_rows_and_declarations_bind_actual_sources_and_options(self):
        for mutate in (
            lambda v: v['Mounts'].append(deepcopy(v['Mounts'][0])),
            lambda v: v['Mounts'].pop(),
            lambda v: v['Mounts'][0].update(RW=True),
            lambda v: v['Mounts'][0].update(Source='/foreign'),
            lambda v: v['Mounts'][0].update(Propagation='rshared'),
            lambda v: v['Mounts'][-1].update(Name='foreign-volume'),
            lambda v: v['Mounts'][-1].update(Driver='foreign-driver'),
            lambda v: v['HostConfig']['Mounts'][0].update(ReadOnly=False),
            lambda v: v['HostConfig']['Mounts'][0]['BindOptions'].update(Propagation='shared'),
            lambda v: v['HostConfig']['Mounts'][-1]['VolumeOptions'].update(NoCopy=False),
            lambda v: v['HostConfig'].update(Tmpfs={'/foreign': 'rw'}),
        ):
            value, expected = created_fixture(inputs=True)
            mutate(value)
            with self.assertRaises(ValueError):
                prestart.validate_created(value, **expected)

    def test_environment_duplicate_key_or_proxy_injection_is_rejected(self):
        for env in (['HOME=/root'], ['HOME=/tmp', 'HOME=/tmp'], ['HTTP_PROXY=http://proxy']):
            value, expected = created_fixture()
            value['Config']['Env'] = env
            with self.assertRaises(ValueError):
                prestart.validate_created(value, **expected)

    def test_original_raw_duplicate_keys_and_oversized_inspection_are_rejected(self):
        _, expected = created_fixture()
        for raw in (b'{"Id":"x","Id":"y"}', b'x' * (prestart.process.CONTROL_LIMIT + 1)):
            with self.assertRaises(ValueError):
                prestart.proof_for(raw, **expected)

    def test_every_original_position_is_required_in_the_order_before_start(self):
        good = {name: i + 1 for i, name in enumerate(prestart.ORDER)}
        prestart.validate_order(good)
        for name in prestart.ORDER:
            for wrong in (False, 0, len(prestart.ORDER) + 1):
                if name == prestart.ORDER[-1] and wrong == len(prestart.ORDER) + 1:
                    continue
                positions = dict(good, **{name: wrong})
                with self.subTest(name=name, wrong=wrong), self.assertRaises(ValueError):
                    prestart.validate_order(positions)
            missing = dict(good)
            del missing[name]
            with self.assertRaises(ValueError):
                prestart.validate_order(missing)

    def test_running_identity_only_allows_full_mount_row_permutation(self):
        value, _ = created_fixture(inputs=True)
        running = deepcopy(value)
        running['State'].update(Status='running', Running=True, Pid=123, StartedAt='2026-10-04T00:00:01Z')
        running['Mounts'].reverse()
        prestart.validate_continuity(value, running, {})
        for group, key, wrong in (('Config', 'User', '65534:65534'), ('HostConfig', 'Memory', 0)):
            changed = deepcopy(running)
            changed[group][key] = wrong
            with self.assertRaises(ValueError):
                prestart.validate_continuity(value, changed, {})
        running['Mounts'][0]['extra-field'] = 'changed'
        with self.assertRaises(ValueError):
            prestart.validate_continuity(value, running, {})

    def test_oom_default_transition_requires_the_existing_qualified_runtime(self):
        value, expected = created_fixture()
        running = deepcopy(value)
        running['HostConfig']['OomKillDisable'] = None
        running['State'].update(Status='running', Running=True, Pid=123, StartedAt='2026-10-04T00:00:01Z')
        with self.assertRaises(ValueError):
            prestart.validate_continuity(value, running, {})
        prestart.validate_continuity(value, running, qualified_runtime(expected['image_id']))
        running['HostConfig']['OomKillDisable'] = True
        with self.assertRaises(ValueError):
            prestart.validate_continuity(value, running, qualified_runtime(expected['image_id']))


    def test_later_running_lineage_and_oom_representation_stay_exact(self):
        value, expected = created_fixture()
        first = deepcopy(value)
        first['State'].update(Status='paused', Running=True, Paused=True, Pid=123,
                              StartedAt='2026-10-04T00:00:01Z')
        first['HostConfig']['OomKillDisable'] = None
        runtime = qualified_runtime(expected['image_id'])
        prestart.validate_continuity(value, first, runtime)
        prestart.validate_continuity(value, deepcopy(first), runtime, previous_running=first)
        for mutate in (
            lambda v: v.update(RestartCount=1),
            lambda v: v.update(RestartCount=False),
            lambda v: v['State'].update(StartedAt='2026-10-04T00:00:02Z'),
            lambda v: v['State'].update(StartedAt='0001-01-01T00:00:00Z'),
            lambda v: v['State'].update(Pid=124),
            lambda v: v['HostConfig'].update(OomKillDisable=False),
        ):
            later = deepcopy(first)
            mutate(later)
            with self.assertRaises(ValueError):
                prestart.validate_continuity(value, later, runtime, previous_running=first)

if __name__ == '__main__':
    unittest.main()
