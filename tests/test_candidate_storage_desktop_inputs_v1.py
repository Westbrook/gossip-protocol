"""Synthetic admission controls using an exact projection of a failed D02 run.

No test here is physical successor evidence. The surrounding created inspection
and HTTP framing are constructed; only the pinned projection values are retained
observations. These controls never import or execute candidate code.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from typing import Any
import unittest

from gossip_harness import candidate_storage_prestart_v1 as prestart
from tests.test_candidate_storage_prestart_v1 import created_fixture


FIXTURE = Path(__file__).parent / 'fixtures/storage-desktop-inputs-v1/d02-mount-projection.json'
FIXTURE_SHA256 = '1b97db10f8348b591c7c3a99b88210bd2d177791ef198ef8fc60314071b7f287'


def framed_response(value: dict[str, Any]) -> bytes:
    """Synthetic HTTP framing around a projected JSON object; never an original."""
    body = prestart.encoded(value)
    return (b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: '
            + str(len(body)).encode('ascii') + b'\r\nConnection: close\r\n\r\n' + body)


class CandidateStorageDesktopInputsV1Tests(unittest.TestCase):
    def setUp(self):
        raw = FIXTURE.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), FIXTURE_SHA256)
        self.fixture = json.loads(raw)

    def constructed(self, *, alias=True):
        value, expected = created_fixture(inputs=True)
        runtime = deepcopy(self.fixture['runtime'])
        expected['image_id'] = runtime['image_id']
        expected['mounts'] = deepcopy(self.fixture['expected_mounts'])
        expected['volume'] = next(row['Name'] for row in self.fixture['Mounts']
                                  if row['Destination'] == '/tmp')
        value['Image'] = expected['image_id']
        value['Config']['Image'] = expected['image_id']
        value['Mounts'] = deepcopy(self.fixture['Mounts'])
        value['HostConfig']['Mounts'] = deepcopy(self.fixture['HostConfig.Mounts'])
        if not alias:
            self.sources(value, '/inputs', expected['mounts']['/inputs'])
        return value, expected, runtime, self.originals()

    def originals(self, *, version=None, info=None):
        # Exact retained request strings are combined with explicitly synthetic
        # response framing; their digests must bind these constructed bytes.
        return {
            **{name: raw.encode('ascii') for name, raw in self.fixture['requests'].items()},
            'runtime-version-response.bin': framed_response(
                self.fixture['version_body'] if version is None else version),
            'runtime-info-response.bin': framed_response(
                self.fixture['info_body'] if info is None else info),
        }

    @staticmethod
    def rows(value, target='/inputs'):
        return (next(row for row in value['Mounts'] if row['Destination'] == target),
                next(row for row in value['HostConfig']['Mounts'] if row['Target'] == target))

    @classmethod
    def sources(cls, value, target, source):
        actual, declared = cls.rows(value, target)
        actual['Source'] = declared['Source'] = source

    @staticmethod
    def proof(value, expected, runtime, originals):
        return prestart.proof_for(prestart.encoded(value), **expected,
                                  runtime=runtime, runtime_originals=originals)

    def test_fixture_retains_failed_origin_and_complete_unmodified_mount_rows(self):
        self.assertEqual(self.fixture['disclosure']['origin_outcome'], 'failed-before-start')
        self.assertIs(self.fixture['disclosure']['passed_successor_evidence'], False)
        self.assertEqual(len(self.fixture['originals']), 7)
        for original in self.fixture['originals'].values():
            self.assertTrue(original['path'].startswith('runs/storage-m2-physical-qualification-1/'))
            self.assertRegex(original['sha256'], r'^[0-9a-f]{64}$')
            self.assertGreater(original['bytes'], 0)
        value, expected, _, _ = self.constructed()
        for target in ('/workspace', '/checks', '/inputs'):
            actual, declared = self.rows(value, target)
            source = expected['mounts'][target]
            observed = '/host_mnt' + source if target == '/inputs' else source
            self.assertEqual(actual['Source'], observed)
            self.assertEqual(declared['Source'], observed)
        self.assertEqual(len(value['Mounts']), 4)
        self.assertEqual(len(value['HostConfig']['Mounts']), 4)

    def test_literal_paths_need_no_desktop_runtime_or_originals(self):
        value, expected, _, _ = self.constructed(alias=False)
        proof = prestart.proof_for(prestart.encoded(value), **expected)
        binding = proof['mount_source_binding']
        self.assertEqual(binding['representation'], 'literal')
        self.assertEqual(binding['host_sources'], expected['mounts'])
        self.assertEqual(binding['observed_sources'], expected['mounts'])
        self.assertIsNone(binding['runtime_sha256'])
        self.assertEqual(binding['runtime_original_sha256'], {})

    def test_exact_alias_binds_policy_full_inspection_runtime_and_constructed_originals(self):
        value, expected, runtime, originals = self.constructed()
        proof = self.proof(value, expected, runtime, originals)
        binding = proof['mount_source_binding']
        self.assertEqual(proof['inspection_sha256'], hashlib.sha256(prestart.encoded(value)).hexdigest())
        self.assertEqual(proof['protocol'], 'candidate-storage-created-prestart-v2-desktop-inputs-v1')
        self.assertEqual(proof['definition']['bind_source_policy'], prestart.bind_source_policy())
        self.assertEqual(binding['protocol'], prestart.MOUNT_POLICY)
        self.assertEqual(binding['representation'], 'docker-desktop-inputs-prefix')
        self.assertEqual(binding['host_sources'], expected['mounts'])
        self.assertEqual(binding['observed_sources'], {
            **expected['mounts'], '/inputs': '/host_mnt' + expected['mounts']['/inputs']})
        self.assertEqual(binding['runtime_sha256'], hashlib.sha256(prestart.encoded(runtime)).hexdigest())
        self.assertEqual(binding['runtime_original_sha256'], {
            name: hashlib.sha256(raw).hexdigest() for name, raw in originals.items()})
        self.assertNotEqual(binding['runtime_original_sha256']['runtime-info-response.bin'],
                            self.fixture['originals']['runtime-info-response.bin']['sha256'])

    def test_alias_requires_complete_runtime_and_each_original(self):
        value, expected, runtime, originals = self.constructed()
        for missing in (None, {}):
            with self.subTest(runtime=missing), self.assertRaises(ValueError):
                self.proof(value, expected, missing, originals)
            with self.subTest(originals=missing), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, missing)
        for name in prestart.RUNTIME_ORIGINAL_NAMES:
            partial = dict(originals)
            del partial[name]
            with self.subTest(original=name), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, partial)
        with self.assertRaises(ValueError):
            self.proof(value, expected, runtime, {**originals, 'foreign.bin': b'foreign'})

    def test_each_runtime_gate_field_is_required_exactly_with_json_types(self):
        value, expected, runtime, originals = self.constructed()
        fields = tuple(prestart.bind_source_policy()['runtime']) + ('image_id', 'daemon_id')
        for field in fields:
            for mutation in ('missing', 'changed', 'wrong-type'):
                changed = deepcopy(runtime)
                if mutation == 'missing':
                    del changed[field]
                else:
                    changed[field] = 'foreign' if mutation == 'changed' else [runtime[field]]
                with self.subTest(field=field, mutation=mutation), self.assertRaises(ValueError):
                    self.proof(value, expected, changed, originals)

    def test_original_vendor_and_runtime_fields_cannot_disagree_with_bound_identity(self):
        value, expected, runtime, _ = self.constructed()
        for kind in ('version', 'info'):
            fields = tuple(prestart.bind_source_policy()[kind]) + (('ID',) if kind == 'info' else ())
            for field in fields:
                for mutation in ('missing', 'changed', 'wrong-type'):
                    body = deepcopy(self.fixture[kind + '_body'])
                    if mutation == 'missing':
                        del body[field]
                    else:
                        body[field] = 'foreign' if mutation == 'changed' else [body[field]]
                    originals = self.originals(**{kind: body})
                    with self.subTest(kind=kind, field=field, mutation=mutation), self.assertRaises(ValueError):
                        self.proof(value, expected, runtime, originals)

    def test_original_requests_and_http_framing_are_not_replaced_by_runtime_dict(self):
        value, expected, runtime, originals = self.constructed()
        for name in prestart.RUNTIME_ORIGINAL_NAMES:
            changed = dict(originals)
            changed[name] = (b'GET /v1.47/foreign HTTP/1.1\r\n\r\n' if name.endswith('request.bin')
                             else b'HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n{}')
            with self.subTest(original=name), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, changed)

    def test_actual_and_declared_inputs_sources_must_agree(self):
        for literal_row in (0, 1):
            value, expected, runtime, originals = self.constructed()
            self.rows(value)[literal_row]['Source'] = expected['mounts']['/inputs']
            with self.subTest(literal_row=literal_row), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, originals)

    def test_alias_never_applies_to_workspace_or_checks(self):
        for target in ('/workspace', '/checks'):
            value, expected, runtime, originals = self.constructed()
            self.sources(value, target, '/host_mnt' + expected['mounts'][target])
            with self.subTest(target=target), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, originals)

    def test_inputs_alias_is_exact_without_prefix_or_path_normalization(self):
        _, expected, _, _ = self.constructed()
        source = '/host_mnt' + expected['mounts']['/inputs']
        wrong_sources = (source + '-sibling', source + '/', source + '/..', source + '/.',
            source.replace('/host_mnt/', '/host_mnt//', 1), '/host_mnt' + source,
            source.replace('/private/', '/private/../private/', 1),
            source.replace('/private/', '/private/%2e%2e/private/', 1),
            source.replace('/private/', '/Private/', 1), source + '\\', source + '\x00',
            '/host_mnt/private/foreign/inputs')
        for wrong in wrong_sources:
            value, expected, runtime, originals = self.constructed()
            self.sources(value, '/inputs', wrong)
            with self.subTest(source=wrong), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, originals)

    def test_expected_alias_root_requires_canonical_private_path(self):
        for source in ('/tmp/inputs', '/private-other/inputs', '/host_mnt/private/inputs',
                       '/private//inputs', '/private/../inputs', '/private/./inputs', '/private/inputs/'):
            value, expected, runtime, originals = self.constructed()
            expected['mounts']['/inputs'] = source
            self.sources(value, '/inputs', '/host_mnt' + source)
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, originals)

    def test_alias_preserves_exact_destination_type_readonly_and_propagation(self):
        mutations = (
            lambda a, d: a.update(Destination='/foreign'),
            lambda a, d: d.update(Target='/foreign'),
            lambda a, d: a.update(Type='volume'),
            lambda a, d: d.update(Type='volume'),
            lambda a, d: a.update(RW=True),
            lambda a, d: d.update(ReadOnly=False),
            lambda a, d: a.update(Propagation='rshared'),
            lambda a, d: d['BindOptions'].update(Propagation='rshared'),
            lambda a, d: d['BindOptions'].update(NonRecursive=True),
            lambda a, d: d.update(VolumeOptions={'NoCopy': True}),
        )
        for index, mutate in enumerate(mutations):
            value, expected, runtime, originals = self.constructed()
            mutate(*self.rows(value))
            with self.subTest(mutation=index), self.assertRaises(ValueError):
                self.proof(value, expected, runtime, originals)

    def test_running_continuity_keeps_originally_admitted_source_representation(self):
        for alias in (False, True):
            value, expected, runtime, originals = self.constructed(alias=alias)
            self.proof(value, expected, runtime, originals)
            running = deepcopy(value)
            running['State'].update(Status='running', Running=True, Pid=123,
                                     StartedAt='2026-10-04T00:00:01Z')
            prestart.validate_continuity(value, running, runtime)
            other = expected['mounts']['/inputs'] if alias else '/host_mnt' + expected['mounts']['/inputs']
            for row in ('both', 0, 1):
                changed = deepcopy(running)
                if row == 'both':
                    self.sources(changed, '/inputs', other)
                else:
                    self.rows(changed)[row]['Source'] = other
                with self.subTest(alias=alias, changed_row=row), self.assertRaises(ValueError):
                    prestart.validate_continuity(value, changed, runtime)


if __name__ == '__main__':
    unittest.main()
