"""Pure plan, capsule and scoring tests; no candidate executes on the host."""
from __future__ import annotations

import base64
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import tarfile
import unittest

from gossip_harness import candidate_release_observer_v1 as observer


def encoded(raw):
    return base64.b64encode(raw).decode('ascii')


def process(value):
    return {'exit_code': 0, 'stdout_b64': encoded(observer.canonical_bytes(value) + b'\n'), 'stderr_b64': ''}


def sample():
    files = {'library/__main__.py': b'print("candidate")\n', 'compatibility/v0.sqlite3': b'SQLite format 3\0\xff\x00'}
    expected = observer.expected_observations(files)
    observed = {'protocol': observer.PROTOCOL, 'build': process(expected['build']),
                'manifest': expected['manifest'],
                'cli': [{'step_id': item['step_id'], **process(item['value'])} for item in expected['cli']]}
    return files, expected, observed


def archive(entries):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode='w:') as tar:
        root = tarfile.TarInfo('tmp/')
        root.type = tarfile.DIRTYPE
        tar.addfile(root)
        package = tarfile.TarInfo('tmp/release/')
        package.type = tarfile.DIRTYPE
        tar.addfile(package)
        for name, value in entries:
            member = tarfile.TarInfo(name)
            if type(value) is bytes:
                member.size = len(value)
                tar.addfile(member, io.BytesIO(value))
            else:
                member.type = value[1]
                member.linkname = 'tmp/release/target'
                tar.addfile(member)
    return output.getvalue()


class CandidateReleaseObserverTests(unittest.TestCase):
    def test_plan_is_fixed_isolated_bootstrap_and_distinct_cli_processes(self):
        self.assertEqual(observer.DELIVERY_PROFILE, 'complete-public-source-capsule-v1')
        self.assertEqual(observer.CASE_IDS, ('release-build', 'release-manifest', 'released-cli-roundtrip'))
        self.assertEqual(observer.BUILD_COMMAND[:4], ('python', '-I', '-B', '-c'))
        self.assertIn("'/tmp/release'", observer.BUILD_COMMAND[-1])
        self.assertEqual([name for name, _ in observer.cli_steps()], ['import', 'list', 'show', 'export'])
        for _, command in observer.cli_steps():
            self.assertEqual(command[:4], ('python', '-I', '-B', '-c'))
            self.assertIn("'/workspace'", command[-1])
            self.assertIn("'/tmp/research.sqlite'", command[-1])
            compile(command[-1], '<fixed-cli-bootstrap>', 'exec')
        self.assertEqual(observer.fixture_files(), {observer.INPUT_SOURCE: observer.INPUT_BYTES})
        self.assertIn('雪', observer.INPUT_BYTES.decode())

    def test_host_scoring_binds_binary_source_and_all_delivered_files(self):
        files, expected, observed = sample()
        self.assertEqual(observer.score_observations(observed, files), (True, True, True))
        manifest = json.loads(base64.b64decode(expected['manifest']['content_b64']))
        record = next(item for item in manifest['files'] if item['path'].endswith('.sqlite3'))
        self.assertEqual(record['sha256'], hashlib.sha256(files['compatibility/v0.sqlite3']).hexdigest())
        files['compatibility/v0.sqlite3'] += b'x'
        self.assertEqual(observer.score_observations(observed, files), (False, False, True))
        _, _, observed = sample()
        observed['manifest']['files'].append({'path': 'unexpected-secret', 'sha256': 'a'*64, 'bytes': 1})
        observed['manifest']['files'].sort(key=lambda item: item['path'])
        self.assertFalse(observer.score_observations(observed, sample()[0])[1])

    def test_candidate_failures_are_assertions_and_not_forged_envelopes(self):
        files, _, observed = sample()
        for stdout in (b'not json', b'{"total":1,"total":1}', b'NaN', b'\xff'):
            altered = deepcopy(observed)
            altered['cli'][1]['stdout_b64'] = encoded(stdout)
            self.assertEqual(observer.score_observations(altered, files), (True, True, False))
        altered = deepcopy(observed)
        value = json.loads(base64.b64decode(altered['cli'][1]['stdout_b64']))
        value['total'] = True
        altered['cli'][1]['stdout_b64'] = encoded(json.dumps(value).encode())
        self.assertFalse(observer.score_observations(altered, files)[2])
        observed['build'] = {'exit_code': 2, 'stdout_b64': '', 'stderr_b64': encoded(b'{"error":"io_error"}\n')}
        observed['manifest'], observed['cli'] = None, []
        self.assertEqual(observer.score_observations(observed, files), (False, False, False))

    def test_incomplete_changed_order_and_malformed_host_records_raise(self):
        files, _, original = sample()
        mutations = [lambda o: o.update(protocol='fixture'), lambda o: o['cli'].reverse(),
                     lambda o: o['cli'].pop(), lambda o: o['cli'].clear(),
                     lambda o: o['build'].update(exit_code=True),
                     lambda o: o['build'].update(stdout_b64='!'),
                     lambda o: o['build'].update(fixture=True),
                     lambda o: o['manifest'].update(content_b64=''),
                     lambda o: o['manifest']['files'][0].update(bytes=True)]
        for mutation in mutations:
            altered = deepcopy(original)
            mutation(altered)
            with self.assertRaises(observer.ObservationError):
                observer.score_observations(altered, files)

    def test_capsule_preserves_bytes_and_rejects_links_paths_duplicates_and_corruption(self):
        files = {'library/__main__.py': b'abc', 'compatibility/v0.sqlite3': b'\x00\xff\x01'}
        raw = archive([('tmp/release/' + name, value) for name, value in files.items()])
        self.assertEqual(observer.parse_capsule(raw), files)
        self.assertEqual(observer.parse_capsule(archive([('tmp/scratch', b'ignored')])), {})
        self.assertEqual(observer.parse_capsule(archive([('tmp/scratch', ('type', tarfile.SYMTYPE)), ('tmp/release/app.py', b'app')])), {'app.py': b'app'})
        invalid = [[('tmp/release/../escape', b'x')], [('other/one', b'x')],
                   [('tmp/release/a', b'x'), ('tmp/release/a', b'y')],
                   [('tmp/release/a', b'x'), ('tmp/release/a/b', b'y')],
                   [('tmp/release/a', ('type', tarfile.SYMTYPE))], [('tmp/release/a', ('type', tarfile.LNKTYPE))],
                   [('tmp/release/a', ('type', tarfile.FIFOTYPE))], [('tmp/release/a', ('type', tarfile.CHRTYPE))],
                   [('tmp/release/a', ('type', tarfile.GNUTYPE_SPARSE))], [('tmp/release//a', b'x')]]
        for entries in invalid:
            with self.subTest(entries=entries), self.assertRaises(observer.ObservationError):
                observer.parse_capsule(archive(entries))
        for broken in (b'', b'invalid tar', raw[:700], raw[:3072], raw + b'x' * 512):
            with self.assertRaises(observer.CapsuleTransportError):
                observer.parse_capsule(broken)
        with self.assertRaises(observer.ObservationError):
            observer.expected_observations({'../unsafe': b'x'})
        with self.assertRaises(observer.ObservationError):
            observer.expected_observations({'release-manifest.json': b'x'})

    def test_size_bounds_cannot_be_bypassed_by_declared_lengths(self):
        output = io.BytesIO()
        with tarfile.open(fileobj=output, mode='w:') as tar:
            root = tarfile.TarInfo('tmp/')
            root.type = tarfile.DIRTYPE
            tar.addfile(root)
            member = tarfile.TarInfo('tmp/release/huge')
            member.size = observer.MAX_FILE_BYTES + 1
            tar.addfile(member)
        with self.assertRaises(observer.ObservationError):
            observer.parse_capsule(output.getvalue())
        files, _, observed = sample()
        observed['build']['stdout_b64'] = encoded(b'x' * (observer.MAX_PROCESS_BYTES + 1))
        with self.assertRaises(observer.ObservationError):
            observer.score_observations(observed, files)

    def test_complete_case_survives_later_missing_observations(self):
        files, _, observed = sample()
        self.assertEqual(observer.LOADED_SOURCE_SHA256,
                         hashlib.sha256(Path(observer.__file__).read_bytes()).hexdigest())
        for case_id in observer.CASE_IDS:
            self.assertTrue(observer.score_case(case_id, observed, files))
        partial = {"protocol": observer.PROTOCOL, "manifest": None, "cli": []}
        self.assertFalse(observer.score_case("release-manifest", partial, files))
        with self.assertRaises(observer.ObservationError):
            observer.score_case("released-cli-roundtrip", partial, files)
        with self.assertRaises(observer.ObservationError):
            observer.score_case("release-build", partial, files)
        observed['manifest']['files'].append({'path': 'unexpected', 'sha256': 'a'*64, 'bytes': 1})
        observed['manifest']['files'].sort(key=lambda item: item['path'])
        observed['cli'] = [{"incomplete": True}]
        self.assertFalse(observer.score_case("release-manifest", observed, files))
        self.assertTrue(observer.score_case("release-build", observed, files))
        with self.assertRaises(observer.ObservationError):
            observer.score_case("released-cli-roundtrip", observed, files)
        with self.assertRaises(observer.ObservationError):
            observer.score_case("invented-case", observed, files)

    def test_incomplete_transport_precedes_invalid_candidate_package(self):
        invalid_entries = [
            ('tmp/release/link', ('type', tarfile.SYMTYPE)),
            ('tmp/release/../escape', b'x'),
        ]
        for entry in invalid_entries:
            raw = archive([entry])
            with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as reader:
                list(reader)
                trailer_offset = reader.offset
            with self.subTest(entry=entry):
                with self.assertRaises(observer.ObservationError) as caught:
                    observer.parse_capsule(raw)
                self.assertNotIsInstance(caught.exception, observer.CapsuleTransportError)
                for truncated in (raw[:trailer_offset], raw[:trailer_offset + 512]):
                    with self.assertRaises(observer.CapsuleTransportError):
                        observer.parse_capsule(truncated)
