"""Versioned B02 execution with exact-source reviewed schedule applicability.

The registered evaluator recipe contains operations and input fixtures only.
Expected values, requirement labels and decisions remain on the host. Imported
v1 transport helpers retain their exact source bindings; no frozen code changes.
"""
from __future__ import annotations

import base64
import binascii
import json
import io
import os
import queue
import stat
import threading
from pathlib import Path, PurePosixPath
import subprocess
import tempfile
import tarfile
from typing import Any
import uuid

from . import candidate_storage_driver_v1 as transport
from . import candidate_intake_store_cases_v2 as cases
from .sandbox import DockerValidator

PROTOCOL = "candidate-intake-store-driver-v2"
SNAPSHOT_PROTOCOL = transport.SNAPSHOT_PROTOCOL
RUNTIME_IMAGE = transport.RUNTIME_IMAGE
VOLUME_OPTIONS = transport.VOLUME_OPTIONS
PHASES = transport.PHASES
MAX_FIXTURE_FILES = 1024
MAX_FIXTURE_BYTES = 8 * 1024 * 1024
MAX_RECIPE_BYTES = 12 * 1024 * 1024
MAX_OPERATIONS = 1024
MAX_STREAM_BYTES = 16 * 1024 * 1024
MAX_FILE_BYTES = 32 * 1024 * 1024
MAX_CAPTURE_BYTES = 40 * 1024 * 1024
DriverError = transport.DriverError
CaptureLayoutError = transport.CaptureLayoutError
StorageExecution = transport.StorageExecution
encoded = transport.encoded
sha256 = transport.sha256
_safe_path = transport._safe_path
require = transport.require

STORE_METHODS = frozenset(("insert", "documents", "show", "create_job", "get_job", "list_jobs",
    "job_manifest", "start_job", "fail_job", "cancel_job", "retry_job", "commit_job"))
MANAGER_METHODS = frozenset(("submit", "get", "prepare", "commit", "cancel", "retry",
    "submit_directory", "submit_zip", "submit_json"))

CHILD_ADAPTER = r'''import inspect
import json
from pathlib import Path
import sys
import base64
sys.path.insert(0, '/workspace')
from library.catalog.store import Store
from library.ingestion.jobs import JobManager
from library.ingestion.local import import_file
from library.common import LibraryError

recipe = json.loads(Path('/checks/recipe.json').read_text())
applicability = json.loads(Path('/checks/applicability.json').read_text())
path = Path('/tmp/catalog.sqlite')
store = Store(path)
manager = JobManager(store)
refs = {}
secondary = None
unavailable = False

class ReviewedScheduleUnavailable(Exception):
    pass

class AdapterUnavailable(Exception):
    pass

def wrapped(call):
    try:
        return {'value':call()}
    except ReviewedScheduleUnavailable:
        return {'observation_unavailable':'reviewed_enclosing_write_transaction'}
    except AdapterUnavailable:
        return {'observation_unavailable':'public_boundary_interposition'}
    except LibraryError as error:
        return {'error':error.code}
    except BaseException as error:
        return {'unexpected_exception':type(error).__name__}

def resolve(value):
    if type(value) is dict:
        if set(value) == {'$path'}:
            return str(Path('/inputs') / value['$path'])
        if set(value) == {'$ref'}:
            return refs[value['$ref']]
        if set(value) == {'$bytes_base64'}:
            return base64.b64decode(value['$bytes_base64'], validate=True)
        return {key:resolve(item) for key,item in value.items()}
    if type(value) is list:
        return [resolve(item) for item in value]
    return value

def operate(op):
    global secondary
    special = op.get('op')
    if special == 'bind':
        refs[op['as']] = resolve(op['value'])
        return None
    if special == 'mutate':
        value = refs[op['ref']]
        trail = op['path']
        for key in trail[:-1]:
            value = value[key]
        value[trail[-1]] = resolve(op['value'])
        return None
    if special == 'import':
        if op.get('connection', 'primary') == 'secondary':
            if secondary is None:
                secondary = Store(path)
            return import_file(secondary, '/inputs', op['source'])
        return import_file(store, '/inputs', op['source'])
    if special == 'signature':
        result = {}
        for label, method in (('store', Store.commit_job), ('manager', JobManager.commit)):
            parameter = inspect.signature(method).parameters.get('fail_before_commit')
            result[label + '_commit_keyword_only'] = parameter is not None and parameter.kind == inspect.Parameter.KEYWORD_ONLY
            result[label + '_default_false'] = parameter is not None and parameter.default is False
        return result
    if special == 'interfere':
        if applicability['decision'] == 'unavailable':
            raise ReviewedScheduleUnavailable()
        if applicability['decision'] != 'instrumentable-control':
            raise AdapterUnavailable()
        boundary = op['boundary']
        original = getattr(store, boundary)
        second_results = []
        calls = 0
        other = Store(path)
        def injected(*args, **kwargs):
            nonlocal calls
            if calls == 0:
                # Count the evaluator's one-shot interposition, not all calls a
                # valid product may make to the required public method.
                try:
                    setattr(store, boundary, original)
                except Exception as error:
                    raise AdapterUnavailable() from error
                calls = 1
                for name in op['second_actions']:
                    receiver = JobManager(other) if name == 'prepare' else other
                    second_results.append(wrapped(lambda name=name,receiver=receiver:getattr(receiver, name)(op['job_id'])))
            return original(*args, **kwargs)
        try:
            setattr(store, boundary, injected)
        except Exception as error:
            other.close()
            raise AdapterUnavailable() from error
        try:
            prepared = wrapped(lambda:manager.prepare(op['job_id']))
        finally:
            try:
                setattr(store, boundary, original)
            except Exception as error:
                raise AdapterUnavailable() from error
            finally:
                other.close()
        return {'boundary_calls':calls, 'second_results':second_results, 'prepare':prepared}
    target = op['target']
    if target in ('secondary_store', 'secondary_manager'):
        if secondary is None:
            secondary = Store(path)
        receiver = secondary if target == 'secondary_store' else JobManager(secondary)
    else:
        receiver = store if target == 'store' else manager
    return getattr(receiver, op['method'])(*resolve(op.get('args', [])), **resolve(op.get('kwargs', {})))

def phase(name):
    global store, manager, secondary, unavailable
    if name == 'reopened':
        if secondary is not None:
            secondary.close()
            secondary = None
        store.close()
        store = Store(path)
        manager = JobManager(store)
        refs.clear()
    results = []
    for op in recipe['phases'][name]:
        if unavailable:
            results.append({'not_run':'dependency_unavailable'})
            continue
        result = wrapped(lambda:operate(op))
        if 'observation_unavailable' in result:
            unavailable = True
        if 'as' in op and 'value' in result and op.get('op') != 'bind':
            refs[op['as']] = result['value']
        # Preserve the historical returned value while keeping the actual object
        # in refs for a subsequent direct-return alias mutation probe.
        try:
            results.append(json.loads(json.dumps(result, ensure_ascii=True, allow_nan=False)))
        except BaseException as error:
            results.append({'unexpected_exception':type(error).__name__})
    return results

for name in ('before', 'after', 'reopened'):
    if sys.stdin.buffer.readline(128) != (name + '\n').encode():
        raise ValueError('unexpected controller phase')
    result = wrapped(lambda:phase(name))
    value = result['value'] if 'value' in result else result
    print(json.dumps({'phase':name,'value':value},ensure_ascii=True,allow_nan=False,separators=(',',':')),flush=True)
if sys.stdin.buffer.readline(128) != b'finish\n':
    raise ValueError('unexpected controller finish')
if secondary is not None:
    secondary.close()
store.close()
'''


def source_sha256(files: dict[str, bytes]) -> str:
    transport.source_sha256(files)  # Shared bounded file/path admission, no execution.
    return sha256(encoded({"protocol": PROTOCOL, "complete_source": [
        {"path": name, "bytes": len(raw), "sha256": sha256(raw)} for name, raw in sorted(files.items())]}))



FORCED_CASE_IDS = (
    "interfere-start_job-cancel", "interfere-start_job-cancel-retry",
    "interfere-fail_job-cancel", "interfere-fail_job-cancel-retry", "interfere-prepare-prepare",
)
APPLICABILITY_PROTOCOL = "b02-authored-schedule-applicability-v2"
REVIEW_PURPOSE = "harness_qualification"
COMPONENT_PATHS = (
    "library/ingestion/jobs.py", "library/catalog/m4_control.py", "library/catalog/store.py",
    "library/catalog/legacy_m1.py", "library/catalog/v2_maintenance.py",
    "library/catalog/maintenance.py", "library/catalog/worker.py", "library/catalog/m4_store.py",
)
# Installed, prospectively source-reviewed qualification fixtures only. A digest
# here is not production review authority. Full source identities prevent another
# module or alternate subclass from substituting different locking behavior.
_QUALIFICATION_REVIEWS: tuple[dict[str, Any], ...] = ({'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'authored-json-reference-v2',
  'purpose': 'harness_qualification',
  'source_sha256': 'd733c498002ae564b2d4e761a0e5d6c0acb40fbc6ce060cf635b8b3881063f01',
  'source_components': {'library/ingestion/jobs.py': 'be070fe6c954828589d8c14f239f83742da14613f2285e3a37fa8d362a9f58ef',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '8d0b77fc85e6833cf9d1e18d6d4909db0166fbffcf106ad313f7c79f7e9ca0e3',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['store-start_job-queued-lower',
                       'store-start_job-queued-current',
                       'store-start_job-queued-higher',
                       'store-start_job-running-lower',
                       'store-start_job-running-current',
                       'store-start_job-running-higher',
                       'store-start_job-completed-lower',
                       'store-start_job-completed-current',
                       'store-start_job-completed-higher',
                       'store-start_job-cancelled-lower',
                       'store-start_job-cancelled-current',
                       'store-start_job-cancelled-higher',
                       'store-start_job-failed-lower',
                       'store-start_job-failed-current',
                       'store-start_job-failed-higher',
                       'store-fail_job-queued-lower',
                       'store-fail_job-queued-current',
                       'store-fail_job-queued-higher',
                       'store-fail_job-running-lower',
                       'store-fail_job-running-current',
                       'store-fail_job-running-higher',
                       'store-fail_job-completed-lower',
                       'store-fail_job-completed-current',
                       'store-fail_job-completed-higher',
                       'store-fail_job-cancelled-lower',
                       'store-fail_job-cancelled-current',
                       'store-fail_job-cancelled-higher',
                       'store-fail_job-failed-lower',
                       'store-fail_job-failed-current',
                       'store-fail_job-failed-higher',
                       'store-commit_job-queued-lower',
                       'store-commit_job-queued-current',
                       'store-commit_job-queued-higher',
                       'store-commit_job-running-lower',
                       'store-commit_job-running-current',
                       'store-commit_job-running-higher',
                       'store-commit_job-completed-lower',
                       'store-commit_job-completed-current',
                       'store-commit_job-completed-higher',
                       'store-commit_job-cancelled-lower',
                       'store-commit_job-cancelled-current',
                       'store-commit_job-cancelled-higher',
                       'store-commit_job-failed-lower',
                       'store-commit_job-failed-current',
                       'store-commit_job-failed-higher',
                       'state-cancel_job-queued',
                       'state-retry_job-queued',
                       'state-prepare-queued',
                       'replay-queued',
                       'state-cancel_job-running',
                       'state-retry_job-running',
                       'state-prepare-running',
                       'replay-running',
                       'state-cancel_job-completed',
                       'state-retry_job-completed',
                       'state-prepare-completed',
                       'replay-completed',
                       'state-cancel_job-cancelled',
                       'state-retry_job-cancelled',
                       'state-prepare-cancelled',
                       'replay-cancelled',
                       'state-cancel_job-failed',
                       'state-retry_job-failed',
                       'state-prepare-failed',
                       'replay-failed',
                       'missing-get_job',
                       'missing-job_manifest',
                       'missing-start_job',
                       'missing-fail_job',
                       'missing-cancel_job',
                       'missing-retry_job',
                       'missing-commit_job',
                       'missing-prepare',
                       'token-domain-zero',
                       'token-domain-negative',
                       'token-domain-overflow',
                       'token-domain-bool',
                       'token-domain-float',
                       'token-domain-string',
                       'token-domain-null',
                       'job-id-one',
                       'job-id-max',
                       'job-id-empty',
                       'job-id-over',
                       'job-id-space',
                       'job-id-punctuation',
                       'job-id-unicode',
                       'job-id-integer',
                       'manifest-shape-object',
                       'manifest-shape-null',
                       'manifest-shape-member-type',
                       'manifest-shape-missing-source',
                       'manifest-shape-missing-text',
                       'manifest-shape-extra-field',
                       'manifest-shape-source-type',
                       'manifest-shape-text-type',
                       'manifest-source-text-ties',
                       'signatures-offline-hook',
                       'fresh-value-create_job',
                       'fresh-value-get_job',
                       'fresh-value-list_jobs',
                       'fresh-value-job_manifest',
                       'fresh-value-start_job',
                       'fresh-value-fail_job',
                       'fresh-value-cancel_job',
                       'fresh-value-retry_job',
                       'fresh-value-commit_job',
                       'input-manifest-snapshot',
                       'cancel-retry-token-history',
                       'running-prepare-after-conflict',
                       'interfere-start_job-cancel',
                       'interfere-start_job-cancel-retry',
                       'interfere-fail_job-cancel',
                       'interfere-fail_job-cancel-retry',
                       'interfere-prepare-prepare',
                       'two-client-admission',
                       'two-client-cancel',
                       'intake-directory-replay-conflict',
                       'intake-zip-replay-conflict',
                       'intake-json-replay-conflict',
                       'intake-directory-recursive',
                       'intake-zip-regular-and-untyped',
                       'intake-json-literal',
                       'intake-directory-empty',
                       'intake-zip-empty',
                       'intake-json-empty',
                       'intake-zip-directory-only',
                       'intake-directory-namespace-empty',
                       'intake-directory-namespace-absolute',
                       'intake-directory-namespace-backslash',
                       'intake-directory-namespace-nul',
                       'intake-directory-namespace-empty-segment',
                       'intake-directory-namespace-dot',
                       'intake-directory-namespace-parent',
                       'intake-directory-namespace-trailing',
                       'intake-directory-namespace-long-segment',
                       'intake-zip-namespace-empty',
                       'intake-zip-namespace-absolute',
                       'intake-zip-namespace-backslash',
                       'intake-zip-namespace-nul',
                       'intake-zip-namespace-empty-segment',
                       'intake-zip-namespace-dot',
                       'intake-zip-namespace-parent',
                       'intake-zip-namespace-trailing',
                       'intake-zip-namespace-long-segment',
                       'intake-zip-member-absolute',
                       'intake-zip-member-backslash',
                       'intake-zip-member-empty-segment',
                       'intake-zip-member-dot',
                       'intake-zip-member-parent',
                       'intake-zip-member-long-segment',
                       'intake-zip-directory-parent',
                       'intake-zip-directory-empty-segment',
                       'intake-directory-key-bytes-limit',
                       'intake-directory-key-bytes-over',
                       'intake-zip-key-bytes-limit',
                       'intake-zip-key-bytes-over',
                       'intake-directory-depth-limit',
                       'intake-directory-depth-over',
                       'intake-zip-depth-limit',
                       'intake-zip-depth-over',
                       'intake-json-depth-limit',
                       'intake-json-depth-over',
                       'intake-directory-leaf-symlink',
                       'intake-directory-descendant-symlink',
                       'intake-directory-root-symlink',
                       'intake-directory-ancestor-symlink',
                       'intake-zip-ancestor-symlink',
                       'intake-zip-bundle-symlink',
                       'intake-json-ancestor-symlink',
                       'intake-json-bundle-symlink',
                       'intake-directory-fifo',
                       'intake-zip-mode-symlink',
                       'intake-zip-mode-fifo',
                       'intake-zip-mode-socket',
                       'intake-zip-encrypted',
                       'intake-directory-unsupported',
                       'intake-directory-invalid-utf8',
                       'intake-zip-unsupported',
                       'intake-zip-invalid-utf8',
                       'intake-zip-duplicate-same',
                       'intake-json-duplicate-same',
                       'intake-zip-duplicate-different',
                       'intake-json-duplicate-different',
                       'intake-directory-count-limit',
                       'intake-directory-count-over',
                       'intake-directory-member-bytes-limit',
                       'intake-directory-member-bytes-over',
                       'intake-directory-total-bytes-limit',
                       'intake-directory-total-bytes-over',
                       'intake-zip-count-limit',
                       'intake-zip-count-over',
                       'intake-zip-member-bytes-limit',
                       'intake-zip-member-bytes-over',
                       'intake-zip-total-bytes-limit',
                       'intake-zip-total-bytes-over',
                       'intake-json-count-limit',
                       'intake-json-count-over',
                       'intake-json-member-bytes-limit',
                       'intake-json-member-bytes-over',
                       'intake-json-total-bytes-limit',
                       'intake-json-total-bytes-over',
                       'intake-json-control-total-byte-limit',
                       'intake-zip-64-files-plus-directory',
                       'intake-zip-archive-bytes-limit',
                       'intake-zip-archive-bytes-over',
                       'intake-zip-ratio-below',
                       'intake-zip-ratio-limit',
                       'intake-zip-ratio-over',
                       'intake-zip-ratio-zero-compressed',
                       'intake-zip-ignored-directory-byte-bound',
                       'intake-zip-ignored-directory-ratio',
                       'intake-zip-ignored-directory-total',
                       'intake-json-schema-top-list',
                       'intake-json-schema-top-null',
                       'intake-json-schema-top-missing',
                       'intake-json-schema-top-extra',
                       'intake-json-schema-entries-object',
                       'intake-json-schema-entries-null',
                       'intake-json-schema-member-list',
                       'intake-json-schema-member-missing-source',
                       'intake-json-schema-member-missing-text',
                       'intake-json-schema-member-extra',
                       'intake-json-schema-source-nonstr',
                       'intake-json-schema-text-nonstr',
                       'intake-json-syntax',
                       'intake-json-invalid-utf8',
                       'intake-zip-syntax',
                       'intake-directory-missing',
                       'intake-directory-wrong-kind',
                       'intake-zip-missing',
                       'intake-zip-wrong-kind',
                       'intake-json-missing',
                       'intake-json-wrong-kind',
                       'intake-json-deferred-source-parent',
                       'intake-json-deferred-unsupported',
                       'intake-json-deferred-unencodable',
                       'intake-json-literal-byte-limit',
                       'intake-json-large-whitespace-valid',
                       'intake-json-large-whitespace-syntax',
                       'intake-json-large-whitespace-utf8'],
  'forced_schedule': 'unavailable',
  'reason': 'reviewed-enclosing-BEGIN-IMMEDIATE-prevents-forced-secondary-precedence',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'authored-json-reference-v2-archive-rejection-admits',
  'purpose': 'harness_qualification',
  'source_sha256': '8f7137ca04d2be0725967ed8712429a0e14d21909184e51bf36877aed71660de',
  'source_components': {'library/ingestion/jobs.py': '09914a7ef2998f81367bccc48d20cc775c8f7981a222b75a7d925076ea05b244',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '8d0b77fc85e6833cf9d1e18d6d4909db0166fbffcf106ad313f7c79f7e9ca0e3',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['intake-zip-mode-fifo'],
  'forced_schedule': 'unavailable',
  'reason': 'reviewed-enclosing-BEGIN-IMMEDIATE-prevents-forced-secondary-precedence',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'authored-json-reference-v2-symlink-rejection-admits',
  'purpose': 'harness_qualification',
  'source_sha256': 'a5fe0602a3542351efad2c63ced8376f1143164dd56d88def76810cd3424918b',
  'source_components': {'library/ingestion/jobs.py': 'ca9c8fe8bd0351cf2cbf582618f4f031a7aa945d492d93f5180ed86c151e9f07',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '8d0b77fc85e6833cf9d1e18d6d4909db0166fbffcf106ad313f7c79f7e9ca0e3',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['intake-directory-leaf-symlink'],
  'forced_schedule': 'unavailable',
  'reason': 'reviewed-enclosing-BEGIN-IMMEDIATE-prevents-forced-secondary-precedence',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'authored-json-reference-v2-stale-epoch',
  'purpose': 'harness_qualification',
  'source_sha256': 'b37c93de38180e846daa5115315cc622afa78a1c42866d17eb8c434422f2ffc0',
  'source_components': {'library/ingestion/jobs.py': 'be070fe6c954828589d8c14f239f83742da14613f2285e3a37fa8d362a9f58ef',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '45225a916dc6cfe7017a76b796dad20e1f761f59e0ad606a17ffb607598e56cc',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['store-start_job-running-lower'],
  'forced_schedule': 'unavailable',
  'reason': 'reviewed-enclosing-BEGIN-IMMEDIATE-prevents-forced-secondary-precedence',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'authored-json-reference-v2-cancel-failed',
  'purpose': 'harness_qualification',
  'source_sha256': 'ae8a45a6335e057914c651d58973bd71a7e1b0a5e6d8184b1ff5fb61a06ff1f5',
  'source_components': {'library/ingestion/jobs.py': 'be070fe6c954828589d8c14f239f83742da14613f2285e3a37fa8d362a9f58ef',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '6d887d17ec58a48547ffa4f2ea28f87fd5bc6e84abfeb44ef72ce30a4f22acd7',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['state-cancel_job-failed'],
  'forced_schedule': 'unavailable',
  'reason': 'reviewed-enclosing-BEGIN-IMMEDIATE-prevents-forced-secondary-precedence',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'instrumentable-schedule-control-v2',
  'purpose': 'harness_qualification',
  'source_sha256': 'ff0c99f4cabbbbf27ccc737ebb31e533d2819f7c0e5fedb23cb87d15499f9b01',
  'source_components': {'library/ingestion/jobs.py': 'b956074966ba9b74d8573f8213e9e01b905b2d7952a53536ea0e1b75181bfe2c',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '8d0b77fc85e6833cf9d1e18d6d4909db0166fbffcf106ad313f7c79f7e9ca0e3',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['interfere-start_job-cancel',
                       'interfere-start_job-cancel-retry',
                       'interfere-fail_job-cancel',
                       'interfere-fail_job-cancel-retry',
                       'interfere-prepare-prepare'],
  'forced_schedule': 'instrumentable-control',
  'reason': 'separately-authored-public-API-schedule-control-not-product-conformance',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'},
 {'protocol': 'b02-authored-schedule-applicability-v2',
  'review_id': 'instrumentable-schedule-control-v2-false-success',
  'purpose': 'harness_qualification',
  'source_sha256': 'e9ed4bee02a716cfc59f51517126b4726a08def0237285437404958cdd0d206a',
  'source_components': {'library/ingestion/jobs.py': '0ac08d07185d51089deb5fa07c22ebb543b0359868470d9532a8da1bcb307f9b',
                        'library/catalog/m4_control.py': '3649e23a7da17653611aab1b56b6c91fce9de8761f736e0faa709e7f151bda95',
                        'library/catalog/store.py': '8d0b77fc85e6833cf9d1e18d6d4909db0166fbffcf106ad313f7c79f7e9ca0e3',
                        'library/catalog/legacy_m1.py': 'ae7511167063569b3688e726e1b526b96504d9d1e3cb157441969340acdfd6e5',
                        'library/catalog/v2_maintenance.py': '4dce085d02aea79e5ea06f045729b5643437cc70280496a11ce1bbb9746cadb9',
                        'library/catalog/maintenance.py': '49e7ae3261f6c890d79774bcabf59554e52fababff9a0016c357aa8b255472e0',
                        'library/catalog/worker.py': '9744d9a1c6e2b588548198ee9e42ca96cbb7f28c1d59402975a9bd018f522bb9',
                        'library/catalog/m4_store.py': '988756723389f5d77f072cfa96a5395ff9b920a4c429aa79a00df68bee0d9e2c'},
  'definition_sha256': 'd582d6143939b56ef834d951799b305a893bd71578904b497c7c6f020f33db30',
  'allowed_case_ids': ['interfere-start_job-cancel-retry'],
  'forced_schedule': 'instrumentable-control',
  'reason': 'separately-authored-public-API-schedule-control-not-product-conformance',
  'qualification_only': True,
  'production_review_authority': False,
  'source_review_basis': 'Full source inventory plus exact public prepare, Store composition and '
                         'transaction-provider components; no runtime exception inference.'})


def review_for_source(files: dict[str, bytes]) -> dict[str, Any]:
    """Return a copy of a closed installed review; never accept candidate flags."""
    actual = source_sha256(files)
    matches = [row for row in _QUALIFICATION_REVIEWS if row["source_sha256"] == actual]
    require(len(matches) == 1, "Unregistered source applicability")
    review = matches[0]
    require(review["purpose"] == REVIEW_PURPOSE and review["qualification_only"] is True
            and review["production_review_authority"] is False, "Review purpose differs")
    require(set(review["source_components"]) == set(COMPONENT_PATHS), "Incomplete applicability component binding")
    require(all(path in files and sha256(files[path]) == digest
                for path, digest in review["source_components"].items()), "Reviewed source component differs")
    result = json.loads(encoded(review))
    result["review_sha256"] = sha256(encoded(review))
    return result


def reviewed_applicability(files: dict[str, bytes], case_id: str, *,
                           expected_review_sha256: str) -> dict[str, Any]:
    review = review_for_source(files)
    require(review["review_sha256"] == expected_review_sha256, "Qualified review identity differs")
    require(case_id in cases.CASE_IDS, "Unknown reviewed case")
    require(case_id in review["allowed_case_ids"], "Case outside qualified review scope")
    require(review["definition_sha256"] == cases.definition_sha256(), "Review definition binding differs")
    forced = case_id in FORCED_CASE_IDS
    if forced:
        decision = review["forced_schedule"]
        require(decision in ("unavailable", "instrumentable-control"), "Unqualified forced schedule")
    else:
        decision = "not-requested"
    recipe = cases.execution_recipe(case_id)
    actual_forced = any(operation.get("op") == "interfere"
        for operations in recipe["phases"].values() for operation in operations)
    require(forced == actual_forced, "Forced schedule roster differs from reviewed cases")
    return {"protocol": APPLICABILITY_PROTOCOL, "case_id": case_id, "decision": decision,
        "reason": review["reason"] if forced else "ordinary-public-operations",
        "review_id": review["review_id"], "review_sha256": review["review_sha256"],
        "review": review, "qualification_only": True, "production_review_authority": False,
        "acceptance_credit": False,
        "planned_secondary_actions": 0 if decision == "unavailable" else None}


def driver_sources() -> dict[str, str]:
    names: tuple[str, ...] = ("candidate_intake_store_driver_v2.py", "candidate_intake_store_driver_v1.py", "candidate_intake_store_cases_v1.py", "candidate_intake_store_cases_v2.py",
             "candidate_storage_driver_v1.py", "candidate_storage_cases_v1.py", "sandbox.py")
    names += ("candidate_intake_fixtures_v1.py", "candidate_intake_store_profile_v1.py", "candidate_storage_observer_v1.py",
              "candidate_intake_store_observer_v1.py")
    return {name: sha256(Path(__file__).with_name(name).read_bytes()) for name in names}


def _input_path(value: Any) -> bool:
    return (type(value) is str and bool(value) and len(value) <= 2048 and "\\" not in value
        and "\x00" not in value and not PurePosixPath(value).is_absolute()
        and all(part not in ("", ".", "..") for part in value.split("/")))



def _validate_values(value: Any, refs: set[str]) -> None:
    if type(value) is dict:
        if set(value) == {"$path"}:
            require(_input_path(value["$path"]), "Input reference path")
        elif set(value) == {"$ref"}:
            require(type(value["$ref"]) is str and value["$ref"] in refs, "Missing or forward reference")
        elif set(value) == {"$bytes_base64"}:
            try:
                require(type(value["$bytes_base64"]) is str, "Bytes reference encoding")
                base64.b64decode(value["$bytes_base64"], validate=True)
            except (ValueError, binascii.Error) as error:
                raise DriverError("Bytes reference encoding") from error
        else:
            for item in value.values():
                _validate_values(item, refs)
    elif type(value) is list:
        for item in value:
            _validate_values(item, refs)


def validate_recipe(recipe: Any) -> dict[str, Any]:
    """Reject arbitrary executable hooks and preserve the exact admitted data."""
    require(type(recipe) is dict and set(recipe) == {"fixtures", "phases"}, "Recipe shape")
    try:
        raw = encoded(recipe)
    except (ValueError, TypeError, RecursionError) as error:
        raise DriverError("Recipe is not bounded JSON data") from error
    require(len(raw) <= MAX_RECIPE_BYTES, "Recipe byte bound")
    require(type(recipe["fixtures"]) is list and len(recipe["fixtures"]) <= MAX_FIXTURE_FILES,
            "Fixture file bound")
    paths: dict[str, str] = {}
    total = 0
    for fixture in recipe["fixtures"]:
        require(type(fixture) is dict and "path" in fixture and "kind" in fixture, "Fixture shape")
        path, kind = fixture["path"], fixture["kind"]
        require(_input_path(path) and path not in paths, "Fixture path")
        require(type(kind) is str and kind in ("file", "directory", "symlink", "fifo"), "Fixture kind")
        required = {"path", "kind"} | ({"bytes_base64"} if kind == "file" else {"target"} if kind == "symlink" else set())
        require(set(fixture) == required, "Fixture fields")
        if kind == "file":
            try:
                require(type(fixture["bytes_base64"]) is str, "Fixture bytes must be base64")
                data = base64.b64decode(fixture["bytes_base64"], validate=True)
            except (ValueError, binascii.Error) as error:
                raise DriverError("Invalid fixture base64") from error
            total += len(data)
            require(total <= MAX_FIXTURE_BYTES, "Fixture byte bound")
        elif kind == "symlink":
            require(type(fixture["target"]) is str and 0 < len(fixture["target"]) <= 2048
                    and "\x00" not in fixture["target"], "Fixture symlink target")
        paths[path] = kind
    for path in paths:
        require(all(paths.get(str(parent), "directory") == "directory"
                    for parent in PurePosixPath(path).parents), "Fixture non-directory ancestor")
    phases = recipe["phases"]
    require(type(phases) is dict and set(phases) == set(PHASES), "Recipe phases")
    operations = 0
    refs: set[str] = set()
    for phase in PHASES:
        if phase == "reopened":
            refs.clear()
        require(type(phases[phase]) is list, "Phase operation list")
        operations += len(phases[phase])
        require(operations <= MAX_OPERATIONS, "Operation bound")
        for op in phases[phase]:
            require(type(op) is dict, "Operation object")
            special = op.get("op")
            _validate_values(op.get("args", []), refs)
            _validate_values(op.get("kwargs", {}), refs)
            if "value" in op:
                _validate_values(op["value"], refs)
            if special == "bind":
                require(set(op) == {"op", "as", "value"}, "Bind operation")
            elif special == "import":
                require(set(op) <= {"op", "source", "as", "connection"} and type(op.get("source")) is str
                    and op.get("connection", "primary") in ("primary", "secondary"), "Import operation")
            elif special == "signature":
                require(set(op) <= {"op", "as"}, "Signature operation")
            elif special == "mutate":
                require(set(op) == {"op", "ref", "path", "value"} and type(op["ref"]) is str
                    and op["ref"] in refs and type(op["path"]) is list and 1 <= len(op["path"]) <= 16
                    and all(type(key) in (str, int) for key in op["path"]), "Mutation operation")
            elif special == "interfere":
                require(set(op) <= {"op", "boundary", "job_id", "second_actions", "as"}
                    and op.get("boundary") in ("start_job", "fail_job") and type(op.get("job_id")) is str
                    and type(op.get("second_actions")) is list and 1 <= len(op["second_actions"]) <= 4
                    and all(name in ("cancel_job", "retry_job", "prepare") for name in op["second_actions"]), "Interference operation")
            else:
                require(special is None and set(op) <= {"target", "method", "args", "kwargs", "as"}, "Call operation fields")
                target = op.get("target")
                require(type(target) is str and target in ("store", "manager", "secondary_store", "secondary_manager"), "Call target")
                methods = STORE_METHODS if target in ("store", "secondary_store") else MANAGER_METHODS
                require(type(op.get("method")) is str and op["method"] in methods, "Call method")
                require(type(op.get("args", [])) is list and type(op.get("kwargs", {})) is dict, "Call arguments")
            if "as" in op:
                require(type(op["as"]) is str and 0 < len(op["as"]) <= 64, "Result reference")
                refs.add(op["as"])
    return json.loads(raw)


def parse_capture(raw: bytes) -> dict[str, bytes]:
    """Validate complete Docker tar framing, then return ordinary /tmp bytes.

    Nothing is extracted onto the host. A complete but unsupported link/special
    file layout is distinct from an incomplete transport. SQLite sidecars are
    preserved here; the declared layout observer decides coherence eligibility.
    """
    require(type(raw) is bytes and 0 < len(raw) <= MAX_CAPTURE_BYTES and len(raw) % 512 == 0,
            "Invalid capture length")
    members: list[tarfile.TarInfo] = []
    try:
        with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as archive:
            for member in archive:
                if len(members) <= 2048:
                    members.append(member)
            trailer = raw[archive.offset:]
            require(len(trailer) >= 1024 and not any(trailer), "Incomplete capture trailer")
            if len(members) > 2048:
                raise CaptureLayoutError("Capture member bound")
            files: dict[str, bytes] = {}
            seen: set[str] = set()
            directories: set[str] = set()
            total = 0
            for member in members:
                name = member.name.rstrip("/") if member.isdir() else member.name
                if not _safe_path(name) or name in seen or (name != "tmp" and not name.startswith("tmp/")):
                    raise CaptureLayoutError("Unsafe or duplicate capture member")
                seen.add(name)
                if member.isdir():
                    if member.size != 0:
                        raise CaptureLayoutError("Nonempty directory entry")
                    directories.add(name)
                    continue
                if (not member.isreg() or member.issparse() or type(member.size) is not int
                        or not 0 <= member.size <= MAX_FILE_BYTES):
                    raise CaptureLayoutError("Unsupported capture member")
                total += member.size
                if total > 32 * 1024 * 1024 or len(files) >= 1024:
                    raise CaptureLayoutError("Capture file bound")
                stream = archive.extractfile(member)
                require(stream is not None, "Missing capture bytes")
                assert stream is not None
                with stream:
                    content = stream.read(member.size + 1)
                require(len(content) == member.size, "Truncated capture member")
                files[name[4:]] = content
            require("tmp" in directories, "Missing capture root")
            for name in files:
                if "tmp/" + name in directories or any(str(parent) in files for parent in PurePosixPath(name).parents):
                    raise CaptureLayoutError("Capture ancestor collision")
            return files
    except DriverError:
        raise
    except (tarfile.TarError, OSError, EOFError, ValueError, UnicodeError) as error:
        raise DriverError("Malformed capture transport") from error


class _Session:
    """Host-controlled persistent stdin session with bounded untrusted streams."""
    def __init__(self, commands: transport._Commands, arguments: list[str]) -> None:
        self.commands = commands
        self.arguments = arguments
        self.process = subprocess.Popen(arguments, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=DockerValidator._environment())
        self.lines: queue.Queue[bytes | None] = queue.Queue(maxsize=8)
        self.streams = {"stdout": bytearray(), "stderr": bytearray()}
        self.counts = {"stdout": 0, "stderr": 0}
        self.errors: set[str] = set()
        self.threads = [threading.Thread(target=self._drain, args=(kind,), daemon=True) for kind in self.streams]
        for thread in self.threads:
            thread.start()
        self.requests: list[dict[str, Any]] = []

    def _drain(self, kind: str) -> None:
        pipe = getattr(self.process, kind)
        try:
            while raw := (pipe.readline(MAX_STREAM_BYTES + 1) if kind == "stdout" else pipe.read(65536)):
                self.counts[kind] += len(raw)
                self.streams[kind].extend(raw[:max(0, MAX_STREAM_BYTES - len(self.streams[kind]))])
                if kind == "stdout":
                    try:
                        self.lines.put_nowait(raw)
                    except queue.Full:
                        self.errors.add("extra-response")
        except OSError:
            self.errors.add(kind)
        finally:
            pipe.close()
            if kind == "stdout":
                try:
                    self.lines.put_nowait(None)
                except queue.Full:
                    self.errors.add("extra-response")

    def phase(self, phase: str) -> Any:
        require(not self.errors and all(count <= MAX_STREAM_BYTES for count in self.counts.values()), "Session output bound")
        assert self.process.stdin is not None
        request = (phase + "\n").encode()
        self.process.stdin.write(request)
        self.process.stdin.flush()
        self.requests.append({"ordinal": len(self.requests), "phase": phase, "request_sha256": sha256(request)})
        try:
            raw = self.lines.get(timeout=self.commands.timeout)
        except queue.Empty as error:
            raise DriverError("Candidate phase response timeout") from error
        require(raw is not None and len(raw) <= MAX_STREAM_BYTES and raw.endswith(b"\n"), "Incomplete candidate response")
        assert raw is not None
        try:
            def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
                result: dict[str, Any] = {}
                for key, item in items:
                    if key in result:
                        raise ValueError("Duplicate response key")
                    result[key] = item
                return result
            value = json.loads(raw, object_pairs_hook=pairs)
            encoded(value)  # Reject NaN/infinity, including exponent overflow.
        except (ValueError, UnicodeError, RecursionError) as error:
            raise DriverError("Malformed candidate response") from error
        require(type(value) is dict and set(value) == {"phase", "value"} and value["phase"] == phase,
                "Candidate phase response differs")
        self.commands.retain(phase + "-response.json", raw)
        return value["value"]

    def finish(self, success: bool) -> bool:
        if self.process.poll() is None:
            if success:
                try:
                    assert self.process.stdin is not None
                    self.process.stdin.write(b"finish\n")
                    self.process.stdin.flush()
                    self.process.stdin.close()
                    self.process.wait(timeout=self.commands.timeout)
                except (OSError, subprocess.SubprocessError):
                    self.process.kill()
                    self.process.wait(timeout=5)
            else:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdin is not None and not self.process.stdin.closed:
            self.process.stdin.close()
        for thread in self.threads:
            thread.join(timeout=5)
        complete = all(not thread.is_alive() for thread in self.threads)
        record: dict[str, Any] = {"arguments": self.arguments, "exit_code": self.process.returncode,
                                  "requests": self.requests, "capture_complete": complete, "errors": sorted(self.errors)}
        for kind, data in self.streams.items():
            raw = bytes(data)
            name = "session-" + kind + ".bin"
            self.commands.retain(name, raw)
            record[kind] = {"path": name, "sha256": sha256(raw), "bytes": len(raw),
                            "observed_bytes": self.counts[kind], "truncated": self.counts[kind] > len(raw)}
        # Three responses plus an end-of-stream sentinel are the entire protocol.
        pending: list[bytes | None] = []
        while not self.lines.empty():
            pending.append(self.lines.get_nowait())
        record["extra_lines"] = sum(item is not None for item in pending)
        self.commands.retain("session.json", encoded(record))
        return (success and complete and self.process.returncode == 0 and not self.errors
                and not record["extra_lines"] and all(count <= MAX_STREAM_BYTES for count in self.counts.values()))



def _stage_inputs(root: Path, fixtures: list[dict[str, Any]]) -> None:
    for item in sorted(fixtures, key=lambda item: (item["kind"] == "symlink", len(PurePosixPath(item["path"]).parts))):
        path = root / item["path"]
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
        if item["kind"] == "directory":
            path.mkdir(exist_ok=True, mode=0o755)
        elif item["kind"] == "file":
            with path.open("xb") as stream:
                stream.write(base64.b64decode(item["bytes_base64"], validate=True))
            path.chmod(0o444)
        elif item["kind"] == "symlink":
            path.symlink_to(item["target"])
        else:
            os.mkfifo(path, mode=0o444)


def _verify_inputs(root: Path, fixtures: list[dict[str, Any]]) -> list[dict[str, Any]]:
    expected: dict[str, dict[str, Any]] = {}
    for item in fixtures:
        path = item["path"]
        for ancestor in PurePosixPath(path).parents:
            if str(ancestor) != ".":
                expected.setdefault(str(ancestor), {"path": str(ancestor), "kind": "directory"})
        value = {"path": path, "kind": item["kind"]}
        if item["kind"] == "file":
            raw = base64.b64decode(item["bytes_base64"], validate=True)
            value.update({"bytes": len(raw), "sha256": sha256(raw)})
        elif item["kind"] == "symlink":
            value["target"] = item["target"]
        expected[path] = value
    observed: dict[str, dict[str, Any]] = {}
    for parent, directories, files in os.walk(root, followlinks=False):
        for name in directories + files:
            path = Path(parent) / name
            relative = path.relative_to(root).as_posix()
            mode = path.lstat().st_mode
            kind = ("symlink" if stat.S_ISLNK(mode) else "directory" if stat.S_ISDIR(mode)
                    else "file" if stat.S_ISREG(mode) else "fifo" if stat.S_ISFIFO(mode) else "unsupported")
            value = {"path": relative, "kind": kind}
            if kind == "file":
                raw = path.read_bytes()
                value.update({"bytes": len(raw), "sha256": sha256(raw)})
            elif kind == "symlink":
                value["target"] = os.readlink(path)
            observed[relative] = value
    require(observed == expected, "Staged fixture identity differs")
    return [observed[name] for name in sorted(observed)]


def _start_arguments(sandbox: DockerValidator, name: str, workspace: Path,
                     checks: Path, inputs: Path, volume: str) -> list[str]:
    arguments = transport._start_arguments(sandbox, name, workspace, checks, volume)
    index = arguments.index("--entrypoint")
    arguments[index:index] = ["--mount", "type=bind,source=" + str(inputs) + ",target=/inputs,readonly,bind-propagation=rprivate"]
    return arguments


# Source identity is captured after all helper code has been loaded. Hashes bind
# imported helpers as well as this module and the registered definitions.
_LOADED_DRIVER_SOURCES = driver_sources()


def run_intake_store_case(files: dict[str, bytes], case_id: str, output_root: Path, *,
        expected_source_sha256: str, expected_definition_sha256: str, expected_review_sha256: str,
        image_id: str = RUNTIME_IMAGE, timeout_seconds: int = 30) -> StorageExecution:
    """One registered history, one fresh container and volume, no retries."""
    require(driver_sources() == _LOADED_DRIVER_SOURCES, "Loaded driver source differs")
    require(type(files) is dict, "Exact source mapping required")
    files = dict(files)
    actual_source = source_sha256(files)
    require(actual_source == expected_source_sha256, "Registered source bytes differ")
    require(type(case_id) is str and case_id in cases.CASE_IDS, "Unknown registered case")
    require(expected_definition_sha256 == cases.definition_sha256(), "Registered definitions differ")
    recipe = validate_recipe(cases.execution_recipe(case_id))
    applicability = reviewed_applicability(files, case_id, expected_review_sha256=expected_review_sha256)
    require(image_id == RUNTIME_IMAGE, "Exact pinned image required")
    require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 120, "Invalid timeout")
    root = Path(output_root).absolute()
    require(not root.exists() and not root.is_symlink() and root.parent.is_dir()
            and root.parent.resolve() == root.parent, "Fresh canonical output root required")
    root.mkdir(mode=0o700)
    commands = transport._Commands(root, timeout_seconds)
    execution_id = "intake-store-" + uuid.uuid4().hex
    name, volume = "gossip-" + execution_id, "gossip-volume-" + execution_id
    sources = driver_sources()
    intent = {"protocol": PROTOCOL, "execution_id": execution_id, "source_sha256": actual_source,
        "case_id": case_id, "ordered_phases": list(PHASES), "case_definition_sha256": expected_definition_sha256,
        "recipe_sha256": sha256(encoded(recipe)), "driver_sources": sources, "applicability": applicability,
        "adapter_sha256": sha256(CHILD_ADAPTER.encode()), "image_id": image_id,
        "timeout_seconds": timeout_seconds, "container": name, "volume": volume,
        "snapshot_protocol": SNAPSHOT_PROTOCOL, "volume_options": VOLUME_OPTIONS,
        "limits": {"source_files": transport.MAX_SOURCE_FILES, "source_bytes": transport.MAX_SOURCE_BYTES,
            "capture_bytes": MAX_CAPTURE_BYTES, "capture_file_bytes": MAX_FILE_BYTES, "stream_bytes": MAX_STREAM_BYTES,
            "fixture_files": MAX_FIXTURE_FILES, "fixture_bytes": MAX_FIXTURE_BYTES,
            "recipe_bytes": MAX_RECIPE_BYTES, "operations": MAX_OPERATIONS}}
    commands.retain("intent.json", encoded(intent))
    commands.retain("recipe.json", encoded(recipe))
    commands.retain("applicability.json", encoded(applicability))
    errors: list[str] = []
    snapshots: dict[str, dict[str, bytes]] = {}
    responses: dict[str, Any] = {}
    layout_errors: dict[str, str] = {}
    runtime: dict[str, Any] = {}
    session: _Session | None = None
    sandbox = DockerValidator(image_id, {"intake_store_adapter.py": CHILD_ADAPTER},
        command=("python", "-I", "-c", "import time;time.sleep(1800)"))
    volume_attempted = container_attempted = False
    container_cleanup = volume_cleanup = False
    def checked(label: str, arguments: list[str], limit: int = transport.MAX_STREAM_BYTES) -> dict[str, Any]:
        record = commands.run(label, arguments, limit)
        require(transport._clean(record), label + ": incomplete or failed controller command")
        return record
    def json_record(record: dict[str, Any]) -> Any:
        try:
            return json.loads(commands.raw(record))
        except (ValueError, UnicodeError) as error:
            raise DriverError("Malformed Docker inspection") from error
    def remove_container() -> bool:
        try:
            removed = commands.run("container-remove", ["docker", "rm", "--force", "--volumes", name])
            absent = checked("container-after", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])
            return transport._clean(removed) and not commands.raw(absent).strip()
        except (DriverError, OSError, subprocess.SubprocessError) as error:
            errors.append("container-cleanup:" + type(error).__name__)
            return False
    try:
        with tempfile.TemporaryDirectory(prefix="gossip-intake-store-stage-") as temporary:
            staging = Path(temporary).resolve()
            workspace, checks, inputs = staging / "source", staging / "checks", staging / "inputs"
            for directory in (workspace, checks, inputs):
                directory.mkdir(mode=0o755)
            for path, raw in files.items():
                target = workspace / path
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
                target.write_bytes(raw)
                target.chmod(0o444)
            (checks / "intake_store_adapter.py").write_text(CHILD_ADAPTER, encoding="utf-8")
            (checks / "recipe.json").write_bytes(encoded({"phases": recipe["phases"]}))
            (checks / "applicability.json").write_bytes(encoded(applicability))
            for check in checks.iterdir():
                check.chmod(0o444)
            _stage_inputs(inputs, recipe["fixtures"])
            commands.retain("fixture-manifest.json", encoded(_verify_inputs(inputs, recipe["fixtures"])))
            staged = {path.relative_to(workspace).as_posix(): path.read_bytes()
                      for path in workspace.rglob("*") if path.is_file()}
            require(staged == files and source_sha256(staged) == expected_source_sha256, "Staged source identity differs")
            commands.retain("plan.json", encoded({"workspace": str(workspace), "checks": str(checks), "inputs": str(inputs)}))
            runtime["server"] = json_record(checked("runtime-server", ["docker", "version", "--format", "{{json .Server}}"]))
            runtime["image"] = json_record(checked("runtime-image", ["docker", "image", "inspect", "--format", "{{json .}}", image_id]))
            require(type(runtime["server"]) is dict and type(runtime["server"].get("Version")) is str
                and type(runtime["image"]) is dict and runtime["image"].get("Id") == image_id, "Runtime identity differs")
            for kind, arguments in (("volume", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"]),
                ("container", ["docker", "container", "ls", "--all", "--quiet", "--filter", "name=^/" + name + "$"])):
                require(not commands.raw(checked(kind + "-before", arguments)).strip(), kind + " absence unproven")
            volume_attempted = True
            made = checked("volume-create", ["docker", "volume", "create", "--driver", "local",
                "--label", "gossip.execution=" + execution_id, "--label", "gossip.snapshot=" + SNAPSHOT_PROTOCOL,
                "--opt", "type=tmpfs", "--opt", "device=tmpfs", "--opt", "o=" + VOLUME_OPTIONS["o"], volume])
            require(commands.raw(made).strip() == volume.encode(), "Volume creation differs")
            inspected = checked("volume-created", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
            require(transport._volume_valid(json_record(inspected), volume, execution_id), "Volume ownership or bounds differ")
            container_attempted = True
            checked("container-start", _start_arguments(sandbox, name, workspace, checks, inputs, volume))
            session = _Session(commands, ["docker", "exec", "--interactive", "--user", "65534:65534", name,
                "python", "-I", "-B", "/checks/intake_store_adapter.py"])
            for phase in PHASES:
                responses[phase] = session.phase(phase)
                checked(phase + "-pause", ["docker", "pause", name])
                state = checked(phase + "-state", ["docker", "inspect", "--format", "{{json .}}", name])
                require(transport._paused(json_record(state), volume, image_id), "Container snapshot not frozen")
                capture = checked(phase + "-capture", ["docker", "cp", name + ":/tmp", "-"], MAX_CAPTURE_BYTES)
                try:
                    snapshots[phase] = parse_capture(commands.raw(capture))
                except CaptureLayoutError as error:
                    layout_errors[phase] = str(error)
                checked(phase + "-unpause", ["docker", "unpause", name])
            finished = session.finish(True)
            session = None
            require(finished, "Candidate session termination incomplete")
    except (DriverError, OSError, subprocess.SubprocessError) as error:
        errors.append(type(error).__name__ + ":" + str(error)[:500])
    finally:
        if container_attempted:
            container_cleanup = remove_container()
        else:
            container_cleanup = True
        if session is not None:
            try:
                session.finish(False)
            except (OSError, subprocess.SubprocessError, DriverError) as error:
                errors.append("session-cleanup:" + type(error).__name__)
        if not container_cleanup:
            errors.append("container:cleanup-unverified")
        if volume_attempted:
            try:
                owned = checked("volume-cleanup-inspect", ["docker", "volume", "inspect", "--format", "{{json .}}", volume])
                require(transport._volume_valid(json_record(owned), volume, execution_id), "Volume cleanup ownership differs")
                require(container_cleanup, "Container cleanup required before volume removal")
                checked("volume-remove", ["docker", "volume", "rm", volume])
                absent = checked("volume-after", ["docker", "volume", "ls", "--quiet", "--filter", "name=^" + volume + "$"])
                volume_cleanup = not commands.raw(absent).strip()
            except (DriverError, OSError, subprocess.SubprocessError) as error:
                errors.append("volume-cleanup:" + type(error).__name__)
        else:
            volume_cleanup = True
        if not volume_cleanup:
            errors.append("volume:cleanup-unverified")
    if driver_sources() != sources:
        errors.append("driver:source-changed")
    terminal = {"protocol": PROTOCOL, "intent_sha256": sha256((root / "intent.json").read_bytes()),
        "case_id": case_id, "source_sha256": actual_source, "responses": responses, "runtime": runtime,
        "applicability": applicability,
        "captures": {phase: [{"path": path, "bytes": len(raw), "sha256": sha256(raw)}
                     for path, raw in sorted(snapshot.items())] for phase, snapshot in snapshots.items()},
        "layout_errors": layout_errors, "infrastructure": errors,
        "container_cleanup": container_cleanup, "volume_cleanup": volume_cleanup,
        "commands": list(commands.records)}
    raw_terminal = encoded(terminal)
    commands.retain("terminal.json", raw_terminal)
    return StorageExecution(case_id, actual_source, str(root), snapshots, responses, tuple(errors),
        layout_errors, container_cleanup and volume_cleanup, sha256(raw_terminal))
