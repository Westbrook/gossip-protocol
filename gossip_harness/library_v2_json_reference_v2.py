"""Source-bound JSON intake repair over the frozen prospective-v2 reference.

Only generated ingestion/jobs.py changes. This trusted source generator does not
import or execute generated product code; physical qualification stays separate.
"""
from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
from pathlib import Path

from . import library_v2_reference_v1 as baseline

PROTOCOL = "library-v2-json-reference-v2"
JOBS_PATH = "library/ingestion/jobs.py"
BASELINE_TREE_SHA256 = "a38c74501ce38b0d8fb05eaebf551d70f04b9229360908d0817b5946ee5525ba"
BASELINE_JOBS_SHA256 = "a41620446485c3c69501f36898533a6975cb325a761a957d87f1c56e598e7116"
_INPUT_SHA256 = {
    "fixtures/library-m4-compatibility-v1/README.md": "c59d7bce0ec8d40b753da1202c914aed159e66c7d36653dd50514835449ea267",
    "fixtures/library-m4-compatibility-v1/build_snapshots.py": "10b013d1284851ad874c23466b61ea248cd1b7f53f694de335a38efb4b493941",
    "fixtures/library-m4-compatibility-v1/m2.manifest.json": "754146fb8643f36faeb6bb76c2f3dbb354a4aed4464189eb2d68b8f4981f9644",
    "fixtures/library-m4-compatibility-v1/m2.sqlite3": "4d52246ca238887d51649fbc63b923a8dfbdfd545238a83cb6c5e0a5df94c1c2",
    "fixtures/library-m4-compatibility-v1/v0.manifest.json": "29bcdfd24eeac87d025926f72c4d712be161d14d70dace30c31dcef04411f81a",
    "fixtures/library-m4-compatibility-v1/v0.sqlite3": "5a550a1a681d1ea0c119394906580c859af51caaf0eabd83e437513dc5b53429",
    "gossip_harness/library_cumulative_public_fixture_v1.py": "694b35895cdeb74df4dca55a65e44cab7cef52d52cf9f3f003e1828b518e9647",
    "gossip_harness/library_cumulative_public_fixture_v2.py": "03e2e74c796084355a37e1e35c9532a81f12749e7cd8cfd17fa902d358019052",
    "gossip_harness/library_m1_clients_reference_v1.py": "e52b6477bb6c5460547d7bde84986e347ab7d16ae07708a6fe28021b5e96a17f",
    "gossip_harness/library_m1_ingestion_reference_v1.py": "514ad7c5caa546a6b33359af3b9c2d04475a05176a2131ac4b562d38fe1db5ff",
    "gossip_harness/library_m1_reference_v1.py": "80e3c2ed79610f2830abdeb59e38ae57b746051c6af538985d96482568882c08",
    "gossip_harness/library_m2_browser_reference_v1.py": "63e047e4c277e49160bbaba2fd14061772f75bf832923c31542cb5ef51e1b825",
    "gossip_harness/library_m2_catalog_reference_v1.py": "4ad0e946e24c986c7b4c0d5ef41ccf7da0096555ececd9c9e62deb9d55754d56",
    "gossip_harness/library_m2_clients_reference_v1.py": "49ccc15b651e7b650b30bbae7b1a1ac9c4b67e7bfa398577fbc00e0b78040d2d",
    "gossip_harness/library_m2_reference_v1.py": "000bdcd8cf167348899875448fa49f1ed599c5ac7273731bbb94accdc35979be",
    "gossip_harness/library_m3_backup_format_v1.py": "5ab258089d064139b1b2f2adfe98bec006d027c2f99783f2cfe635f7a8136e77",
    "gossip_harness/library_m3_backup_reference_v1.py": "5c276fde9ffad85609a18a91be9746862d2ea0b17120412f67b6d74b1ae2cee9",
    "gossip_harness/library_m3_browser_reference_v1.py": "522d99198ae851f038d88ee28d93b45b67c70511890ac12481d81509e1586dec",
    "gossip_harness/library_m3_clients_reference_v1.py": "19a58f61d16fedc0f8305e2777990561defebb7d5cd72c1980a663e774fdd03b",
    "gossip_harness/library_m3_control_reference_v1.py": "38df5e59bf10ac99c6f4e3d9c958cecee1b0dfb5a58a71afadd3b22757e8bb43",
    "gossip_harness/library_m3_maintenance_reference_v1.py": "e05b73a3edf7900c8c890c5c330f55f80befb0566db7b0fac5fdd139e74332cb",
    "gossip_harness/library_m3_reference_v1.py": "3ad17289d83fca6bc576cc001a2bba6bdf52d2cbf3c5dc536fe0222e2a89f39c",
    "gossip_harness/library_m3_worker_reference_v1.py": "43c7bc3753cae763e0d6522fc72bc0548b12f17e39998efa11853c6782bbe8f3",
    "gossip_harness/library_m4_backup_reference_v1.py": "4e59a68b6f71e9af68d321a2beff50e15c3f82b4c150965995458911fc3dc3fc",
    "gossip_harness/library_m4_browser_reference_v1.py": "233cc5e0dc01dd566fcd2062731694324454afb265a3248200435c8e2b49df5d",
    "gossip_harness/library_m4_catalog_reference_v1.py": "73055d1dd8cb42ef39cd37c3b0911c1e74f012d7520068c39de40962d2d6717c",
    "gossip_harness/library_m4_clients_reference_v1.py": "c65f622ea49bad039602b82e33d9398fbeb05cfe90baa8e7b83be44bed423728",
    "gossip_harness/library_m4_fixture_v1.py": "61c1d53e0a582927639a9fcde955e3fd0e9a66288aac3a291fbfb7d670a8764c",
    "gossip_harness/library_m4_reference_v1.py": "6df29231508ee642ddfe60dfbe5f36fe8382deb4692ac7177728b0183e5e1452",
    "gossip_harness/library_m4_release_reference_v1.py": "934863d6bd0c38de5f6c5b8f862d8ed6dbda90fbbd29864f07f51efdef78de30",
    "gossip_harness/library_project_fixture_v1.py": "fa4440f1ac7e63e3b4ef7d9978a8970e8d6cd688efe9c2e1b6d1e75c0f757caf",
    "gossip_harness/library_v2_browser_reference_v1.py": "38cb7fff3ef5d81a5cc68c9bd1b28c957df94db0955009b2892af24cdc434214",
    "gossip_harness/library_v2_catalog_reference_v1.py": "26fabb8b0e7a317fe5240e3a3d7bea912b844432c541d850799d1275fb27fb82",
    "gossip_harness/library_v2_clients_reference_v1.py": "a9ff6bfb7daf4c9d7b539fac30fd2992d076f7490ef278ad52a94f7e4074a369",
    "gossip_harness/library_v2_maintenance_reference_v1.py": "3888260d5d622ca8ac471be5fde43730fe4b69413bb9a508ecae0d3d476cc50f",
    "gossip_harness/library_v2_reference_v1.py": "dbbcd0e46c2a9c3d6d79a9e3d1129d45536f59eb86453f9472f8c409077ee97d",
    "gossip_harness/library_v2_release_reference_v1.py": "abcfe610444a88baa8ea024c9638a5cc6ff9c887045114efb7cdaf9133adf881",
    "library-cumulative-product-v1.json": "3353dbaa0c2474527cbffb0d87a4091b5f77ed3f98418fff2f216d90b6a8487c",
    "library-cumulative-product-v2.json": "2d88ce0775888f148b0ec3caf90b3d5c82d8fed71f53bec5f7e75f492ae998dc",
    "library-m1-acceptance-inventory-v1.json": "f5d808866d6f18f3fd095de5d447bb5b0e2a43e20771761fe91a38cf4f59ad4a"
}
_SOURCE = Path(__file__).resolve()
_ROOT = _SOURCE.parents[1]
_LOADED_SOURCE_SHA256 = hashlib.sha256(_SOURCE.read_bytes()).hexdigest()

# Reading JSON to EOF introduces no encoded-byte product limit. Execution memory
# and deadlines still apply outside the product; they must not become too_large.
_JSON_READER = '''def _read_json_path(path):
    """Read JSON with the same confined descriptors and I/O errors as ZIP."""
    fd = _path_fd(path)
    try:
        pieces = []
        while True:
            piece = os.read(fd, 65536)
            if not piece:
                return b''.join(pieces)
            pieces.append(piece)
    except OSError as error:
        raise _io_error(error) from error
    finally:
        os.close(fd)

'''
_JSON_OLD = "    def submit_json(self, job_id, bundle_path):\n        raw = _read_path(bundle_path, ARCHIVE_BYTES)"
_JSON_NEW = "    def submit_json(self, job_id, bundle_path):\n        raw = _read_json_path(bundle_path)"
_HELPER_SEAM = "def _append(entries, seen, source, text, total):"


def corrected_v2_source_inputs() -> dict[str, str]:
    """Recheck the explicit transitive generators, fixtures and normative inputs.

    The returned dictionary is independent. Source pins and the exact generated
    tree guard together bind both on-disk dependencies and their loaded output.
    The new module is also checked against its import-time source identity.
    The caller records this inventory in its prospective execution freeze.
    """
    if hashlib.sha256(_SOURCE.read_bytes()).hexdigest() != _LOADED_SOURCE_SHA256:
        raise ValueError("Loaded JSON reference repair source changed")
    for name, expected in _INPUT_SHA256.items():
        path = _ROOT / name
        if path.is_symlink() or not path.is_file():
            raise ValueError("Reference input must be an ordinary file: " + name)
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("Reference source input changed: " + name)
    return dict(_INPUT_SHA256) | {"gossip_harness/library_v2_json_reference_v2.py": _LOADED_SOURCE_SHA256}


def _tree_sha256(files: Mapping[str, bytes]) -> str:
    records = {name: hashlib.sha256(raw).hexdigest() for name, raw in sorted(files.items())}
    return hashlib.sha256(json.dumps(records, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _baseline_sources() -> tuple[dict[str, str], dict[str, bytes]]:
    corrected_v2_source_inputs()
    texts, binaries = baseline.v2_files(), baseline.v2_binary_files()
    if set(texts).intersection(binaries):
        raise ValueError("Frozen baseline text/binary source collision")
    raw = {name: text.encode("utf-8") for name, text in texts.items()} | binaries
    if _tree_sha256(raw) != BASELINE_TREE_SHA256:
        raise ValueError("Frozen baseline generated source tree changed")
    corrected_v2_source_inputs()
    return texts, binaries


def _repair_jobs(source: str) -> str:
    if hashlib.sha256(source.encode("utf-8")).hexdigest() != BASELINE_JOBS_SHA256:
        raise ValueError("Frozen JSON intake source changed")
    if source.count(_JSON_OLD) != 1 or source.count(_HELPER_SEAM) != 1:
        raise ValueError("Frozen JSON intake repair seam changed")
    if "def _read_json_path(" in source:
        raise ValueError("JSON intake repair already exists")
    return source.replace(_JSON_OLD, _JSON_NEW).replace(_HELPER_SEAM, _JSON_READER + _HELPER_SEAM)


def corrected_v2_files() -> dict[str, str]:
    """Fresh full v2 text tree with the one source-checked JSON intake repair."""
    files, _ = _baseline_sources()
    files[JOBS_PATH] = _repair_jobs(files[JOBS_PATH])
    return files


def corrected_v2_binary_files() -> dict[str, bytes]:
    """Unchanged compatibility snapshots, bound to the same complete baseline."""
    _, binaries = _baseline_sources()
    return binaries
