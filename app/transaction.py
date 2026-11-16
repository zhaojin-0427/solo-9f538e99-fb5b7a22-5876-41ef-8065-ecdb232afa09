"""FHIR R4 transaction Bundle ingestion.

Scope
-----
* ``Bundle.type = transaction``, at most ``FHIR_MAX_BUNDLE_ENTRIES`` (30) entries.
* Entries are ``POST`` only and only ``Patient``, ``Specimen`` and ``Observation``.
* ``Bundle.id`` makes ingestion idempotent: an identical body replays the stored
  response; a different body under the same id is a ``409`` conflict. The record
  lives in SQLite, so it survives restarts.
* A Patient is matched by any complete ``identifier(system, value)`` pair. A
  matched patient is reused as-is: existing demographics and identifiers are
  never rewritten. Patient fullUrls that resolve to the same identity are merged.
* References must be a fullUrl of an entry in the same bundle (forward
  references allowed) or a permanent library address
  ``<FHIR_BASE_URL>/<Type>/<id>`` of an existing resource. Dangling or
  ambiguous references reject the whole bundle. Stored references are rewritten
  to permanent addresses.
* ``Specimen.subject`` must resolve to a Patient. ``Observation.subject`` and
  the patient behind ``Observation.specimen`` must be the same patient.
* Everything happens in one SQLite write transaction; a rejected bundle writes
  nothing.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from . import settings
from .store import Store

SUPPORTED_TYPES = ("Patient", "Specimen", "Observation")
SUPPORTED_TYPES_SET = frozenset(SUPPORTED_TYPES)

_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")

# Status codes used by this module:
#   400 - the HTTP body is not a JSON object / not a Bundle
#   409 - Bundle.id replay conflict, or a requested resource id already exists
#   422 - bundle content rejected (unsupported request, dangling reference, ...)
MALFORMED = 400
CONFLICT = 409
REJECTED = 422


class BundleError(Exception):
    """A bundle was rejected. Nothing from it was written."""

    def __init__(self, status: int, message: str,
                 issues: list[dict[str, Any]] | None = None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.issues = issues or []


def _issue(severity: str, code: str, diagnostics: str,
           expression: str | None = None) -> dict[str, Any]:
    issue: dict[str, Any] = {
        "severity": severity,
        "code": code,
        "diagnostics": diagnostics,
    }
    if expression is not None:
        issue["expression"] = [expression]
    return issue


def _fail(status: int, message: str, expression: str | None = None,
          code: str = "invalid", severity: str = "error") -> BundleError:
    return BundleError(status, message, [_issue(severity, code, message, expression)])


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def fingerprint(bundle: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical(bundle).encode("utf-8")).hexdigest()


@dataclass
class Entry:
    index: int
    full_url: str
    resource: dict[str, Any]

    @property
    def resource_type(self) -> str:
        return str(self.resource.get("resourceType"))

    @property
    def expr(self) -> str:
        return f"Bundle.entry[{self.index}]"


# --------------------------------------------------------------------------- parsing


def _head(bundle: Any) -> None:
    if not isinstance(bundle, dict):
        raise BundleError(MALFORMED, "Request body must be a FHIR Bundle object",
                          [_issue("error", "invalid", "Body is not a JSON object")])
    if bundle.get("resourceType") != "Bundle":
        raise _fail(REJECTED, "resourceType must be 'Bundle'", "resourceType")
    if bundle.get("type") != "transaction":
        raise _fail(REJECTED, "Only Bundle.type = 'transaction' is supported", "type")
    bundle_id = bundle.get("id")
    if bundle_id is not None and (
        not isinstance(bundle_id, str) or not _ID_RE.match(bundle_id)
    ):
        raise _fail(
            REJECTED,
            "Bundle.id must be a valid FHIR id (letters, digits, '.', '_', '-'; "
            "at most 64 characters)",
            "id",
        )


def _entries(bundle: dict[str, Any]) -> list[Entry]:
    raw_entries = bundle.get("entry")
    if raw_entries is None:
        raw_entries = []
    if not isinstance(raw_entries, list):
        raise _fail(REJECTED, "Bundle.entry must be an array", "entry")
    if len(raw_entries) > settings.MAX_BUNDLE_ENTRIES:
        raise BundleError(
            REJECTED,
            f"A transaction may contain at most {settings.MAX_BUNDLE_ENTRIES} entries",
            [_issue("error", "too-costly",
                    f"{len(raw_entries)} entries exceed the limit of "
                    f"{settings.MAX_BUNDLE_ENTRIES}", "entry")],
        )

    entries: list[Entry] = []
    for index, raw in enumerate(raw_entries):
        expr = f"Bundle.entry[{index}]"
        if not isinstance(raw, dict):
            raise _fail(REJECTED, "Each entry must be an object", expr)
        request = raw.get("request")
        if not isinstance(request, dict):
            raise _fail(REJECTED, "Each entry requires request",
                        expr + ".request", code="required")
        if request.get("method") != "POST":
            raise _fail(
                REJECTED,
                "Only POST entries are accepted; update, delete and GET are not supported",
                expr + ".request.method", code="not-supported",
            )
        resource = raw.get("resource")
        if not isinstance(resource, dict):
            raise _fail(REJECTED, "Each entry requires a resource",
                        expr + ".resource", code="required")
        resource_type = resource.get("resourceType")
        if resource_type not in SUPPORTED_TYPES_SET:
            raise _fail(
                REJECTED,
                f"resourceType {resource_type!r} is not supported; only Patient, "
                "Specimen and Observation are accepted",
                expr + ".resource.resourceType", code="not-supported",
            )
        full_url = raw.get("fullUrl")
        if not isinstance(full_url, str) or not full_url.strip():
            raise _fail(REJECTED,
                        "Each entry requires a fullUrl so references can be resolved",
                        expr + ".fullUrl", code="required")
        url = request.get("url")
        if url not in (None, ""):
            if not isinstance(url, str):
                raise _fail(REJECTED, "request.url must be a string",
                            expr + ".request.url")
            allowed = {resource_type, f"{settings.BASE_URL}/{resource_type}"}
            if url not in allowed:
                raise _fail(
                    REJECTED,
                    f"request.url {url!r} is not supported; use POST {resource_type}",
                    expr + ".request.url", code="not-supported",
                )
        entries.append(Entry(index, full_url, resource))

    seen: dict[str, int] = {}
    for entry in entries:
        if entry.full_url in seen:
            raise _fail(
                REJECTED,
                f"fullUrl {entry.full_url!r} is used by entries "
                f"{seen[entry.full_url]} and {entry.index}",
                entry.expr + ".fullUrl", code="duplicate",
            )
        seen[entry.full_url] = entry.index
    return entries


def _identifier_pairs(patient: dict[str, Any]) -> list[tuple[str, str]]:
    """Complete, de-duplicated (system, value) pairs used for matching.

    Incomplete identifiers are ignored for matching but preserved verbatim in
    the stored Patient.
    """
    identifiers = patient.get("identifier")
    if not isinstance(identifiers, list):
        return []
    pairs: list[tuple[str, str]] = []
    for item in identifiers:
        if not isinstance(item, dict):
            continue
        system, value = item.get("system"), item.get("value")
        if isinstance(system, str) and isinstance(value, str) and system and value:
            pair = (system, value)
            if pair not in pairs:
                pairs.append(pair)
    return pairs


def _require_identity_pairs(entry: Entry) -> list[tuple[str, str]]:
    pairs = _identifier_pairs(entry.resource)
    if not pairs:
        raise _fail(
            REJECTED,
            "A Patient requires at least one identifier with both system and value so "
            "the same person can be reused instead of duplicated",
            entry.expr + ".resource.identifier", code="required",
        )
    return pairs


# --------------------------------------------------------------------------- references


def _refs(value: Any) -> Iterable[tuple[dict[str, Any], Any]]:
    """Yield every object that carries a ``reference`` entry (string or not)."""
    if isinstance(value, list):
        for item in value:
            yield from _refs(item)
        return
    if not isinstance(value, dict):
        return
    if "reference" in value:
        yield value, value["reference"]
    for name, item in value.items():
        if name != "reference":
            yield from _refs(item)


def _rewrite_refs(node: Any, resolve: Any) -> None:
    if isinstance(node, list):
        for item in node:
            _rewrite_refs(item, resolve)
        return
    if not isinstance(node, dict):
        return
    reference = node.get("reference")
    if isinstance(reference, str):
        node["reference"] = resolve(reference)
    for name, item in node.items():
        if name != "reference":
            _rewrite_refs(item, resolve)


def _requested_id(entry: Entry) -> str | None:
    resource_id = entry.resource.get("id")
    if resource_id is None:
        return None
    if not isinstance(resource_id, str) or not _ID_RE.match(resource_id):
        raise _fail(REJECTED, "Resource.id must be a valid FHIR id",
                    entry.expr + ".resource.id")
    return resource_id


def _permanent(resource_type: str, resource_id: str) -> str:
    return f"{settings.BASE_URL}/{resource_type}/{resource_id}"


def _parse_library_ref(reference: str) -> tuple[str, str] | None:
    """Parse ``<FHIR_BASE_URL>/<Type>/<id>``; return None for anything else."""
    prefix = settings.BASE_URL + "/"
    if not reference.startswith(prefix):
        return None
    remainder = reference[len(prefix):]
    if not remainder or remainder.endswith("/"):
        return None
    parts = remainder.split("/")
    if len(parts) != 2:
        return None
    resource_type, resource_id = parts
    if resource_type not in SUPPORTED_TYPES_SET or not _ID_RE.match(resource_id):
        return None
    return resource_type, resource_id


def _resource_row(conn: sqlite3.Connection, resource_type: str,
                  resource_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT content FROM resources WHERE resource_type = ? AND id = ?",
        (resource_type, resource_id),
    ).fetchone()


# --------------------------------------------------------------------------- engine


def _load_identities(conn: sqlite3.Connection) -> dict[tuple[str, str], str]:
    rows = conn.execute(
        "SELECT i.system, i.value, i.patient_id FROM patient_identities i "
        "JOIN resources r ON r.id = i.patient_id AND r.resource_type = 'Patient'"
    ).fetchall()
    return {
        (row["system"], row["value"]): _permanent("Patient", row["patient_id"])
        for row in rows
    }


def ingest(store: Store, bundle: dict[str, Any]) -> dict[str, Any]:
    """Validate and atomically apply a transaction Bundle. Raises BundleError."""
    _head(bundle)
    entries = _entries(bundle)
    bundle_id = bundle.get("id")
    body_fingerprint = fingerprint(bundle)

    # Cheap, lock-free path for an identical retry.
    replay = _replay(store, bundle_id, body_fingerprint)
    if replay is not None:
        return replay

    with store.transaction() as conn:
        # Re-check under the write lock: a concurrent duplicate may have won.
        # Writers are serialised, so exactly one bundle becomes the original.
        if isinstance(bundle_id, str) and bundle_id:
            row = conn.execute(
                "SELECT fingerprint, response FROM bundle_records WHERE bundle_id = ?",
                (bundle_id,),
            ).fetchone()
            if row is not None:
                if row["fingerprint"] != body_fingerprint:
                    raise _id_conflict(bundle_id)
                return json.loads(row["response"])

        result = _execute(conn, entries)
        if isinstance(bundle_id, str) and bundle_id:
            conn.execute(
                "INSERT INTO bundle_records(bundle_id, fingerprint, response, created_at) "
                "VALUES (?,?,?,?)",
                (
                    bundle_id,
                    body_fingerprint,
                    # Preserve the response exactly as first returned so an
                    # idempotent replay is indistinguishable to the client.
                    json.dumps(result, ensure_ascii=False, separators=(",", ":")),
                    time.time(),
                ),
            )
        return result


def _replay(store: Store, bundle_id: Any, body_fingerprint: str) -> dict[str, Any] | None:
    if not isinstance(bundle_id, str) or not bundle_id:
        return None
    conn = store.raw_connection()
    try:
        row = conn.execute(
            "SELECT fingerprint, response FROM bundle_records WHERE bundle_id = ?",
            (bundle_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        return None
    if row["fingerprint"] != body_fingerprint:
        raise _id_conflict(bundle_id)
    return json.loads(row["response"])


def _id_conflict(bundle_id: str) -> BundleError:
    return BundleError(
        CONFLICT,
        f"Bundle.id {bundle_id!r} was already processed with a different payload",
        [_issue("error", "conflict",
                "An idempotent retry must repeat the exact original bundle body", "id")],
    )


def _execute(conn: sqlite3.Connection, entries: list[Entry]) -> dict[str, Any]:
    identities = _load_identities(conn)

    # ---- 1. Patient identity resolution ---------------------------------
    # owner is either a fullUrl of a Patient created by this bundle, or the
    # permanent address of a Patient already in the library.
    patient_owner: dict[str, str] = {}
    created_patient_owners: set[str] = set()
    new_pairs: dict[str, list[tuple[str, str]]] = {}

    for entry in entries:
        if entry.resource_type != "Patient":
            continue
        pairs = _require_identity_pairs(entry)
        _requested_id(entry)
        owners = {identities[pair] for pair in pairs if pair in identities}
        if len(owners) > 1:
            raise _fail(
                REJECTED,
                "This patient's identifiers match more than one existing patient; "
                "the identity is ambiguous",
                entry.expr + ".resource.identifier", code="conflict",
            )
        if owners:
            owner = next(iter(owners))
            # Existing patient: match only; never rewrite its identity. Extra
            # identifiers from this entry are registered only for resolving
            # references inside this bundle, so fullUrls that share an identity
            # merge onto the same patient while the stored resource keeps its
            # own identity untouched.
            for pair in pairs:
                identities.setdefault(pair, owner)
        else:
            owner = entry.full_url
            created_patient_owners.add(owner)
            new_pairs.setdefault(owner, [])
            for pair in pairs:
                if pair not in new_pairs[owner]:
                    new_pairs[owner].append(pair)
                identities.setdefault(pair, owner)
        patient_owner[entry.full_url] = owner

    # ---- 2. Allocate ids and permanent addresses ------------------------
    assigned_ids: dict[str, str] = {}
    used_ids: set[tuple[str, str]] = set()
    for entry in entries:
        if entry.resource_type == "Patient" and entry.full_url not in created_patient_owners:
            continue  # reused existing patient: no new resource
        resource_id = _requested_id(entry) or str(uuid.uuid4())
        key = (entry.resource_type, resource_id)
        if key in used_ids:
            raise _fail(
                REJECTED,
                f"Two entries of this bundle both use id {resource_id!r} for "
                f"{entry.resource_type}",
                entry.expr + ".resource.id", code="duplicate",
            )
        if _resource_row(conn, entry.resource_type, resource_id) is not None:
            raise BundleError(
                CONFLICT,
                f"{entry.resource_type}/{resource_id} already exists in the library",
                [_issue("error", "duplicate",
                        "POST creates new resources and never overwrites an existing id",
                        entry.expr + ".resource.id")],
            )
        used_ids.add(key)
        assigned_ids[entry.full_url] = resource_id

    owner_permanent: dict[str, str] = {
        owner: _permanent("Patient", assigned_ids[owner])
        for owner in created_patient_owners
    }

    full_url_to_perm: dict[str, str] = {}
    full_url_to_type: dict[str, str] = {}
    perm_to_type: dict[str, str] = {}
    for entry in entries:
        full_url_to_type[entry.full_url] = entry.resource_type
        if entry.resource_type == "Patient":
            owner = patient_owner[entry.full_url]
            perm = owner_permanent.get(owner, owner)
        else:
            perm = _permanent(entry.resource_type, assigned_ids[entry.full_url])
        full_url_to_perm[entry.full_url] = perm
        perm_to_type.setdefault(perm, entry.resource_type)

    # A reference string must mean exactly one resource. If an entry's fullUrl
    # is also the permanent address of a *different* resource (one already in
    # the library, or one this bundle places), the string is ambiguous.
    perm_owners: dict[str, set[str]] = {}
    for entry in entries:
        perm_owners.setdefault(full_url_to_perm[entry.full_url], set()).add(entry.full_url)
    for entry in entries:
        full_url = entry.full_url
        own_perm = full_url_to_perm[full_url]
        if full_url == own_perm:
            continue
        if full_url in perm_owners:
            raise _fail(
                REJECTED,
                f"fullUrl {full_url!r} is also the permanent address of another "
                "resource in this bundle; the reference is ambiguous",
                entry.expr + ".fullUrl", code="conflict",
            )
        parsed = _parse_library_ref(full_url)
        if parsed is not None and _resource_row(conn, *parsed) is not None:
            raise _fail(
                REJECTED,
                f"fullUrl {full_url!r} is the permanent address of an existing "
                "resource and cannot also identify a new resource",
                entry.expr + ".fullUrl", code="conflict",
            )

    # ---- 3. Reference resolution and generic validation -----------------
    library_cache: dict[str, tuple[str, str] | None] = {}

    def resolve(reference: Any, expr: str) -> tuple[str, str]:
        """Return (permanent address, resourceType), or reject the bundle."""
        if not isinstance(reference, str) or not reference:
            raise _fail(
                REJECTED,
                "Every Reference must carry a non-empty reference string",
                expr, code="required",
            )
        if reference in full_url_to_perm:
            return full_url_to_perm[reference], full_url_to_type[reference]
        if reference in perm_to_type:
            # A permanent address allocated by this bundle (predictable id).
            return reference, perm_to_type[reference]
        if reference not in library_cache:
            parsed = _parse_library_ref(reference)
            if parsed is None:
                raise _fail(
                    REJECTED,
                    f"Reference {reference!r} is neither a fullUrl of this bundle nor "
                    f"a permanent library address under {settings.BASE_URL}/<Type>/<id>",
                    expr, code="not-found",
                )
            resource_type, resource_id = parsed
            row = _resource_row(conn, resource_type, resource_id)
            library_cache[reference] = (
                None if row is None else (reference, resource_type)
            )
        cached = library_cache[reference]
        if cached is None:
            raise _fail(
                REJECTED,
                f"Reference {reference!r} does not resolve to an existing resource",
                expr, code="not-found",
            )
        return cached

    for entry in entries:
        for _node, reference in _refs(entry.resource):
            resolve(reference, entry.expr + ".resource")

    # ---- 4. Topology: subjects and specimens ----------------------------
    def patient_key(reference: str, expr: str) -> str:
        """Canonical patient identity: the permanent address of the patient.

        Two fullUrls merged onto the same patient therefore compare equal, and
        a library patient's address compares equal whether reached through a
        fullUrl or directly.
        """
        perm, resource_type = resolve(reference, expr)
        if resource_type != "Patient":
            raise _fail(
                REJECTED,
                f"{reference!r} points to a {resource_type}, but a Patient is required",
                expr,
            )
        return perm

    def required_ref(resource: dict[str, Any], field: str,
                     expected_type: str, expr: str) -> tuple[str, str]:
        value = resource.get(field)
        reference = value.get("reference") if isinstance(value, dict) else None
        if not isinstance(reference, str) or not reference:
            raise _fail(
                REJECTED,
                f"{resource.get('resourceType')}.{field} is required and must be a "
                f"reference to a {expected_type}",
                expr, code="required",
            )
        perm, resource_type = resolve(reference, expr)
        if resource_type != expected_type:
            raise _fail(
                REJECTED,
                f"{resource.get('resourceType')}.{field} {reference!r} points to a "
                f"{resource_type}, but a {expected_type} is required",
                expr,
            )
        return reference, perm

    specimen_patient_key: dict[str, str] = {}
    for entry in entries:
        if entry.resource_type != "Specimen":
            continue
        reference, _perm = required_ref(
            entry.resource, "subject", "Patient", entry.expr + ".resource.subject"
        )
        specimen_patient_key[entry.full_url] = patient_key(
            reference, entry.expr + ".resource.subject"
        )

    for entry in entries:
        if entry.resource_type != "Observation":
            continue
        subject_ref, _subject_perm = required_ref(
            entry.resource, "subject", "Patient", entry.expr + ".resource.subject"
        )
        observation_patient = patient_key(
            subject_ref, entry.expr + ".resource.subject"
        )

        specimen_ref, _specimen_perm = required_ref(
            entry.resource, "specimen", "Specimen", entry.expr + ".resource.specimen"
        )
        if specimen_ref in specimen_patient_key:
            specimen_patient = specimen_patient_key[specimen_ref]
        else:
            # A Specimen already in the library: follow its stored subject.
            parsed = _parse_library_ref(specimen_ref)
            assert parsed is not None  # resolve() already accepted it
            row = _resource_row(conn, *parsed)
            if row is None:
                raise _fail(REJECTED, f"Specimen {specimen_ref!r} no longer exists",
                            entry.expr + ".resource.specimen", code="not-found")
            stored = json.loads(row["content"])
            stored_subject = stored.get("subject")
            stored_ref = (
                stored_subject.get("reference")
                if isinstance(stored_subject, dict) else None
            )
            if not isinstance(stored_ref, str) or not stored_ref:
                raise _fail(
                    REJECTED,
                    f"Specimen {specimen_ref!r} has no subject patient to compare",
                    entry.expr + ".resource.specimen",
                )
            specimen_patient, stored_type = resolve(
                stored_ref, entry.expr + ".resource.specimen"
            )
            if stored_type != "Patient":
                raise _fail(
                    REJECTED,
                    f"Specimen {specimen_ref!r} does not belong to a Patient",
                    entry.expr + ".resource.specimen",
                )
        if observation_patient != specimen_patient:
            raise _fail(
                REJECTED,
                "Observation.subject and the patient of Observation.specimen must be "
                "the same patient",
                entry.expr + ".resource", code="conflict",
            )

    # ---- 5. Persist everything (one transaction) ------------------------
    for entry in entries:
        if entry.resource_type == "Patient" and entry.full_url not in created_patient_owners:
            continue
        resource = json.loads(json.dumps(entry.resource, ensure_ascii=False))
        resource["id"] = assigned_ids[entry.full_url]
        _rewrite_refs(
            resource,
            lambda reference, expr=entry.expr + ".resource": resolve(reference, expr)[0],
        )
        conn.execute(
            "INSERT INTO resources(resource_type, id, content, created_at) "
            "VALUES (?,?,?,?)",
            (entry.resource_type, resource["id"], _canonical(resource), time.time()),
        )
        if entry.resource_type == "Patient":
            for system, value in new_pairs.get(entry.full_url, []):
                conn.execute(
                    "INSERT INTO patient_identities(system, value, patient_id) "
                    "VALUES (?,?,?)",
                    (system, value, resource["id"]),
                )

    # ---- 6. Response in request order -----------------------------------
    response_entries: list[dict[str, Any]] = []
    for entry in entries:
        reused = (
            entry.resource_type == "Patient"
            and entry.full_url not in created_patient_owners
        )
        response_entries.append({
            "fullUrl": entry.full_url,
            "response": {
                "status": "200 OK" if reused else "201 Created",
                "location": full_url_to_perm[entry.full_url],
            },
        })

    return {
        "resourceType": "Bundle",
        "type": "transaction-response",
        "entry": response_entries,
    }
