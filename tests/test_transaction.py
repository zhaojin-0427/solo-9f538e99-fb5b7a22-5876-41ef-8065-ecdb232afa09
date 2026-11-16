"""End-to-end tests for the FHIR R4 transaction ingest API."""
from __future__ import annotations

import json
import multiprocessing as mp
import threading
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.store import Store
from app.transaction import BundleError, ingest

FHIR_HEADERS = {"content-type": "application/fhir+json"}
BASE = "http://localhost:8000"


# --------------------------------------------------------------------------- helpers


def patient(full_url: str, mrn: str, **extra: Any) -> dict[str, Any]:
    return {
        "fullUrl": full_url,
        "resource": {
            "resourceType": "Patient",
            "identifier": [{"system": "urn:lis:mrn", "value": mrn}],
            **extra,
        },
        "request": {"method": "POST", "url": "Patient"},
    }


def specimen(full_url: str, subject: str) -> dict[str, Any]:
    return {
        "fullUrl": full_url,
        "resource": {
            "resourceType": "Specimen",
            "status": "available",
            "type": {"text": "Serum"},
            "subject": {"reference": subject},
        },
        "request": {"method": "POST", "url": "Specimen"},
    }


def observation(full_url: str, subject: str, spec: str) -> dict[str, Any]:
    return {
        "fullUrl": full_url,
        "resource": {
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"system": "http://loinc.org", "code": "2345-7"}]},
            "subject": {"reference": subject},
            "specimen": {"reference": spec},
            "valueQuantity": {"value": 5.4, "unit": "mmol/L"},
        },
        "request": {"method": "POST", "url": "Observation"},
    }


def bundle(bundle_id: str | None, *entries: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": list(entries),
    }
    if bundle_id is not None:
        result["id"] = bundle_id
    return result


@pytest.fixture()
def db_path(tmp_path):
    return tmp_path / "fhir.sqlite3"


@pytest.fixture()
def client(db_path):
    app = create_app(Store(db_path))
    with TestClient(app) as test_client:
        yield test_client


def post(client: TestClient, payload: dict[str, Any]):
    return client.post("/", content=json.dumps(payload), headers=FHIR_HEADERS)


def entry_location(response, index: int) -> str:
    return response.json()["entry"][index]["response"]["location"]


def read(client: TestClient, location: str) -> dict[str, Any]:
    return client.get(location.replace(BASE, "")).json()


def _references(value):
    if isinstance(value, list):
        for item in value:
            yield from _references(item)
    elif isinstance(value, dict):
        if isinstance(value.get("reference"), str):
            yield value
        for name, item in value.items():
            if name != "reference":
                yield from _references(item)


# --------------------------------------------------------------------------- happy path


def test_ingests_patient_specimen_observation(client):
    response = post(
        client,
        bundle(
            "ok-1",
            patient("urn:uuid:p", "MRN-1"),
            specimen("urn:uuid:s", "urn:uuid:p"),
            observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:s"),
        ),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["resourceType"] == "Bundle"
    assert body["type"] == "transaction-response"
    assert [e["response"]["status"] for e in body["entry"]] == ["201 Created"] * 3

    patient_perm = entry_location(response, 0)
    patient_id = patient_perm.rsplit("/", 1)[-1]
    created_patient = read(client, patient_perm)
    assert created_patient["id"] == patient_id
    assert created_patient["identifier"] == [{"system": "urn:lis:mrn", "value": "MRN-1"}]

    # Stored references are permanent addresses, not bundle fullUrls.
    for index in (1, 2):
        stored = read(client, entry_location(response, index))
        references = list(_references(stored))
        assert references
        for node in references:
            assert node["reference"].startswith(BASE + "/"), stored
    assert read(client, entry_location(response, 1))["subject"]["reference"] == patient_perm
    observation_stored = read(client, entry_location(response, 2))
    assert observation_stored["subject"]["reference"] == patient_perm
    assert observation_stored["specimen"]["reference"] == entry_location(response, 1)


def test_docs_and_openapi(client):
    assert client.get("/docs").status_code == 200
    assert client.get("/redoc").status_code == 200
    spec = client.get("/openapi.json").json()
    assert "/" in spec["paths"]
    assert "/{resource_type}/{resource_id}" in spec["paths"]
    assert "application/fhir+json" in spec["paths"]["/"]["post"]["requestBody"]["content"]


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "resources": 0}
    post(client, bundle("health-1", patient("urn:uuid:p", "MRN-H")))
    assert client.get("/health").json() == {"status": "ok", "resources": 1}


# --------------------------------------------------------------------------- references


def test_forward_references_across_entries(client):
    response = post(
        client,
        bundle(
            "forward-1",
            observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:s"),
            specimen("urn:uuid:s", "urn:uuid:p"),
            patient("urn:uuid:p", "MRN-FWD"),
        ),
    )
    assert response.status_code == 200, response.text
    observation_perm = entry_location(response, 0)
    patient_perm = entry_location(response, 2)
    stored = read(client, observation_perm)
    assert stored["subject"]["reference"] == patient_perm
    assert stored["specimen"]["reference"] == entry_location(response, 1)


def test_references_to_library_resources(client):
    first = post(
        client,
        bundle(
            "lib-1",
            patient("urn:uuid:p", "MRN-LIB"),
            specimen("urn:uuid:s", "urn:uuid:p"),
        ),
    )
    assert first.status_code == 200, first.text
    patient_perm = entry_location(first, 0)
    specimen_perm = entry_location(first, 1)

    # A later bundle references both resources by their permanent addresses.
    second = post(
        client,
        bundle("lib-2", observation("urn:uuid:o", patient_perm, specimen_perm)),
    )
    assert second.status_code == 200, second.text
    stored = read(client, entry_location(second, 0))
    assert stored["subject"]["reference"] == patient_perm
    assert stored["specimen"]["reference"] == specimen_perm


def test_dangling_reference_rolls_back_entire_bundle(client):
    response = post(
        client,
        bundle(
            "dangling-1",
            patient("urn:uuid:p", "MRN-D"),
            specimen("urn:uuid:s", "urn:uuid:p"),
            observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:missing"),
        ),
    )
    assert response.status_code == 422
    assert response.json()["issue"][0]["code"] == "not-found"
    # Nothing from the rejected bundle is visible.
    assert client.get("/health").json()["resources"] == 0
    # And the Bundle.id was not recorded: a corrected retry can use the same id.
    fixed = post(
        client,
        bundle(
            "dangling-1",
            patient("urn:uuid:p", "MRN-D"),
            specimen("urn:uuid:s", "urn:uuid:p"),
            observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:s"),
        ),
    )
    assert fixed.status_code == 200, fixed.text


def test_dangling_permanent_address_is_rejected(client):
    response = post(
        client,
        bundle(
            "dangling-2",
            observation(
                "urn:uuid:o",
                BASE + "/Patient/does-not-exist",
                BASE + "/Specimen/nope",
            ),
        ),
    )
    assert response.status_code == 422
    assert response.json()["issue"][0]["code"] == "not-found"
    assert client.get("/health").json()["resources"] == 0


def test_foreign_absolute_reference_is_rejected(client):
    response = post(
        client,
        bundle(
            "foreign-1",
            patient("urn:uuid:p", "MRN-F"),
            specimen("urn:uuid:s", "https://other.example.org/Patient/1"),
        ),
    )
    assert response.status_code == 422


def test_ambiguous_full_url_colliding_with_library_address_is_rejected(client):
    first = post(client, bundle("ambig-1", patient("urn:uuid:p", "MRN-A")))
    assert first.status_code == 200
    patient_perm = entry_location(first, 0)

    second_entry = patient(patient_perm, "MRN-B")
    second_entry["resource"]["id"] = "different-id"
    response = post(client, bundle("ambig-2", second_entry))
    assert response.status_code == 422
    assert "permanent address of an existing resource" in (
        response.json()["issue"][0]["diagnostics"]
    )
    assert client.get("/health").json()["resources"] == 1


def test_expression_pinpoints_the_offending_entry(client):
    response = post(
        client,
        bundle(
            "expr-1",
            patient("urn:uuid:p", "MRN-E"),
            {
                "fullUrl": "urn:uuid:o",
                "resource": {
                    "resourceType": "Observation",
                    "status": "final",
                    "code": {"text": "x"},
                    "subject": {"reference": "urn:uuid:p"},
                },
                "request": {"method": "POST", "url": "Observation"},
            },
        ),
    )
    assert response.status_code == 422
    assert response.json()["issue"][0]["expression"] == [
        "Bundle.entry[1].resource.specimen"
    ]


# --------------------------------------------------------------------------- topology


def test_mismatched_patient_between_observation_and_specimen_is_rejected(client):
    response = post(
        client,
        bundle(
            "mismatch-1",
            patient("urn:uuid:p1", "MRN-X"),
            patient("urn:uuid:p2", "MRN-Y"),
            specimen("urn:uuid:s", "urn:uuid:p1"),
            observation("urn:uuid:o", "urn:uuid:p2", "urn:uuid:s"),
        ),
    )
    assert response.status_code == 422
    assert "same patient" in response.json()["issue"][0]["diagnostics"]
    assert client.get("/health").json()["resources"] == 0


def test_observation_cannot_link_library_specimen_of_another_patient(client):
    first = post(
        client,
        bundle(
            "chain-1",
            patient("urn:uuid:p1", "MRN-CHAIN-1"),
            specimen("urn:uuid:s1", "urn:uuid:p1"),
            patient("urn:uuid:p2", "MRN-CHAIN-2"),
        ),
    )
    assert first.status_code == 200, first.text
    other_patient = entry_location(first, 2)
    library_specimen = entry_location(first, 1)
    response = post(
        client,
        bundle("chain-2", observation("urn:uuid:o", other_patient, library_specimen)),
    )
    assert response.status_code == 422
    assert "same patient" in response.json()["issue"][0]["diagnostics"]


def test_observation_can_link_library_specimen_of_same_patient(client):
    first = post(
        client,
        bundle(
            "same-1",
            patient("urn:uuid:p", "MRN-SAME"),
            specimen("urn:uuid:s", "urn:uuid:p"),
        ),
    )
    assert first.status_code == 200
    second = post(
        client,
        bundle(
            "same-2",
            observation("urn:uuid:o", entry_location(first, 0), entry_location(first, 1)),
        ),
    )
    assert second.status_code == 200, second.text


def test_specimen_requires_a_patient_subject(client):
    response = post(
        client,
        bundle("specimen-kind-1", specimen("urn:uuid:s", "urn:uuid:not-a-patient")),
    )
    assert response.status_code == 422
    assert response.json()["issue"][0]["code"] == "not-found"


def test_reference_to_wrong_type_is_rejected(client):
    response = post(
        client,
        bundle(
            "wrongtype-1",
            patient("urn:uuid:p", "MRN-WT"),
            specimen("urn:uuid:s", "urn:uuid:p"),
            observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:p"),
        ),
    )
    assert response.status_code == 422
    assert "Specimen is required" in response.json()["issue"][0]["diagnostics"]
    assert client.get("/health").json()["resources"] == 0


# --------------------------------------------------------------------------- identity


def test_patient_reused_by_single_identifier_without_rewriting(client):
    first = post(
        client,
        bundle("reuse-1", patient("urn:uuid:p", "MRN-42", name=[{"family": "Original"}])),
    )
    assert first.status_code == 200
    original_location = entry_location(first, 0)

    second = post(
        client,
        bundle(
            "reuse-2",
            patient(
                "urn:uuid:other-full-url",
                "MRN-42",
                name=[{"family": "Changed"}],
                gender="male",
            ),
        ),
    )
    assert second.status_code == 200, second.text
    assert second.json()["entry"][0]["response"]["status"] == "200 OK"
    assert entry_location(second, 0) == original_location
    stored = read(client, original_location)
    assert stored["name"] == [{"family": "Original"}]
    assert "gender" not in stored
    assert client.get("/health").json()["resources"] == 1


def test_patient_reused_when_any_identifier_matches(client):
    first = post(client, bundle("ident-1", patient("urn:uuid:p", "MRN-A")))
    assert first.status_code == 200

    # The second identifier matches the first patient; the first is new. The
    # existing patient is reused and does not gain identifiers.
    entry = patient("urn:uuid:q", "MRN-B")
    entry["resource"]["identifier"].append({"system": "urn:lis:mrn", "value": "MRN-A"})
    second = post(client, bundle("ident-2", entry))
    assert second.status_code == 200, second.text
    assert second.json()["entry"][0]["response"]["status"] == "200 OK"
    assert entry_location(second, 0) == entry_location(first, 0)
    stored = read(client, entry_location(first, 0))
    assert stored["identifier"] == [{"system": "urn:lis:mrn", "value": "MRN-A"}]


def test_same_identity_merges_full_urls_within_bundle(client):
    response = post(
        client,
        bundle(
            "merge-1",
            patient("urn:uuid:a", "MRN-MERGE"),
            patient("urn:uuid:b", "MRN-MERGE", name=[{"family": "Ignored"}]),
            specimen("urn:uuid:s", "urn:uuid:b"),
            observation("urn:uuid:o", "urn:uuid:a", "urn:uuid:s"),
        ),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["entry"][0]["response"]["status"] == "201 Created"
    assert body["entry"][1]["response"]["status"] == "200 OK"
    assert body["entry"][0]["response"]["location"] == body["entry"][1]["response"]["location"]
    # 1 patient + 1 specimen + 1 observation: the second Patient is a merge.
    assert client.get("/health").json()["resources"] == 3
    # First occurrence wins; the merged copy does not rewrite the patient.
    assert read(client, body["entry"][0]["response"]["location"]).get("name") is None


def test_chain_of_merged_identities_stays_consistent(client):
    # b merges onto a, d merges onto c. References to *either* fullUrl of a
    # merged pair must compare as the same patient for the topology checks.
    response = post(
        client,
        bundle(
            "merge-chain",
            patient("urn:uuid:a", "ID-A"),
            patient("urn:uuid:b", "ID-A"),
            patient("urn:uuid:c", "ID-C"),
            patient("urn:uuid:d", "ID-C"),
            specimen("urn:uuid:s1", "urn:uuid:b"),
            observation("urn:uuid:o1", "urn:uuid:a", "urn:uuid:s1"),
            specimen("urn:uuid:s2", "urn:uuid:c"),
            observation("urn:uuid:o2", "urn:uuid:d", "urn:uuid:s2"),
        ),
    )
    assert response.status_code == 200, response.text
    locations = [e["response"]["location"] for e in response.json()["entry"]]
    assert locations[0] == locations[1]
    assert locations[2] == locations[3]
    assert locations[0] != locations[2]
    assert read(client, locations[5])["subject"]["reference"] == locations[0]
    assert read(client, locations[7])["subject"]["reference"] == locations[2]
    # 2 patients + 2 specimens + 2 observations; merged entries add nothing.
    assert client.get("/health").json()["resources"] == 6


def test_merged_identity_does_not_rewrite_the_stored_patient(client):
    # B carries an extra identifier; merging must not append it to A's record.
    sid = "urn:lis:mrn"
    a = patient("urn:uuid:a", "MRN-A")
    b = patient("urn:uuid:b", "MRN-A")
    b["resource"]["identifier"].append({"system": sid, "value": "MRN-EXTRA"})
    response = post(client, bundle("merge-extra", a, b))
    assert response.status_code == 200, response.text
    stored = read(client, entry_location(response, 0))
    assert stored["identifier"] == [{"system": sid, "value": "MRN-A"}]
    # A later bundle using only the never-stored extra identifier creates a new
    # patient, because identifiers are never silently added to an existing one.
    later = post(client, bundle("merge-extra-2", patient("urn:uuid:c", "MRN-EXTRA")))
    assert later.status_code == 200
    assert later.json()["entry"][0]["response"]["status"] == "201 Created"


def test_ambiguous_identity_across_two_existing_patients_is_rejected(client):
    assert post(client, bundle("amb-1", patient("urn:uuid:p", "MRN-1"))).status_code == 200
    assert post(client, bundle("amb-2", patient("urn:uuid:q", "MRN-2"))).status_code == 200

    entry = patient("urn:uuid:x", "MRN-1")
    entry["resource"]["identifier"].append({"system": "urn:lis:mrn", "value": "MRN-2"})
    before = client.get("/health").json()["resources"]
    response = post(client, bundle("amb-3", entry))
    assert response.status_code == 422
    assert response.json()["issue"][0]["code"] == "conflict"
    assert client.get("/health").json()["resources"] == before


def test_patient_without_complete_identifier_is_rejected(client):
    entry = {
        "fullUrl": "urn:uuid:p",
        "resource": {
            "resourceType": "Patient",
            "identifier": [{"system": "urn:lis:mrn"}],
        },
        "request": {"method": "POST", "url": "Patient"},
    }
    response = post(client, bundle("noid-ident", entry))
    assert response.status_code == 422
    assert client.get("/health").json()["resources"] == 0


# --------------------------------------------------------------------------- shapes


@pytest.mark.parametrize(
    "mutate, expected",
    [
        (lambda b: b.update(type="batch"), "transaction"),
        (lambda b: b["entry"][0]["request"].update(method="PUT"), "post"),
        (lambda b: b["entry"][0]["request"].update(method="DELETE"), "post"),
        (lambda b: b["entry"][0]["resource"].update(resourceType="Encounter"), "not supported"),
        (lambda b: b["entry"][0].update(fullUrl=""), "fullurl"),
        (lambda b: b["entry"][0]["resource"].pop("identifier"), "identifier"),
        (lambda b: b["entry"][0].pop("request"), "request"),
        (lambda b: b["entry"][0].pop("resource"), "resource"),
        (lambda b: b["entry"][0]["request"].update(url="Patient/1"), "request.url"),
    ],
)
def test_invalid_bundle_shapes_are_rejected(client, mutate, expected):
    payload = bundle("bad-1", patient("urn:uuid:p", "MRN-BAD"))
    mutate(payload)
    response = post(client, payload)
    assert response.status_code == 422, response.text
    assert expected in json.dumps(response.json()).lower()
    assert client.get("/health").json()["resources"] == 0


def test_more_than_30_entries_is_rejected(client):
    entries = [patient(f"urn:uuid:p{i}", f"MRN-{i}") for i in range(31)]
    response = post(client, bundle("too-many", *entries))
    assert response.status_code == 422
    assert response.json()["issue"][0]["code"] == "too-costly"
    assert client.get("/health").json()["resources"] == 0


def test_exactly_30_entries_is_accepted(client):
    entries = [patient(f"urn:uuid:p{i}", f"MRN-{i}") for i in range(30)]
    response = post(client, bundle("exactly-30", *entries))
    assert response.status_code == 200, response.text
    assert client.get("/health").json()["resources"] == 30


def test_duplicate_full_url_is_rejected(client):
    response = post(
        client,
        bundle("dup-1", patient("urn:uuid:same", "MRN-1"), patient("urn:uuid:same", "MRN-2")),
    )
    assert response.status_code == 422
    assert "fullUrl" in response.json()["issue"][0]["diagnostics"]


def test_existing_resource_id_is_never_overwritten(client):
    fixed = patient("urn:uuid:p", "MRN-ID-1")
    fixed["resource"]["id"] = "fixed"
    assert post(client, bundle("id-1", fixed)).status_code == 200

    entry = patient("urn:uuid:q", "MRN-ID-2")
    entry["resource"]["id"] = "fixed"
    response = post(client, bundle("id-2", entry))
    assert response.status_code == 409

    assert read(client, BASE + "/Patient/fixed")["identifier"] == [
        {"system": "urn:lis:mrn", "value": "MRN-ID-1"}
    ]
    assert client.get("/health").json()["resources"] == 1


def test_supplied_ids_are_used(client):
    entry = patient("urn:uuid:p", "MRN-ID")
    entry["resource"]["id"] = "patient-abc"
    response = post(client, bundle("ids-1", entry))
    assert response.status_code == 200
    assert entry_location(response, 0) == BASE + "/Patient/patient-abc"
    assert read(client, BASE + "/Patient/patient-abc")["id"] == "patient-abc"


def test_invalid_supplied_id_is_rejected(client):
    entry = patient("urn:uuid:p", "MRN-ID")
    entry["resource"]["id"] = "../etc/passwd"
    response = post(client, bundle("ids-2", entry))
    assert response.status_code == 422


def test_bundle_without_id_is_accepted(client):
    response = post(client, bundle(None, patient("urn:uuid:p", "MRN-NOID")))
    assert response.status_code == 200
    assert entry_location(response, 0).startswith(BASE + "/Patient/")


def test_unsupported_content_type(client):
    response = client.post("/", content=b"{}", headers={"content-type": "text/plain"})
    assert response.status_code == 415


def test_plain_json_content_type_is_accepted(client):
    payload = bundle("plain-json", patient("urn:uuid:p", "MRN-PLAIN"))
    response = client.post(
        "/", content=json.dumps(payload), headers={"content-type": "application/json"}
    )
    assert response.status_code == 200


@pytest.mark.parametrize("body", [b"{", b"", b"[]", b"null", b'"x"'])
def test_malformed_bodies(client, body):
    response = client.post("/", content=body, headers=FHIR_HEADERS)
    assert response.status_code == 400, response.text
    assert response.json()["resourceType"] == "OperationOutcome"


def test_oversized_body_is_rejected(client, monkeypatch):
    from app import settings

    monkeypatch.setattr(settings, "MAX_BODY_BYTES", 10)
    response = post(client, bundle("big-1", patient("urn:uuid:p", "MRN-BIG")))
    assert response.status_code == 413


def test_read_unknown_resource(client):
    assert client.get("/Patient/nope").status_code == 404
    assert client.get("/Encounter/nope").status_code == 404
    assert client.get("/Patient/nope").json()["resourceType"] == "OperationOutcome"


# --------------------------------------------------------------------------- idempotency


def test_same_bundle_retry_replays_original_mapping(client):
    payload = bundle(
        "retry-1",
        patient("urn:uuid:p", "MRN-R"),
        specimen("urn:uuid:s", "urn:uuid:p"),
    )
    first = post(client, payload)
    assert first.status_code == 200
    second = post(client, payload)
    assert second.status_code == 200
    assert second.json() == first.json()
    # Byte-identical, including field order: a retry is indistinguishable.
    assert second.content == first.content
    assert client.get("/health").json()["resources"] == 2


def test_same_bundle_id_with_different_body_conflicts(client):
    first = post(client, bundle("retry-2", patient("urn:uuid:p", "MRN-R2")))
    assert first.status_code == 200
    second = post(client, bundle("retry-2", patient("urn:uuid:p", "MRN-OTHER")))
    assert second.status_code == 409
    assert second.json()["issue"][0]["code"] == "conflict"
    assert client.get("/health").json()["resources"] == 1


def test_idempotency_survives_restart(db_path):
    payload = bundle(
        "restart-1",
        patient("urn:uuid:p", "MRN-RESTART"),
        specimen("urn:uuid:s", "urn:uuid:p"),
    )
    with TestClient(create_app(Store(db_path))) as first:
        original = post(first, payload)
        assert original.status_code == 200

    with TestClient(create_app(Store(db_path))) as restarted:
        replay = post(restarted, payload)
        assert replay.status_code == 200
        assert replay.json() == original.json()
        assert replay.content == original.content
        assert restarted.get("/health").json()["resources"] == 2
        stored = read(restarted, entry_location(original, 1))
        assert stored["subject"]["reference"] == entry_location(original, 0)

        conflict = post(restarted, bundle("restart-1", patient("urn:uuid:p", "OTHER")))
        assert conflict.status_code == 409


def test_bundle_without_id_is_not_deduplicated(client):
    payload = bundle(None, patient("urn:uuid:p", "MRN-NID"))
    assert post(client, payload).status_code == 200
    second = post(client, payload)
    assert second.status_code == 200
    # Same identifier, so the second POST maps onto the first patient.
    assert second.json()["entry"][0]["response"]["status"] == "200 OK"
    assert client.get("/health").json()["resources"] == 1


# --------------------------------------------------------------------------- concurrency


def _concurrent_ingest(db_file: str, bundle_id: str, mrn: str, start, results):
    store = Store(db_file)
    payload = bundle(
        bundle_id,
        patient("urn:uuid:p", mrn),
        specimen("urn:uuid:s", "urn:uuid:p"),
        observation("urn:uuid:o", "urn:uuid:p", "urn:uuid:s"),
    )
    start.wait()
    try:
        results.put((bundle_id, "ok", ingest(store, payload)))
    except BundleError as exc:  # pragma: no cover - unexpected in these tests
        results.put((bundle_id, "error", exc.message))
    except Exception as exc:  # noqa: BLE001 - report any crash back to the test
        results.put((bundle_id, "crash", repr(exc)))


def _run_processes(db_path, bundle_ids, mrn):
    ctx = mp.get_context("fork")
    start = ctx.Barrier(len(bundle_ids))
    results = ctx.Queue()
    processes = [
        ctx.Process(target=_concurrent_ingest, args=(str(db_path), bid, mrn, start, results))
        for bid in bundle_ids
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=60)
        assert process.exitcode == 0
    return [results.get(timeout=5) for _ in processes]


def test_concurrent_patients_are_not_duplicated(db_path):
    worker_count = 8
    received = _run_processes(db_path, [f"conc-{i}" for i in range(worker_count)],
                              "MRN-CONCURRENT")
    assert all(kind == "ok" for _bid, kind, _payload in received), received

    store = Store(db_path)
    conn = store.raw_connection()
    try:
        counts = {
            row["resource_type"]: row["n"]
            for row in conn.execute(
                "SELECT resource_type, COUNT(*) AS n FROM resources GROUP BY resource_type"
            )
        }
        patient_ids = [
            row["id"]
            for row in conn.execute(
                "SELECT id FROM resources WHERE resource_type='Patient'"
            )
        ]
        specimen_rows = conn.execute(
            "SELECT id, content FROM resources WHERE resource_type='Specimen'"
        ).fetchall()
        observation_rows = conn.execute(
            "SELECT id, content FROM resources WHERE resource_type='Observation'"
        ).fetchall()
    finally:
        conn.close()

    assert counts["Patient"] == 1
    assert counts["Specimen"] == worker_count
    assert counts["Observation"] == worker_count

    patient_perm = BASE + "/Patient/" + patient_ids[0]
    specimen_by_perm = {}
    for row in specimen_rows:
        content = json.loads(row["content"])
        assert content["subject"]["reference"] == patient_perm
        specimen_by_perm[BASE + "/Specimen/" + row["id"]] = content
    for row in observation_rows:
        content = json.loads(row["content"])
        # Bound to the single patient and to its own specimen, not another's.
        assert content["subject"]["reference"] == patient_perm
        assert content["specimen"]["reference"] in specimen_by_perm
        assert specimen_by_perm[content["specimen"]["reference"]]["subject"][
            "reference"
        ] == patient_perm


def test_concurrent_same_bundle_id_creates_one_copy(db_path):
    received = _run_processes(db_path, ["same-id"] * 6, "MRN-SAME")
    assert all(kind == "ok" for _bid, kind, _payload in received), received
    responses = {json.dumps(payload, sort_keys=True) for _b, _k, payload in received}
    assert len(responses) == 1  # every caller observes the same mapping
    assert Store(db_path).count_resources() == 3


def test_concurrent_identical_body_under_different_ids_uses_one_patient(db_path):
    received = _run_processes(db_path, [f"diff-id-{i}" for i in range(6)], "MRN-DIFF")
    assert all(kind == "ok" for _bid, kind, _payload in received), received
    store = Store(db_path)
    conn = store.raw_connection()
    try:
        patients = conn.execute(
            "SELECT COUNT(*) FROM resources WHERE resource_type='Patient'"
        ).fetchone()[0]
        specimens = conn.execute(
            "SELECT COUNT(*) FROM resources WHERE resource_type='Specimen'"
        ).fetchone()[0]
    finally:
        conn.close()
    assert patients == 1
    assert specimens == 6


def test_concurrent_in_process_requests_do_not_duplicate(db_path):
    app = create_app(Store(db_path))
    errors: list[str] = []

    def worker(index: int) -> None:
        with TestClient(app) as local:
            response = post(
                local,
                bundle(
                    f"thread-{index}",
                    patient("urn:uuid:p", "MRN-THREAD"),
                    specimen("urn:uuid:s", "urn:uuid:p"),
                ),
            )
            if response.status_code != 200:
                errors.append(response.text)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not errors, errors
    assert Store(db_path).count_resources() == 11  # 1 patient + 10 specimens
