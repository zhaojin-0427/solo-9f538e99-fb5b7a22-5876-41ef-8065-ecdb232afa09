"""HTTP layer: POST / (transaction), GET /<Type>/<id> (permanent read), /docs."""
from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response

from . import settings
from .store import Store
from .transaction import SUPPORTED_TYPES_SET, BundleError, ingest

logger = logging.getLogger("fhir_ingest")

EXAMPLE_BUNDLE = {
    "resourceType": "Bundle",
    "type": "transaction",
    "id": "lis-demo-0001",
    "entry": [
        {
            "fullUrl": "urn:uuid:11111111-1111-4111-8111-111111111111",
            "resource": {
                "resourceType": "Patient",
                "identifier": [
                    {"system": "urn:lis:mrn", "value": "MRN-1001"}
                ],
                "name": [{"family": "Zhang", "given": ["Wei"]}],
                "gender": "female",
                "birthDate": "1985-04-12",
            },
            "request": {"method": "POST", "url": "Patient"},
        },
        {
            "fullUrl": "urn:uuid:22222222-2222-4222-8222-222222222222",
            "resource": {
                "resourceType": "Specimen",
                "status": "available",
                "type": {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0487",
                            "code": "SER",
                            "display": "Serum",
                        }
                    ]
                },
                "subject": {
                    "reference": "urn:uuid:11111111-1111-4111-8111-111111111111"
                },
            },
            "request": {"method": "POST", "url": "Specimen"},
        },
        {
            "fullUrl": "urn:uuid:33333333-3333-4333-8333-333333333333",
            "resource": {
                "resourceType": "Observation",
                "status": "final",
                "code": {
                    "coding": [
                        {
                            "system": "http://loinc.org",
                            "code": "2345-7",
                            "display": "Glucose [Mass/volume] in Serum or Plasma",
                        }
                    ]
                },
                "subject": {
                    "reference": "urn:uuid:11111111-1111-4111-8111-111111111111"
                },
                "specimen": {
                    "reference": "urn:uuid:22222222-2222-4222-8222-222222222222"
                },
                "valueQuantity": {
                    "value": 5.4,
                    "unit": "mmol/L",
                    "system": "http://unitsofmeasure.org",
                    "code": "mmol/L",
                },
            },
            "request": {"method": "POST", "url": "Observation"},
        },
    ],
}


def error_response(status: int, message: str,
                   issues: list[dict[str, Any]] | None = None,
                   headers: dict[str, str] | None = None) -> JSONResponse:
    outcome = {
        "resourceType": "OperationOutcome",
        "issue": issues or [
            {"severity": "error", "code": "processing", "diagnostics": message}
        ],
    }
    return JSONResponse(
        status_code=status, content=outcome, headers=headers,
        media_type="application/fhir+json",
    )


def create_app(store: Store | None = None) -> FastAPI:
    store = store or Store(settings.DB_PATH)

    app = FastAPI(
        title="LIS FHIR R4 transaction ingest",
        version="1.0.0",
        description=(
            "Accept FHIR R4 transaction Bundles (POST-only Patient, Specimen and "
            "Observation; at most 30 entries), persist them atomically into the "
            "research library and expose every stored resource at its permanent "
            "address.\n\n"
            "* **Identity** – a Patient is reused when any complete "
            "`identifier(system, value)` is already known (from the library or an "
            "earlier entry of the same bundle). Reused patients are never rewritten "
            "and all fullUrls with the same identity collapse onto one patient.\n"
            "* **References** – limited to a `fullUrl` inside the bundle (forward "
            "references allowed) or a permanent library address "
            "`<base>/<Type>/<id>`; dangling or ambiguous references reject the whole "
            "bundle. Stored references are rewritten to permanent addresses.\n"
            "* **Atomicity** – one bundle is one SQLite transaction; a rejected "
            "bundle leaves no new resources.\n"
            "* **Idempotency** – an identical body with the same `Bundle.id` replays "
            "the original response; a different body with that `Bundle.id` returns "
            "`409 Conflict`. The mapping survives restarts."
        ),
    )
    app.state.store = store

    @app.exception_handler(BundleError)
    async def _bundle_error(_request: Request, exc: BundleError) -> JSONResponse:
        return error_response(exc.status, exc.message, exc.issues)

    @app.get("/health", tags=["ops"], summary="Liveness and database check")
    def health() -> dict[str, Any]:
        return {"status": "ok", "resources": store.count_resources()}

    @app.post(
        "/",
        tags=["transaction"],
        summary="Submit a FHIR R4 transaction Bundle",
        status_code=200,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "application/fhir+json": {
                        "schema": {"type": "object"},
                        "example": EXAMPLE_BUNDLE,
                    }
                },
            }
        },
        responses={
            200: {"description": "transaction-response Bundle"},
            400: {"description": "Malformed JSON"},
            409: {"description": "Bundle.id conflict"},
            413: {"description": "Body too large"},
            415: {"description": "Unsupported Content-Type"},
            422: {"description": "Bundle rejected; OperationOutcome, nothing written"},
        },
    )
    async def post_bundle(request: Request) -> Response:
        content_type = (
            (request.headers.get("content-type") or "").split(";")[0].strip().lower()
        )
        if content_type not in {"application/fhir+json", "application/json"}:
            return error_response(
                415,
                "Content-Type must be application/fhir+json",
                headers={"Accept-Post": "application/fhir+json"},
            )
        body = await request.body()
        if len(body) > settings.MAX_BODY_BYTES:
            return error_response(413, "Request body is too large")
        try:
            bundle = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return error_response(
                400, "Request body is not valid JSON",
                [{"severity": "error", "code": "invalid",
                  "diagnostics": "Body is not valid JSON"}],
            )
        result = await run_in_threadpool(ingest, store, bundle)
        return JSONResponse(status_code=200, content=result,
                            media_type="application/fhir+json")

    @app.get(
        "/{resource_type}/{resource_id}",
        tags=["read"],
        summary="Read a stored resource at its permanent address",
        responses={
            200: {"description": "The stored resource"},
            404: {"description": "No such resource"},
        },
    )
    def read_resource(resource_type: str, resource_id: str) -> Response:
        if resource_type not in SUPPORTED_TYPES_SET:
            return error_response(
                404, f"Unsupported resource type {resource_type!r}",
                [{"severity": "error", "code": "not-found",
                  "diagnostics": "Resource type is not stored here"}],
            )
        resource = store.get_resource(resource_type, resource_id)
        if resource is None:
            return error_response(
                404, "Resource not found",
                [{"severity": "error", "code": "not-found",
                  "diagnostics": f"{resource_type}/{resource_id} does not exist"}],
            )
        return JSONResponse(status_code=200, content=resource,
                            media_type="application/fhir+json")

    return app
