"""
tests/contract/test_schema_contracts.py
========================================
Contract tests — verify that SDK Pydantic schemas match the server's
OpenAPI specification.

What this tests
---------------
For every schema mapping defined in RESPONSE_CONTRACTS below, we assert:

  1. Every field in the Pydantic model exists in the OpenAPI component.
  2. Every *required* field in the OpenAPI component exists in the Pydantic model.
  3. Field types are compatible (both directions).

This catches two failure modes:
  - SDK adds a field the server doesn't know about (direction 1)
  - Server adds/changes a required field the SDK hasn't modelled (direction 2)

No real HTTP requests are made to the database — only the OpenAPI spec
document is fetched, and even that falls back to a local cache.

Spec source
-----------
Tests try to fetch the spec from the live server. If the server is
unreachable, the locally cached openapi.yaml is used instead. If neither
is available the test session fails immediately with a clear message.

Adding new contracts
--------------------
Add an entry to RESPONSE_CONTRACTS:
    "OpenApiComponentName": schemas.YourPydanticModel

The OpenAPI component name must exactly match the key under
`components.schemas` in the spec.
"""

import datetime
import logging
import re
from pathlib import Path
from typing import Any, Union, get_args, get_origin

import pytest
import requests
import yaml

import achillesdb.schemas as schemas

logger = logging.getLogger(__name__)

HOST = "localhost"
PORT = 8180
OPENAPI_URL = f"http://{HOST}:{PORT}/api/v1/openapi.yaml"
CACHE_FILE = Path(__file__).parent.parent / "openapi.yaml"


# ─────────────────────────────────────────────────────────────────────────────
# Schema contract registry
#
# Maps OpenAPI component name → Pydantic model.
# Only include schemas that have a direct 1-to-1 correspondence.
# Nested sub-schemas (e.g. CollectionCatalogEntry inside GetCollectionRes)
# are tested separately via their own entry.
# ─────────────────────────────────────────────────────────────────────────────

RESPONSE_CONTRACTS: dict[str, type] = {
    # Shared
    "MessageResponse":          schemas.MessageResponse,
    "ErrorResponse":            schemas.ErrorResponse,

    # Database
    "DatabaseInfo":             schemas.DatabaseInfo,
    "ListDatabasesResponse":    schemas.GetDatabasesRes,

    # Collection
    "CollectionCatalogEntry":   schemas.CollectionCatalogEntry,
    "CollectionStats":          schemas.CollectionStats,
    "CollectionEntry":          schemas.GetCollectionRes,

    # Documents — responses
    "Document":                 schemas.Document,
    "QueryResponse":            schemas.QueryRes,

    # Documents — request payloads (that use components)
}

# ─────────────────────────────────────────────────────────────────────────────
# Path/Method contract registry
#
# Maps (Method, Path) → Pydantic model for request bodies.
# These schemas are defined inline under the path's requestBody.
# ─────────────────────────────────────────────────────────────────────────────

REQUEST_CONTRACTS: dict[tuple[str, str], type] = {
    ("post", "/database"): schemas.CreateDatabaseReq,
    ("post", "/database/{database_name}/collections"): schemas.CreateCollectionReqInput,
    ("post", "/database/{database_name}/collections/{collection_name}/documents"): schemas.InsertDocumentReqInput,
    ("put", "/database/{database_name}/collections/{collection_name}/documents"): schemas.UpdateDocumentsReqInput,
    ("delete", "/database/{database_name}/collections/{collection_name}/documents"): schemas.DeleteDocumentsReqInput,
    ("post", "/database/{database_name}/collections/{collection_name}/documents/query"): schemas.QueryReqInput,
}

# ─────────────────────────────────────────────────────────────────────────────
# OpenAPI spec fixture
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def openapi_spec() -> dict:
    """
    Fetch the OpenAPI spec from the live server, falling back to the
    cached openapi.yaml if the server is unreachable.
    """
    spec_text = None

    try:
        response = requests.get(OPENAPI_URL, timeout=3)
        response.raise_for_status()
        spec_text = response.text
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        CACHE_FILE.write_text(spec_text)
        logger.info("Fetched OpenAPI spec from %s and updated cache.", OPENAPI_URL)
    except Exception as exc:
        logger.warning("Could not fetch OpenAPI spec from %s: %s", OPENAPI_URL, exc)
        if CACHE_FILE.exists():
            spec_text = CACHE_FILE.read_text()
            logger.info("Using cached openapi.yaml at %s.", CACHE_FILE)
        else:
            pytest.fail(
                f"Cannot reach server at {OPENAPI_URL} and no local cache exists at {CACHE_FILE}. "
                "Run with a live server at least once to create the cache."
            )

    spec = yaml.safe_load(spec_text)
    assert spec, "OpenAPI spec loaded but is empty."
    return spec


# ─────────────────────────────────────────────────────────────────────────────
# Type compatibility helpers
# ─────────────────────────────────────────────────────────────────────────────

import types

def _unwrap_optional(python_type: Any) -> Any:
    """Strip Optional (Union[X, None]) down to X."""
    origin = get_origin(python_type)
    args = get_args(python_type)
    
    # Handle typing.Union and Python 3.10+ types.UnionType
    is_union = origin is Union or origin is getattr(types, "UnionType", None)
    
    if is_union and type(None) in args:
        non_none = [t for t in args if t is not type(None)]
        return non_none[0] if len(non_none) == 1 else Union[tuple(non_none)]
    return python_type


def _openapi_type_matches(openapi_type: str, python_type: Any) -> bool:
    """
    Return True if the OpenAPI primitive type is compatible with the
    Python type annotation.

    Handles Optional unwrapping and list/dict origins.
    """
    python_type = _unwrap_optional(python_type)
    origin = get_origin(python_type)
    args = get_args(python_type)

    if openapi_type == "string":
        return python_type in (str, datetime.datetime)

    if openapi_type == "integer":
        return python_type is int

    if openapi_type == "boolean":
        return python_type is bool

    if openapi_type == "number":
        return python_type in (float, int)

    if openapi_type == "array":
        return origin is list

    if openapi_type == "object":
        return (
            origin is dict
            or python_type is dict
            or python_type is Any
            or hasattr(python_type, "model_fields")  # Pydantic model
            or type(python_type).__name__ == "ForwardRef" # Handles forward refs to other models
        )

    return False


# ─────────────────────────────────────────────────────────────────────────────
# Core validation logic
# ─────────────────────────────────────────────────────────────────────────────

def _resolve_ref(ref: str, spec: dict) -> dict:
    """
    Resolve a $ref like '#/components/schemas/Foo' to its schema dict.
    """
    parts = ref.lstrip("#/").split("/")
    node = spec
    for part in parts:
        node = node[part]
    return node


def _get_openapi_schema(component_name: str, spec: dict) -> dict:
    """
    Look up a component schema by name, resolving $ref if needed.
    Fails the test clearly if the component doesn't exist.
    """
    components = spec.get("components", {}).get("schemas", {})
    if component_name not in components:
        available = sorted(components.keys())
        pytest.fail(
            f"OpenAPI component '{component_name}' not found in spec.\n"
            f"Available components ({len(available)}): {available}"
        )
    schema = components[component_name]
    if "$ref" in schema:
        schema = _resolve_ref(schema["$ref"], spec)
    return schema


def _get_request_schema(method: str, path: str, spec: dict) -> dict | None:
    """
    Look up an inline request body schema by path and method.
    Fails if the path or method does not exist, or if there is no requestBody.
    """
    paths = spec.get("paths", {})
    if path not in paths:
        pytest.fail(f"OpenAPI path '{path}' not found in spec.")

    path_item = paths[path]
    if method not in path_item:
        pytest.fail(f"HTTP method '{method}' not found for path '{path}'.")

    operation = path_item[method]
    request_body = operation.get("requestBody", {})
    
    if not request_body:
        return None  # Some endpoints don't have a payload, which is fine

    content = request_body.get("content", {})
    json_content = content.get("application/json", {})
    
    schema = json_content.get("schema", {})
    if "$ref" in schema:
        schema = _resolve_ref(schema["$ref"], spec)
        
    return schema


def validate_contract(
    component_name: str,
    pydantic_model: type,
    spec: dict,
    *,
    schema_dict: dict | None = None,
    check_required_in_openapi: bool = True,
) -> None:
    """
    Bidirectional contract check between a Pydantic model and an OpenAPI schema.

    Direction 1 (SDK → spec): every field defined in the Pydantic model must
    exist in the OpenAPI component.

    Direction 2 (spec → SDK): every field marked as required in the OpenAPI
    component must exist in the Pydantic model.
    """
    if pydantic_model.__name__ == "Dummy":
        pytest.skip(f"Skipping {component_name} due to missing SDK schema")

    # If schema mapping wasn't directly provided, look it up via component name 
    openapi_schema = schema_dict if schema_dict is not None else _get_openapi_schema(component_name, spec)
    openapi_props: dict = openapi_schema.get("properties", {})
    openapi_required: list[str] = openapi_schema.get("required", [])

    pydantic_fields = pydantic_model.model_fields
    failures: list[str] = []

    # ── Direction 1: Pydantic → OpenAPI ──────────────────────────────────────
    for field_name, field_info in pydantic_fields.items():
        # Use alias if defined (e.g. _id for CollectionCatalogEntry.id)
        wire_name = field_info.alias or field_name

        if wire_name not in openapi_props:
            # Optional fields missing from OpenAPI are a warning, not a failure
            if not field_info.is_required():
                logger.warning(
                    "[%s] Optional field '%s' is in Pydantic but absent from OpenAPI spec.",
                    component_name, wire_name,
                )
                continue
            failures.append(
                f"  PYDANTIC→SPEC: Required field '{wire_name}' is in "
                f"{pydantic_model.__name__} but missing from OpenAPI '{component_name}'"
            )
            continue

        # Type check (skip if OpenAPI gives no type — e.g. $ref fields)
        openapi_type = openapi_props[wire_name].get("type")
        if openapi_type and field_info.annotation is not None:
            if not _openapi_type_matches(openapi_type, field_info.annotation):
                failures.append(
                    f"  TYPE MISMATCH: '{wire_name}' in {component_name} — "
                    f"OpenAPI type '{openapi_type}' vs Python type '{field_info.annotation}'"
                )

    # ── Direction 2: OpenAPI → Pydantic ──────────────────────────────────────
    if check_required_in_openapi:
        pydantic_wire_names = {
            (info.alias or name)
            for name, info in pydantic_fields.items()
        }
        for required_field in openapi_required:
            if required_field not in pydantic_wire_names:
                failures.append(
                    f"  SPEC→PYDANTIC: OpenAPI marks '{required_field}' as required in "
                    f"'{component_name}' but it is absent from {pydantic_model.__name__}"
                )

    if failures:
        failure_text = "\n".join(failures)
        pytest.fail(
            f"Contract violations for '{component_name}' ↔ "
            f"{pydantic_model.__name__}:\n{failure_text}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Parametrised contract tests
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "component_name,pydantic_model",
    list(RESPONSE_CONTRACTS.items()),
    ids=list(RESPONSE_CONTRACTS.keys()),
)
def test_response_schema_contract(component_name, pydantic_model, openapi_spec):
    """
    One test per schema mapping. Each test ID is the OpenAPI component name,
    making failures immediately obvious in the test output:

        FAILED test_schema_contracts.py::test_response_schema_contract[Document]
        FAILED test_schema_contracts.py::test_response_schema_contract[QueryRes]
    """
    validate_contract(component_name, pydantic_model, openapi_spec)


@pytest.mark.parametrize(
    "route,pydantic_model",
    list(REQUEST_CONTRACTS.items()),
    ids=[f"{r[0].upper()} {r[1]}" for r in REQUEST_CONTRACTS.keys()],
)
def test_request_schema_contract(route, pydantic_model, openapi_spec):
    """
    Tests inline request payloads retrieved via path and method.
    """
    method, path = route
    schema_dict = _get_request_schema(method, path, openapi_spec)

    if not schema_dict:
        pytest.fail(f"Expected requestBody schema for {method.upper()} {path} but none was found.")
        
    component_name = f"{method.upper()} {path} Request"
    validate_contract(component_name, pydantic_model, openapi_spec, schema_dict=schema_dict)


# ─────────────────────────────────────────────────────────────────────────────
# Endpoint coverage test
# ─────────────────────────────────────────────────────────────────────────────

def test_all_endpoints_have_response_schema_coverage(openapi_spec):
    """
    Verify that every response schema referenced in the OpenAPI spec's paths
    has a corresponding entry in RESPONSE_CONTRACTS.

    This test catches new endpoints added to the server that the SDK hasn't
    modelled yet. It does not fail on request body schemas — only responses.
    """
    paths = openapi_spec.get("paths", {})
    uncovered: list[str] = []

    ref_pattern = re.compile(r"#/components/schemas/(\w+)")

    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method not in ("get", "post", "put", "patch", "delete"):
                continue

            responses = operation.get("responses", {})
            for status_code, response_obj in responses.items():
                # Only check success responses
                if not str(status_code).startswith("2"):
                    continue

                content = response_obj.get("content", {})
                json_content = content.get("application/json", {})
                schema_obj = json_content.get("schema", {})

                # Find all $ref in the response schema
                schema_str = str(schema_obj)
                refs = ref_pattern.findall(schema_str)

                for ref_name in refs:
                    if ref_name not in RESPONSE_CONTRACTS:
                        uncovered.append(
                            f"  {method.upper()} {path} [{status_code}] "
                            f"→ '{ref_name}' not in RESPONSE_CONTRACTS"
                        )

    if uncovered:
        uncovered_text = "\n".join(uncovered)
        pytest.fail(
            f"The following response schemas are referenced in the OpenAPI spec "
            f"but have no contract test coverage:\n{uncovered_text}\n\n"
            f"Add them to RESPONSE_CONTRACTS in test_schema_contracts.py."
        )
