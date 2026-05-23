import pytest
from fastapi import HTTPException

from data_etl_app.api.routes.rdf_validation import validate_rdf_file


class FakeUploadFile:
    def __init__(self, payload: bytes):
        self._payload = payload

    async def read(self) -> bytes:
        return self._payload


@pytest.mark.asyncio
async def test_validate_rdf_file_returns_validation_result():
    rdf_text = b"""<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF
    xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
    xmlns:owl='http://www.w3.org/2002/07/owl#'>
  <owl:Class rdf:about='http://example.com/Thing'>
    <rdfs:label>Thing</rdfs:label>
  </owl:Class>
</rdf:RDF>
"""

    result = await validate_rdf_file(FakeUploadFile(rdf_text))

    assert result["valid"] is True
    assert result["validated_concept_roots_count"] == 0
    assert "validated_concept_roots" not in result
    assert result["issues"] == []


@pytest.mark.asyncio
async def test_validate_rdf_file_can_include_full_concept_roots_list():
    rdf_text = b"""<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF
    xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
    xmlns:owl='http://www.w3.org/2002/07/owl#'>
    <owl:Class rdf:about='http://example.com/Thing'>
    <rdfs:label>Thing</rdfs:label>
    </owl:Class>
</rdf:RDF>
"""

    result = await validate_rdf_file(FakeUploadFile(rdf_text), include_concept_roots=True)

    assert result["valid"] is True
    assert result["validated_concept_roots_count"] == 0
    assert result["validated_concept_roots"] == []


@pytest.mark.asyncio
async def test_validate_rdf_file_rejects_non_utf8_payload():
    with pytest.raises(HTTPException) as exc_info:
        await validate_rdf_file(FakeUploadFile(b"\xff\xfe\xfd"))

    assert exc_info.value.status_code == 400