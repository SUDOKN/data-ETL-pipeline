from data_etl_app.services.validation.rdf_validation_service import (
    validate_rdf_content,
)


def test_validate_rdf_content_returns_valid_result_for_simple_rdf():
    rdf_text = """<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF
    xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
    xmlns:owl='http://www.w3.org/2002/07/owl#'>
  <owl:Class rdf:about='http://example.com/Thing'>
    <rdfs:label>Thing</rdfs:label>
  </owl:Class>
</rdf:RDF>
"""

    result = validate_rdf_content(rdf_text)

    assert result["valid"] is True
    assert result["total_unique_subjects"] == 1
    assert result["issues"] == []


def test_validate_rdf_content_reports_invalid_uri():
    rdf_text = """<?xml version='1.0' encoding='utf-8'?>
<rdf:RDF
    xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
    xmlns:rdfs='http://www.w3.org/2000/01/rdf-schema#'
    xmlns:owl='http://www.w3.org/2002/07/owl#'>
  <owl:Class rdf:about='urn:invalid'>
    <rdfs:label>Thing</rdfs:label>
  </owl:Class>
</rdf:RDF>
"""

    result = validate_rdf_content(rdf_text)

    assert result["valid"] is False
    assert result["issues"][0]["type"] == "invalid_uri"