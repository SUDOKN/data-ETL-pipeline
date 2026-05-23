import json
from pathlib import Path

from data_etl_app.services.validation.rdf_validation_service import (
    validate_rdf_content,
)


def main() -> int:
    ontology_file_path = Path("ontology/SUDOKN1_1.rdf")

    print(f"Ontology file path: {ontology_file_path}")
    with open(ontology_file_path, "r", encoding="utf-8") as file:
        validation_result = validate_rdf_content(file.read())

    print(json.dumps(validation_result, indent=2, default=str))
    if not validation_result["valid"]:
        raise ValueError("RDF validation failed")

    print("RDF validation completed successfully.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Error during validation: {exc}")
        raise SystemExit(1)
