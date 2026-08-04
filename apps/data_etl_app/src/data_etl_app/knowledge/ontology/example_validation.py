#!/usr/bin/env python3
"""
Example usage of the ontology RDF validation functionality.

This script demonstrates how to use the validation functions to check
an ontology RDF file for compliance with the specified requirements.

Usage:
    python example_validation.py [ontology_file.rdf]

If no file is provided, the script will run example validations.
"""

import sys
import asyncio
import argparse
from pathlib import Path

# Add the src directories to the path so we can import our modules
base_path = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(base_path / "data_etl_app" / "src"))
sys.path.insert(0, str(base_path / "core" / "src"))
sys.path.insert(0, str(base_path / "scraper_app" / "src"))
sys.path.insert(0, str(base_path / "open_ai_key_app" / "src"))

# Load environment variables using the proper dependency loaders
from packages.core.src.core.dependencies.load_core_env import load_core_env
from apps.data_etl_app.src.data_etl_app.dependencies.load_scraper_env import (
    load_scraper_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_open_ai_app_env import (
    load_open_ai_app_env,
)
from apps.data_etl_app.src.data_etl_app.dependencies.load_data_etl_env import (
    load_data_etl_env,
)

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

# Import AWS client initialization
from core.dependencies.aws_clients import initialize_core_aws_clients
from apps.data_etl_app.src.data_etl_app.dependencies.aws_clients import (
    initialize_data_etl_aws_clients,
)

from apps.data_etl_app.src.data_etl_app.knowledge.ontology.validate_ontology_rdf import (
    validate_ontology_rdf,
    validate_ontology_from_file,
    is_ontology_valid,
)
from packages.core.src.core.services.knowledge.ontology_service import (
    get_ontology_service,
)


async def example_validate_current_ontology():
    """Example: Validate the currently loaded ontology from the service."""
    print("=" * 60)
    print("Validating current ontology from OntologyService")
    print("=" * 60)

    try:
        # Get the ontology service (this will load the current ontology)
        service = await get_ontology_service()

        # We need to get the raw RDF content - this would require adding a method
        # to the service or loading it directly from S3
        print("Note: To validate current ontology, you would need to:")
        print("1. Add a method to OntologyService to return raw RDF content")
        print("2. Or load the RDF content directly from S3")
        print("3. Then call validate_ontology_rdf(rdf_content)")

        service_info = service.get_service_info()
        print(f"Current service info: {service_info}")

    except Exception as e:
        print(f"Error accessing ontology service: {e}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate ontology RDF files for compliance with SUDOKN requirements.",
        epilog="If no file is provided, example validations will be demonstrated.",
    )
    parser.add_argument(
        "ontology_file", nargs="?", help="Path to the ontology RDF file to validate"
    )
    parser.add_argument(
        "--examples",
        action="store_true",
        help="Run example validations even when a file is provided",
    )
    return parser.parse_args()


def validate_from_file(file_path: str):
    """Validate ontology from a local RDF file."""
    print("=" * 60)
    print(f"VALIDATING ONTOLOGY FILE: {file_path}")
    print("=" * 60)

    if not Path(file_path).exists():
        print(f"❌ ERROR: File not found: {file_path}")
        return False

    try:
        # Validate the ontology
        result = validate_ontology_from_file(file_path)

        # Print results
        summary = result.get_summary()
        status_icon = "✅" if summary["is_valid"] else "❌"
        print(
            f"{status_icon} Validation Status: {'PASSED' if summary['is_valid'] else 'FAILED'}"
        )
        print(f"📊 Errors: {summary['error_count']}")
        print(f"⚠️  Warnings: {summary['warning_count']}")

        if summary["errors"]:
            print("\n🚨 ERRORS:")
            for i, error in enumerate(summary["errors"], 1):
                print(f"  {i}. {error}")

        if summary["warnings"]:
            print("\n⚠️  WARNINGS:")
            for i, warning in enumerate(summary["warnings"], 1):
                print(f"  {i}. {warning}")

        # Also demonstrate the quick check function
        is_valid = is_ontology_valid(Path(file_path).read_text())
        print(f"\n🔍 Quick validation check: {'VALID' if is_valid else 'INVALID'}")

        return summary["is_valid"]

    except Exception as e:
        print(f"❌ ERROR: Failed to validate file: {e}")
        return False


def example_validate_rdf_string():
    """Example: Validate a simple RDF string (for testing)."""
    print("=" * 60)
    print("Validating simple RDF string example")
    print("=" * 60)

    # This is a minimal example - in practice you'd have a much larger RDF
    sample_rdf = """<?xml version="1.0" encoding="UTF-8"?>
<rdf:RDF
    xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
    xmlns:skos="http://www.w3.org/2004/02/skos/core#">
    
    <rdf:Description rdf:about="http://example.com/TestCapability">
        <rdfs:label>Test Capability</rdfs:label>
        <skos:altLabel>Alternative Test</skos:altLabel>
    </rdf:Description>
    
</rdf:RDF>"""

    result = validate_ontology_rdf(sample_rdf)
    summary = result.get_summary()

    print(f"Validation Status: {'PASSED' if summary['is_valid'] else 'FAILED'}")
    print(f"Errors: {summary['error_count']}")
    print(f"Warnings: {summary['warning_count']}")

    if summary["errors"]:
        print("\nERRORS:")
        for error in summary["errors"]:
            print(f"  - {error}")

    if summary["warnings"]:
        print("\nWARNINGS:")
        for warning in summary["warnings"]:
            print(f"  - {warning}")


async def main():
    """Main function demonstrating various validation approaches."""
    # Initialize AWS clients
    await initialize_core_aws_clients()
    await initialize_data_etl_aws_clients()

    args = parse_args()

    if args.ontology_file:
        # Primary use case: validate a specific file
        print("🔍 SUDOKN Ontology RDF Validator")
        print("=" * 40)

        is_valid = validate_from_file(args.ontology_file)

        if not args.examples:
            # Exit with appropriate code
            sys.exit(0 if is_valid else 1)

    if not args.ontology_file or args.examples:
        # Demonstration mode
        print("\n📚 Ontology RDF Validation Examples")
        print("=" * 40)

        # Example 1: Validate simple RDF string
        example_validate_rdf_string()

        # Example 2: Show how to validate current ontology (requires service setup)
        await example_validate_current_ontology()

        if not args.ontology_file:
            print("\n💡 TIP: To validate a specific file, run:")
            print(f"python {Path(__file__).name} /path/to/your/ontology.rdf")


if __name__ == "__main__":
    asyncio.run(main())
