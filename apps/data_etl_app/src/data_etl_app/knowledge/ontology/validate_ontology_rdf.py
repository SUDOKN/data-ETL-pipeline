"""
Validation utilities for ontology RDF files.

This module provides comprehensive validation for ontology RDF data, including:
- URI format and suffix validation
- Label uniqueness validation
- Recursive validation of concept hierarchies
"""

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple
from urllib.parse import urlparse

import rdflib
from rdflib.term import URIRef
from rdflib.namespace import RDFS

from pure_utils.env_util import load_env

from data_etl_app.dependencies.env import ONTOLOGY_SCRIPT_ENV

load_env(ONTOLOGY_SCRIPT_ENV)

from core.models.skos_concept import ConceptNode
from core.utils.rdf_to_graph_util import (
    get_graph,
    get_label,
    get_alt_labels,
    build_concept_tree,
)
from data_etl_app.utils.ontology_uri_util import (
    process_cap_base_uri,
    material_cap_base_uri,
    industry_base_uri,
    certificate_base_uri,
)

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Custom exception for ontology validation errors."""

    pass


class ValidationResult:
    """Container for validation results and errors."""

    def __init__(self):
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.is_valid: bool = True

    def add_error(self, message: str):
        """Add an error message and mark validation as failed."""
        self.errors.append(message)
        self.is_valid = False
        logger.error(f"Validation error: {message}")

    def add_warning(self, message: str):
        """Add a warning message."""
        self.warnings.append(message)
        logger.warning(f"Validation warning: {message}")

    def get_summary(self) -> Dict:
        """Get a summary of validation results."""
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors,
            "warnings": self.warnings,
        }


def _is_valid_uri(uri: str) -> bool:
    """Check if a URI is valid and not empty."""
    if not uri or not isinstance(uri, str):
        return False

    try:
        parsed = urlparse(uri)
        # Must have scheme and netloc for a valid URI, and scheme must be http or https
        return bool(
            parsed.scheme
            and parsed.netloc
            and parsed.scheme.lower() in ("http", "https")
        )
    except Exception:
        return False


def _validate_uri_suffix(uri: str, resource_type: str) -> bool:
    """
    Validate that URI ends with the correct suffix based on resource type.

    Args:
        uri: The URI to validate
        resource_type: One of 'process', 'material', 'industry', 'certificate'

    Returns:
        True if URI has correct suffix, False otherwise
    """
    suffix_requirements = {
        "process": "Capability",
        "material": "Capability",
        "industry": "Industry",
        "certificate": "Certificate",
    }

    required_suffix = suffix_requirements.get(resource_type)
    if not required_suffix:
        logger.warning(f"Unknown resource type: {resource_type}")
        return False

    return uri.endswith(required_suffix)


def _validate_labels_uniqueness(
    name: str, alt_labels: List[str], seen_labels: Set[str], uri: str
) -> List[str]:
    """
    Validate that name and alternate labels are unique.

    Args:
        name: The primary label
        alt_labels: List of alternate labels
        seen_labels: Set of previously seen labels
        uri: URI for error reporting

    Returns:
        List of validation error messages
    """
    errors = []
    all_labels = [name] + alt_labels

    # Check for duplicates within this concept
    label_counts = defaultdict(int)
    for label in all_labels:
        label_counts[label] += 1
        if label_counts[label] > 1:
            errors.append(f"Duplicate label '{label}' in concept {uri}")

    # Check for conflicts with previously seen labels
    for label in all_labels:
        if label in seen_labels:
            errors.append(
                f"Label '{label}' already used elsewhere, found again in {uri}"
            )
        else:
            seen_labels.add(label)

    return errors


def _validate_concept_node(
    node: ConceptNode,
    uri: str,
    resource_type: str,
    seen_labels: Set[str],
    validation_result: ValidationResult,
) -> None:
    """
    Validate a single concept node.

    Args:
        node: The concept node to validate
        uri: The URI of the concept
        resource_type: Type of resource ('process', 'material', 'industry', 'certificate')
        seen_labels: Set of previously seen labels (modified in-place)
        validation_result: Result container to accumulate errors
    """
    # Validate URI
    if not _is_valid_uri(uri):
        validation_result.add_error(f"Invalid or empty URI: {uri}")
        return

    # Validate URI suffix
    if not _validate_uri_suffix(uri, resource_type):
        suffix_map = {
            "process": "Capability",
            "material": "Capability",
            "industry": "Industry",
            "certificate": "Certificate",
        }
        expected_suffix = suffix_map.get(resource_type, "Unknown")
        validation_result.add_error(
            f"URI {uri} does not end with required suffix '{expected_suffix}' for {resource_type} resource"
        )

    # Validate label uniqueness
    label_errors = _validate_labels_uniqueness(
        node["name"], node["altLabels"], seen_labels, uri
    )
    for error in label_errors:
        validation_result.add_error(error)


def _validate_concept_hierarchy_recursive(
    graph: rdflib.Graph,
    parent_uri: URIRef,
    resource_type: str,
    seen_labels: Set[str],
    validation_result: ValidationResult,
) -> None:
    """
    Recursively validate concept hierarchy starting from parent URI.

    Args:
        graph: RDF graph containing the ontology
        parent_uri: Parent URI to start validation from
        resource_type: Type of resource being validated
        seen_labels: Set of previously seen labels (modified in-place)
        validation_result: Result container to accumulate errors
    """
    # Get all direct subclasses
    for subclass, _, _ in graph.triples((None, RDFS.subClassOf, parent_uri)):
        if not isinstance(subclass, URIRef):
            continue

        uri_str = str(subclass)

        try:
            # Get label and alt labels for this concept
            name = get_label(graph, uri_str)
            alt_labels = get_alt_labels(graph, uri_str)

            # Create concept node for validation
            concept_node: ConceptNode = {
                "name": name,
                "altLabels": alt_labels,
                "children": [],  # We don't need children for validation
            }

            # Validate this concept node
            _validate_concept_node(
                concept_node, uri_str, resource_type, seen_labels, validation_result
            )

            # Recursively validate children
            _validate_concept_hierarchy_recursive(
                graph, subclass, resource_type, seen_labels, validation_result
            )

        except ValueError as e:
            validation_result.add_error(f"Failed to get label for URI {uri_str}: {e}")
        except Exception as e:
            validation_result.add_error(
                f"Unexpected error validating URI {uri_str}: {e}"
            )


def _get_resource_type_from_uri(
    uri: str, base_uris: Dict[str, str | None]
) -> str | None:
    """
    Determine the resource type of a URI based on its relationship to base URIs.

    Args:
        uri: The URI to classify
        base_uris: Dictionary mapping resource types to their base URIs

    Returns:
        Resource type string or None if not classified
    """
    for resource_type, base_uri in base_uris.items():
        if base_uri and uri.startswith(base_uri):
            return resource_type
    return None


def _is_descendant_of_base_uri(
    graph: rdflib.Graph, uri: URIRef, base_uri: URIRef, max_depth: int = 10
) -> bool:
    """
    Check if a URI is a descendant of a base URI through rdfs:subClassOf relationships.

    Args:
        graph: RDF graph
        uri: URI to check
        base_uri: Base URI to check against
        max_depth: Maximum recursion depth to prevent infinite loops

    Returns:
        True if uri is a descendant of base_uri
    """
    if max_depth <= 0:
        return False

    # Check direct parent relationships
    for _, _, parent in graph.triples((uri, RDFS.subClassOf, None)):
        if parent == base_uri:
            return True
        if isinstance(parent, URIRef) and _is_descendant_of_base_uri(
            graph, parent, base_uri, max_depth - 1
        ):
            return True

    return False


def _classify_uri_resource_type(
    graph: rdflib.Graph, uri: str, base_uris: Dict[str, str | None]
) -> str | None:
    """
    Classify a URI based on its hierarchy relationship to base URIs.

    Args:
        graph: RDF graph
        uri: URI to classify
        base_uris: Dictionary mapping resource types to base URIs

    Returns:
        Resource type or None if not classifiable
    """
    uri_ref = URIRef(uri)

    # Check if URI is a descendant of any base URI
    for resource_type, base_uri in base_uris.items():
        if base_uri:
            base_uri_ref = URIRef(base_uri)
            if uri == base_uri or _is_descendant_of_base_uri(
                graph, uri_ref, base_uri_ref
            ):
                return resource_type

    return None


def validate_ontology_rdf(rdf_content: str) -> ValidationResult:
    """
    Validate ontology RDF content comprehensively.

    This function validates:
    1. URI format for all URIs in the RDF
    2. Suffix requirements for classified resource types
    3. Label uniqueness across the entire ontology
    4. Proper concept hierarchy structure

    Args:
        rdf_content: Raw RDF content as string

    Returns:
        ValidationResult containing validation status and any errors/warnings
    """
    result = ValidationResult()
    seen_labels: Set[str] = set()

    try:
        # Parse the RDF graph
        graph = get_graph(rdf_content)
        logger.info("Successfully parsed RDF graph")

        # Define base URIs and their corresponding resource types
        base_uris = {
            "process": process_cap_base_uri(),
            "material": material_cap_base_uri(),
            "industry": industry_base_uri(),
            "certificate": certificate_base_uri(),
        }

        # Get all subjects (URIs) in the RDF graph
        all_subjects = set()
        for subject, _, _ in graph:
            if isinstance(subject, URIRef):
                all_subjects.add(str(subject))

        logger.info(f"Found {len(all_subjects)} unique URIs to validate")

        # Validate each URI in the ontology
        for uri_str in all_subjects:
            try:
                # Validate basic URI format
                if not _is_valid_uri(uri_str):
                    result.add_error(f"Invalid or empty URI: {uri_str}")
                    continue

                # Try to get labels for this URI
                try:
                    name = get_label(graph, uri_str)
                    alt_labels = get_alt_labels(graph, uri_str)

                    # Create concept node for validation
                    concept_node: ConceptNode = {
                        "name": name,
                        "altLabels": alt_labels,
                        "children": [],
                    }

                    # Classify the URI to determine if it needs suffix validation
                    resource_type = _classify_uri_resource_type(
                        graph, uri_str, base_uris
                    )

                    if resource_type:
                        # Apply suffix validation for classified resources
                        _validate_concept_node(
                            concept_node, uri_str, resource_type, seen_labels, result
                        )
                        logger.debug(f"Validated {uri_str} as {resource_type} resource")
                    else:
                        # For unclassified URIs, only validate labels uniqueness
                        label_errors = _validate_labels_uniqueness(
                            concept_node["name"],
                            concept_node["altLabels"],
                            seen_labels,
                            uri_str,
                        )
                        for error in label_errors:
                            result.add_error(error)
                        logger.debug(
                            f"Validated {uri_str} as unclassified resource (labels only)"
                        )

                except ValueError:
                    # URI doesn't have labels, which is okay for some resources
                    logger.debug(
                        f"URI {uri_str} has no labels - skipping label validation"
                    )
                    continue

            except Exception as e:
                result.add_error(f"Unexpected error validating URI {uri_str}: {e}")

        # Report summary of what was found
        classified_count = sum(
            1
            for uri in all_subjects
            if _classify_uri_resource_type(graph, uri, base_uris)
        )
        logger.info(
            f"Validated {len(all_subjects)} URIs total, {classified_count} classified resources"
        )

    except Exception as e:
        result.add_error(f"Failed to parse RDF graph: {e}")
        return result

    # Log summary
    summary = result.get_summary()
    logger.info(
        f"Validation completed. Valid: {summary['is_valid']}, "
        f"Errors: {summary['error_count']}, Warnings: {summary['warning_count']}"
    )

    return result


def validate_ontology_from_file(file_path: str) -> ValidationResult:
    """
    Validate ontology from RDF file.

    Args:
        file_path: Path to RDF file

    Returns:
        ValidationResult containing validation status and any errors/warnings
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            rdf_content = f.read()
        return validate_ontology_rdf(rdf_content)
    except Exception as e:
        result = ValidationResult()
        result.add_error(f"Failed to read RDF file {file_path}: {e}")
        return result


# Convenience function for quick validation checks
def is_ontology_valid(rdf_content: str) -> bool:
    """
    Quick validation check that returns only boolean result.

    Args:
        rdf_content: Raw RDF content as string

    Returns:
        True if ontology is valid, False otherwise
    """
    return validate_ontology_rdf(rdf_content).is_valid


async def main():
    """
    Main function that validates the SUDOKN.rdf file.

    This function initializes AWS clients and validates the standard SUDOKN ontology file.
    """
    import asyncio
    from pathlib import Path

    # Initialize AWS clients (required for ontology service)
    try:
        from infra.utils.aws.clients import (
            AWSClientName,
            initialize_aws_clients,
        )

        await initialize_aws_clients(AWSClientName.PROMPT_RDF_S3)
        logger.info("AWS clients initialized successfully")
    except Exception as e:
        logger.warning(f"Could not initialize AWS clients: {e}")

    # Look for SUDOKN.rdf in the same directory as this script
    script_dir = Path(__file__).parent
    possible_paths = [
        # script_dir / "SUDOKN.rdf",
        script_dir / "SUDOKN1_1.rdf",
        # Path("SUDOKN.rdf"),
        # Path("data/SUDOKN.rdf"),
        # Path("ontology/SUDOKN.rdf"),
        # Path("../SUDOKN.rdf"),
        # Path("../../SUDOKN.rdf"),
    ]

    sudokn_file = None
    for path in possible_paths:
        if path.exists():
            sudokn_file = path
            break

    if not sudokn_file:
        print("❌ ERROR: SUDOKN.rdf file not found in common locations.")
        print("📂 Searched in:")
        for path in possible_paths:
            print(f"   - {path.resolve()}")
        print(
            "\n💡 Please ensure SUDOKN.rdf is in one of these locations or specify the path."
        )
        return False

    print("🔍 SUDOKN Ontology Validator")
    print("=" * 40)
    print(f"📁 Found SUDOKN.rdf at: {sudokn_file.resolve()}")
    print()

    # Validate the ontology
    try:
        result = validate_ontology_from_file(str(sudokn_file))
        summary = result.get_summary()

        # Print results with clear formatting
        status_icon = "✅" if summary["is_valid"] else "❌"
        print(
            f"{status_icon} Validation Status: {'PASSED' if summary['is_valid'] else 'FAILED'}"
        )
        print(f"📊 Total Errors: {summary['error_count']}")
        print(f"⚠️  Total Warnings: {summary['warning_count']}")

        if summary["errors"]:
            print(f"\n🚨 ERRORS ({summary['error_count']}):")
            for i, error in enumerate(summary["errors"], 1):
                print(f"  {i}. {error}")

        if summary["warnings"]:
            print(f"\n⚠️  WARNINGS ({summary['warning_count']}):")
            for i, warning in enumerate(summary["warnings"], 1):
                print(f"  {i}. {warning}")

        print(f"\n📋 Summary:")
        print(f"   - File: {sudokn_file.name}")
        print(f"   - Status: {'VALID' if summary['is_valid'] else 'INVALID'}")
        print(
            f"   - Issues: {summary['error_count']} errors, {summary['warning_count']} warnings"
        )

        return summary["is_valid"]

    except Exception as e:
        print(f"❌ ERROR: Failed to validate SUDOKN.rdf: {e}")
        return False


if __name__ == "__main__":
    """Run the main validation when script is executed directly."""
    import asyncio
    import sys

    # Set up logging for command line usage
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run the main function and exit with appropriate code
    try:
        is_valid = asyncio.run(main())
        sys.exit(0 if is_valid else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Validation interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ FATAL ERROR: {e}")
        sys.exit(1)
