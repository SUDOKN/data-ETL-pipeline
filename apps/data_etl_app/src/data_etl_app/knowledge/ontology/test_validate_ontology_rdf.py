"""
Unit tests for ontology RDF validation functionality.

These tests verify that the validation logic correctly identifies:
- Invalid URIs
- Incorrect URI suffixes
- Duplicate labels
- Missing labels
- Proper recursive validation
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
from pathlib import Path

# Add the src directories to the path
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

from apps.data_etl_app.src.data_etl_app.knowledge.ontology.validate_ontology_rdf import (
    ValidationResult,
    ValidationError,
    _is_valid_uri,
    _validate_uri_suffix,
    _validate_labels_uniqueness,
    _validate_concept_node,
    validate_ontology_rdf,
    is_ontology_valid,
)
from packages.core.src.core.models.skos_concept import ConceptNode


class TestValidationResult(unittest.TestCase):
    """Test the ValidationResult class."""

    def setUp(self):
        self.result = ValidationResult()

    def test_initial_state(self):
        """Test initial state of ValidationResult."""
        self.assertTrue(self.result.is_valid)
        self.assertEqual(len(self.result.errors), 0)
        self.assertEqual(len(self.result.warnings), 0)

    def test_add_error(self):
        """Test adding errors marks validation as failed."""
        self.result.add_error("Test error")
        self.assertFalse(self.result.is_valid)
        self.assertEqual(len(self.result.errors), 1)
        self.assertIn("Test error", self.result.errors)

    def test_add_warning(self):
        """Test adding warnings doesn't affect validity."""
        self.result.add_warning("Test warning")
        self.assertTrue(self.result.is_valid)
        self.assertEqual(len(self.result.warnings), 1)
        self.assertIn("Test warning", self.result.warnings)

    def test_get_summary(self):
        """Test summary generation."""
        self.result.add_error("Error 1")
        self.result.add_warning("Warning 1")

        summary = self.result.get_summary()
        self.assertFalse(summary["is_valid"])
        self.assertEqual(summary["error_count"], 1)
        self.assertEqual(summary["warning_count"], 1)


class TestURIValidation(unittest.TestCase):
    """Test URI validation functions."""

    def test_valid_uris(self):
        """Test that valid URIs are recognized."""
        valid_uris = [
            "http://example.com/resource",
            "https://example.com/resource",
            "http://example.com/path/to/resource",
            "https://subdomain.example.com/resource",
        ]

        for uri in valid_uris:
            with self.subTest(uri=uri):
                self.assertTrue(_is_valid_uri(uri))

    def test_invalid_uris(self):
        """Test that invalid URIs are rejected."""
        invalid_uris = [
            "",
            None,
            "not-a-uri",
            "ftp://example.com",  # We want http/https
            "example.com",  # Missing scheme
            "http://",  # Missing netloc
        ]

        for uri in invalid_uris:
            with self.subTest(uri=uri):
                self.assertFalse(_is_valid_uri(uri))

    def test_uri_suffix_validation(self):
        """Test URI suffix validation for different resource types."""
        test_cases = [
            ("http://example.com/TestCapability", "process", True),
            ("http://example.com/TestCapability", "material", True),
            ("http://example.com/TestIndustry", "industry", True),
            ("http://example.com/TestCertificate", "certificate", True),
            ("http://example.com/TestWrong", "process", False),
            ("http://example.com/TestWrong", "material", False),
            ("http://example.com/TestWrong", "industry", False),
            ("http://example.com/TestWrong", "certificate", False),
        ]

        for uri, resource_type, expected in test_cases:
            with self.subTest(uri=uri, resource_type=resource_type):
                self.assertEqual(_validate_uri_suffix(uri, resource_type), expected)


class TestLabelValidation(unittest.TestCase):
    """Test label uniqueness validation."""

    def test_unique_labels(self):
        """Test that unique labels pass validation."""
        seen_labels = set()
        errors = _validate_labels_uniqueness(
            "Primary Label",
            ["Alt Label 1", "Alt Label 2"],
            seen_labels,
            "http://example.com/test",
        )

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(seen_labels), 3)

    def test_duplicate_within_concept(self):
        """Test that duplicate labels within a concept are caught."""
        seen_labels = set()
        errors = _validate_labels_uniqueness(
            "Same Label",
            ["Alt Label", "Same Label"],  # Duplicate with primary
            seen_labels,
            "http://example.com/test",
        )

        # Should find at least 1 error for the duplicate, but might find more
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("Duplicate label" in error for error in errors))

    def test_duplicate_across_concepts(self):
        """Test that duplicate labels across concepts are caught."""
        seen_labels = {"Existing Label"}
        errors = _validate_labels_uniqueness(
            "Existing Label",  # Already in seen_labels
            ["New Label"],
            seen_labels,
            "http://example.com/test",
        )

        self.assertEqual(len(errors), 1)
        self.assertIn("already used elsewhere", errors[0])


class TestConceptNodeValidation(unittest.TestCase):
    """Test concept node validation."""

    def setUp(self):
        self.result = ValidationResult()
        self.seen_labels = set()

    def test_valid_concept_node(self):
        """Test validation of a valid concept node."""
        concept: ConceptNode = {
            "name": "Valid Capability",
            "altLabels": ["Alt1", "Alt2"],
            "children": [],
        }

        _validate_concept_node(
            concept,
            "http://example.com/ValidCapability",
            "process",
            self.seen_labels,
            self.result,
        )

        self.assertTrue(self.result.is_valid)
        self.assertEqual(len(self.result.errors), 0)

    def test_invalid_uri_concept_node(self):
        """Test validation fails for invalid URI."""
        concept: ConceptNode = {
            "name": "Test Capability",
            "altLabels": [],
            "children": [],
        }

        _validate_concept_node(
            concept, "invalid-uri", "process", self.seen_labels, self.result
        )

        self.assertFalse(self.result.is_valid)
        self.assertGreater(len(self.result.errors), 0)

    def test_wrong_suffix_concept_node(self):
        """Test validation fails for wrong URI suffix."""
        concept: ConceptNode = {"name": "Test Wrong", "altLabels": [], "children": []}

        _validate_concept_node(
            concept,
            "http://example.com/TestWrong",
            "process",
            self.seen_labels,
            self.result,
        )

        self.assertFalse(self.result.is_valid)
        self.assertTrue(
            any(
                "does not end with required suffix" in error
                for error in self.result.errors
            )
        )


class TestRDFValidation(unittest.TestCase):
    """Test full RDF validation with mocked dependencies."""

    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.process_cap_uri")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.material_cap_uri")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.industry_uri")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.certificate_uri")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.get_graph")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.get_label")
    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.get_alt_labels")
    def test_validate_ontology_with_mocked_dependencies(
        self,
        mock_alt_labels,
        mock_label,
        mock_get_graph,
        mock_cert,
        mock_industry,
        mock_material,
        mock_process,
    ):
        """Test ontology validation with mocked dependencies."""
        # Setup mocks
        mock_process.return_value = "http://example.com/ProcessCapability"
        mock_material.return_value = "http://example.com/MaterialCapability"
        mock_industry.return_value = "http://example.com/Industry"
        mock_cert.return_value = "http://example.com/Certificate"

        # Create counter for unique labels
        label_counter = 0

        def get_unique_label(
            graph, uri
        ):  # Fixed: get_label expects graph and uri parameters
            nonlocal label_counter
            label_counter += 1
            return f"Label {label_counter}"

        mock_label.side_effect = get_unique_label
        mock_alt_labels.return_value = []  # No alt labels to avoid conflicts

        # Create a mock graph
        mock_graph = MagicMock()
        mock_graph.triples.return_value = []  # No subclasses for simplicity
        mock_get_graph.return_value = mock_graph

        # Test validation
        result = validate_ontology_rdf("<rdf></rdf>")

        # Should complete without errors (since we have no triples and unique labels)
        self.assertTrue(result.is_valid)

    def test_is_ontology_valid_convenience_function(self):
        """Test the convenience function for quick validation."""
        with patch(
            "data_etl_app.knowledge.ontology.validate_ontology_rdf.validate_ontology_rdf"
        ) as mock_validate:
            mock_result = ValidationResult()
            mock_result.is_valid = True
            mock_validate.return_value = mock_result

            self.assertTrue(is_ontology_valid("<rdf></rdf>"))

            mock_result.is_valid = False
            self.assertFalse(is_ontology_valid("<rdf></rdf>"))


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions."""

    def test_empty_rdf_content(self):
        """Test validation with empty RDF content."""
        result = validate_ontology_rdf("")
        self.assertFalse(result.is_valid)
        self.assertGreater(len(result.errors), 0)

    def test_malformed_rdf_content(self):
        """Test validation with malformed RDF content."""
        result = validate_ontology_rdf("not valid xml")
        self.assertFalse(result.is_valid)
        self.assertGreater(len(result.errors), 0)

    @patch("data_etl_app.knowledge.ontology.validate_ontology_rdf.process_cap_uri")
    def test_missing_base_uri_configuration(self, mock_process):
        """Test handling of missing base URI configuration."""
        mock_process.return_value = None

        with patch(
            "data_etl_app.knowledge.ontology.validate_ontology_rdf.get_graph"
        ) as mock_get_graph:
            mock_graph = MagicMock()
            # Mock empty graph (no triples)
            mock_graph.__iter__.return_value = iter([])
            mock_get_graph.return_value = mock_graph

            result = validate_ontology_rdf("<rdf></rdf>")

            # With the new validation logic, missing base URIs don't cause warnings
            # but the validation should still complete successfully with an empty graph
            self.assertTrue(result.is_valid)
            self.assertEqual(len(result.errors), 0)


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
