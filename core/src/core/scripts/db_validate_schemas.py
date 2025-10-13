#!/usr/bin/env python3
"""
Database Schema Validation Script

This script validates that all MongoDB collections have the correct JSON schema validators
by comparing the current validators in the database with the expected schemas defined in
the db_schemas directory.

Usage:
    python db_validate_schemas.py

Environment Variables:
    MONGO_DB_URI: MongoDB connection string (required)
    MONGODB_DATABASE: Database name (default: 'sudokn')
    APPLY_SCHEMAS: If 'true', apply schemas to missing collections/validators (default: 'false')
    FORCE_OVERWRITE: If 'true', overwrite existing validators even if they differ (default: 'false')

Requirements:
    - MongoDB connection configured
    - Proper environment variables set
    - PyMongo driver for MongoDB
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Tuple

from pymongo import MongoClient

from core.dependencies.load_core_env import load_core_env

# Load environment variables
load_core_env()

logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""

    pass


class DatabaseSchemaValidator:
    """Validates MongoDB collection schemas against expected schema files."""

    # Mapping of collection names to their schema filenames
    SCHEMA_MAPPINGS = {
        "manufacturers": "manufacturer.schema.json",
        "users": "user.schema.json",
        "binary_ground_truths": "binary_ground_truth.schema.json",
        "concept_ground_truths": "concept_ground_truth.schema.json",
        "keyword_ground_truths": "keyword_ground_truth.schema.json",
        "scraping_errors": "scraping_error.schema.json",
        "extraction_errors": "extraction_error.schema.json",
        "gpt_batch_requests": "gpt_batch_request.schema.json",
        "deferred_manufacturers": "deferred_manufacturer.schema.json",
    }

    def __init__(self, connection_string: str, database_name: str):
        """Initialize the schema validator.

        Args:
            connection_string: MongoDB connection string
            database_name: Name of the database to validate
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]
        self.schema_dir = Path(__file__).parent.parent / "db_schemas"

        if not self.schema_dir.exists():
            raise FileNotFoundError(f"Schema directory not found: {self.schema_dir}")

    def collection_exists(self, collection_name: str) -> bool:
        """Check if a collection exists in the database.

        Args:
            collection_name: Name of the collection

        Returns:
            True if collection exists, False otherwise
        """
        try:
            collection_info = self.db.command(
                "listCollections", filter={"name": collection_name}
            )
            collections = collection_info.get("cursor", {}).get("firstBatch", [])
            return len(collections) > 0
        except Exception as e:
            logger.error(
                f"Failed to check if collection '{collection_name}' exists: {e}"
            )
            raise

    def get_collection_validator(self, collection_name: str) -> Optional[dict]:
        """Fetch the current JSON schema validator for a collection.

        Args:
            collection_name: Name of the collection

        Returns:
            The validator dict if it exists, None otherwise
        """
        try:
            collection_info = self.db.command(
                "listCollections", filter={"name": collection_name}
            )
            collections = collection_info.get("cursor", {}).get("firstBatch", [])

            if not collections:
                logger.warning(f"Collection '{collection_name}' not found in database")
                return None

            collection_data = collections[0]
            validator = collection_data.get("options", {}).get("validator", {})

            return validator if validator else None

        except Exception as e:
            logger.error(
                f"Failed to fetch validator for collection '{collection_name}': {e}"
            )
            raise

    def load_schema_file(self, schema_filename: str) -> Optional[dict]:
        """Load a JSON schema file from the db_schemas directory.

        Args:
            schema_filename: Name of the schema file

        Returns:
            The schema dict if successful, None otherwise
        """
        try:
            schema_path = self.schema_dir / schema_filename

            if not schema_path.exists():
                logger.warning(f"Schema file not found: {schema_path}")
                return None

            with open(schema_path, "r") as f:
                return json.load(f)

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in schema file '{schema_filename}': {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load schema file '{schema_filename}': {e}")
            raise

    def compare_schemas(
        self, collection_name: str, schema_filename: str
    ) -> Tuple[bool, Optional[str]]:
        """Compare the current collection validator with the expected schema file.

        Args:
            collection_name: Name of the collection to validate
            schema_filename: Name of the schema file to compare against

        Returns:
            Tuple of (is_match, error_message)
            - is_match: True if schemas match, False otherwise
            - error_message: Description of the mismatch if any, None if schemas match
        """
        logger.info(f"Validating schema for '{collection_name}'...")

        # Check if collection exists
        if not self.collection_exists(collection_name):
            error_msg = f"Collection '{collection_name}' does not exist in database"
            logger.error(f"  ✗ {error_msg}")
            logger.info(
                f"     Hint: Run with APPLY_SCHEMAS=true to create missing collections"
            )
            return False, error_msg

        current_validator = self.get_collection_validator(collection_name)
        expected_schema = self.load_schema_file(schema_filename)

        # Handle missing schemas
        if current_validator is None and expected_schema is None:
            logger.warning(
                f"  ⚠ Both current validator and schema file are missing for '{collection_name}'"
            )
            return True, None  # Consider it a match if both are missing

        if current_validator is None:
            error_msg = f"No validator set in database for '{collection_name}'"
            logger.error(f"  ✗ {error_msg}")
            logger.info(f"     Hint: Run with APPLY_SCHEMAS=true to apply the schema")
            return False, error_msg

        if expected_schema is None:
            error_msg = (
                f"No schema file found for '{collection_name}' ({schema_filename})"
            )
            logger.error(f"  ✗ {error_msg}")
            return False, error_msg

        # Compare the schemas
        if current_validator == expected_schema:
            logger.info(f"  ✓ Schema matches for '{collection_name}'")
            return True, None
        else:
            error_msg = self._generate_diff_message(
                collection_name, current_validator, expected_schema
            )
            logger.error(f"  ✗ Schema mismatch for '{collection_name}'")
            logger.info(f"     Hint: Run with APPLY_SCHEMAS=true to update the schema")
            logger.debug(f"\n{error_msg}")
            return False, error_msg

    def _generate_diff_message(
        self, collection_name: str, current: dict, expected: dict
    ) -> str:
        """Generate a detailed difference message between two schemas.

        Args:
            collection_name: Name of the collection
            current: Current validator schema
            expected: Expected schema

        Returns:
            A formatted string describing the differences
        """
        msg = f"\nSchema mismatch for '{collection_name}':\n"
        msg += "=" * 80 + "\n"
        msg += f"Current validator:\n{json.dumps(current, indent=2)}\n"
        msg += "=" * 80 + "\n"
        msg += f"Expected schema:\n{json.dumps(expected, indent=2)}\n"
        msg += "=" * 80 + "\n"
        return msg

    def validate_all_schemas(self, raise_on_mismatch: bool = True) -> bool:
        """Validate all collection schemas against their schema files.

        Args:
            raise_on_mismatch: If True, raise exception on any mismatch

        Returns:
            True if all schemas match, False otherwise

        Raises:
            SchemaValidationError: If raise_on_mismatch is True and any schema doesn't match
        """
        logger.info("=" * 80)
        logger.info("Starting Database Schema Validation")
        logger.info("=" * 80)

        results = {}
        error_messages = []

        for collection_name, schema_filename in self.SCHEMA_MAPPINGS.items():
            is_match, error_msg = self.compare_schemas(collection_name, schema_filename)
            results[collection_name] = is_match
            if not is_match and error_msg:
                error_messages.append(error_msg)

        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("Schema Validation Summary")
        logger.info("=" * 80)

        all_match = all(results.values())
        for collection_name, is_match in results.items():
            status = "✓ MATCH" if is_match else "✗ MISMATCH"
            log_func = logger.info if is_match else logger.error
            log_func(f"  {collection_name}: {status}")

        logger.info("=" * 80)

        if all_match:
            logger.info("✓ All schemas are valid!")
        else:
            logger.error("✗ Some schemas do not match!")
            if raise_on_mismatch:
                error_summary = "\n\n".join(error_messages)
                raise SchemaValidationError(
                    f"Schema validation failed for {len(error_messages)} collection(s):\n{error_summary}"
                )

        return all_match

    def apply_schema_validator(
        self,
        collection_name: str,
        schema_filename: str,
        create_if_missing: bool = True,
        force_overwrite: bool = False,
    ) -> bool:
        """Apply a JSON schema validator to a collection.

        Args:
            collection_name: Name of the collection
            schema_filename: Name of the schema file to apply
            create_if_missing: If True, create the collection if it doesn't exist
            force_overwrite: If True, overwrite existing validators even if they differ

        Returns:
            True if successful, False otherwise
        """
        try:
            schema = self.load_schema_file(schema_filename)
            if schema is None:
                logger.error(
                    f"Cannot apply validator - schema file not found: {schema_filename}"
                )
                return False

            # Check if collection exists
            if not self.collection_exists(collection_name):
                if create_if_missing:
                    logger.info(
                        f"Creating collection '{collection_name}' with schema validator..."
                    )
                    # Create collection with validator
                    self.db.create_collection(
                        collection_name,
                        validator=schema,
                        validationLevel="strict",
                        validationAction="error",
                    )
                    logger.info(
                        f"✓ Created collection '{collection_name}' with validator"
                    )
                    return True
                else:
                    logger.error(f"Collection '{collection_name}' does not exist")
                    return False

            # Collection exists, check if it has a validator
            current_validator = self.get_collection_validator(collection_name)

            if current_validator is None:
                # No validator exists, apply it
                logger.info(
                    f"Applying validator to '{collection_name}' (no existing validator)..."
                )
                self.db.command(
                    {
                        "collMod": collection_name,
                        "validator": schema,
                        "validationLevel": "strict",
                        "validationAction": "error",
                    }
                )
                logger.info(f"✓ Applied validator to '{collection_name}'")
                return True

            # Validator exists, check if it matches
            if current_validator == schema:
                logger.info(
                    f"✓ Validator for '{collection_name}' already matches (no update needed)"
                )
                return True
            else:
                logger.warning(
                    f"⚠ Validator exists for '{collection_name}' but differs from expected schema"
                )

            # Validator exists but differs
            if force_overwrite:
                logger.warning(
                    f"Overwriting existing validator for '{collection_name}' (FORCE_OVERWRITE=true)..."
                )
                self.db.command(
                    {
                        "collMod": collection_name,
                        "validator": schema,
                        "validationLevel": "strict",
                        "validationAction": "error",
                    }
                )
                logger.info(f"✓ Overwrote validator for '{collection_name}'")
                return True
            else:
                logger.warning(
                    f"   Set FORCE_OVERWRITE=true to overwrite the existing validator"
                )
                logger.debug(
                    self._generate_diff_message(
                        collection_name, current_validator, schema
                    )
                )
                return False

        except Exception as e:
            logger.error(f"Failed to apply validator to '{collection_name}': {e}")
            return False

    def apply_all_validators(
        self, create_if_missing: bool = True, force_overwrite: bool = False
    ) -> bool:
        """Apply all JSON schema validators to their respective collections.

        Args:
            create_if_missing: If True, create collections that don't exist
            force_overwrite: If True, overwrite existing validators even if they differ

        Returns:
            True if all validators were applied successfully, False otherwise
        """
        logger.info("=" * 80)
        logger.info("Applying JSON Schema Validators")
        logger.info("=" * 80)

        success_count = 0
        failure_count = 0
        skipped_count = 0

        for collection_name, schema_filename in self.SCHEMA_MAPPINGS.items():
            result = self.apply_schema_validator(
                collection_name, schema_filename, create_if_missing, force_overwrite
            )
            if result:
                success_count += 1
            else:
                # Check if it was skipped due to existing validator mismatch
                if (
                    self.collection_exists(collection_name)
                    and self.get_collection_validator(collection_name) is not None
                ):
                    skipped_count += 1
                else:
                    failure_count += 1

        logger.info("=" * 80)
        logger.info(f"Applied {success_count} validators successfully")
        if skipped_count > 0:
            logger.warning(
                f"Skipped {skipped_count} validators (existing validators differ, use FORCE_OVERWRITE=true)"
            )
        if failure_count > 0:
            logger.error(f"{failure_count} failures")
        logger.info("=" * 80)

        return failure_count == 0 and skipped_count == 0

    def close(self):
        """Close the database connection."""
        self.client.close()


def get_connection_string() -> str:
    """Get MongoDB connection string from environment variables.

    Returns:
        MongoDB connection string

    Raises:
        ValueError: If connection string is not found
    """
    connection_string = os.getenv("MONGO_DB_URI")
    if not connection_string:
        raise ValueError(
            "MongoDB connection string not found. Set MONGO_DB_URI environment variable."
        )
    return connection_string


def get_database_name() -> str:
    """Get database name from environment variables.

    Returns:
        Database name (defaults to 'sudokn')
    """
    return os.getenv("MONGODB_DATABASE", "sudokn")


def main():
    """Main execution function."""
    try:
        connection_string = get_connection_string()
        database_name = get_database_name()

        logger.info(f"Database: {database_name}")
        masked_conn = (
            connection_string.replace(connection_string.split("@")[-1], "@***")
            if "@" in connection_string
            else connection_string
        )
        logger.info(f"Connection: {masked_conn}\n")

        validator = DatabaseSchemaValidator(connection_string, database_name)

        try:
            # Test connection
            validator.client.admin.command("ping")
            logger.info("✓ Successfully connected to MongoDB\n")

            # Check if we should apply schemas
            apply_schemas = os.getenv("APPLY_SCHEMAS", "false").lower() == "true"
            force_overwrite = os.getenv("FORCE_OVERWRITE", "false").lower() == "true"

            if apply_schemas:
                # Apply all schemas
                success = validator.apply_all_validators(
                    create_if_missing=True, force_overwrite=force_overwrite
                )
                if not success:
                    if force_overwrite:
                        logger.error("Failed to apply some schemas")
                        exit(1)
                    else:
                        logger.warning("Some validators were skipped due to mismatches")
                        logger.warning(
                            "Run with FORCE_OVERWRITE=true to overwrite existing validators"
                        )
                        logger.info("\nProceeding with validation...\n")

                # Validate after applying
                logger.info("\nValidating applied schemas...\n")
                validator.validate_all_schemas(raise_on_mismatch=True)
            else:
                # Just validate
                validator.validate_all_schemas(raise_on_mismatch=True)

            logger.info("\n✓ Schema validation completed successfully!")

        finally:
            validator.close()

    except KeyboardInterrupt:
        logger.info("\nSchema validation interrupted by user")
        exit(130)
    except SchemaValidationError as e:
        logger.error(f"\n{e}")
        exit(1)
    except Exception as e:
        logger.error(f"\nSchema validation failed: {e}", exc_info=True)
        exit(1)


if __name__ == "__main__":
    main()
