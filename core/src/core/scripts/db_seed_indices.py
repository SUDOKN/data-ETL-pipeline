#!/usr/bin/env python3
"""
Database Index Seeding Script

This script creates all necessary MongoDB indexes for the SUDOKN data ETL pipeline collections.
It reads the index specifications from the model files and creates them in the database.

Usage:
    python db_seed_indices.py

Requirements:
    - MongoDB connection configured
    - Proper environment variables set
    - PyMongo driver for MongoDB
"""

import logging
from pymongo import MongoClient
import os
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DatabaseIndexSeeder:
    """Handles creation of all database indexes for SUDOKN collections."""

    def __init__(self, connection_string: str, database_name: str):
        self.client = MongoClient(connection_string)
        self.db = self.client[database_name]

    def create_manufacturer_indexes(self):
        """Create indexes for manufacturers collection."""
        collection = self.db.manufacturers

        indexes = [
            {
                "keys": [("etld1", 1)],
                "options": {"name": "mfg_etld1_unique_idx", "unique": True},
            },
            {
                "keys": [("url_accessible_at", 1)],
                "options": {"name": "mfg_url_accessible_at_idx", "unique": True},
            },
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def create_user_indexes(self):
        """Create indexes for users collection."""
        collection = self.db.users

        indexes = [
            {
                "keys": [("email", 1)],
                "options": {"name": "unique_user_email", "unique": True},
            }
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def create_manufacturer_user_form_indexes(self):
        """Create indexes for manufacturer_user_forms collection."""
        collection = self.db.manufacturer_user_forms

        indexes = [
            {
                "keys": [("mfg_etld1", 1)],
                "options": {"name": "mfg_user_form_unique_per_etld1", "unique": True},
            }
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def create_binary_ground_truth_indexes(self):
        """Create indexes for binary_ground_truths collection."""
        collection = self.db.binary_ground_truths

        indexes = [
            {
                "keys": [
                    ("mfg_etld1", 1),
                    ("scraped_text_file_version_id", 1),
                    ("classification_type", 1),
                ],
                "options": {"name": "binary_gt_unique_idx", "unique": True},
            }
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def create_concept_ground_truth_indexes(self):
        """Create indexes for concept_ground_truths collection."""
        collection = self.db.concept_ground_truths

        indexes = [
            {
                "keys": [
                    ("mfg_etld1", 1),
                    ("scraped_text_file_version_id", 1),
                    ("extract_prompt_version_id", 1),
                    ("map_prompt_version_id", 1),
                    ("ontology_version_id", 1),
                    ("concept_type", 1),
                    ("chunk_no", 1),
                ],
                "options": {"name": "concept_gt_unique_idx", "unique": True},
            }
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def create_keyword_ground_truth_indexes(self):
        """Create indexes for keyword_ground_truths collection."""
        collection = self.db.keyword_ground_truths

        indexes = [
            {
                "keys": [
                    ("mfg_etld1", 1),
                    ("scraped_text_file_version_id", 1),
                    ("extract_prompt_version_id", 1),
                    ("keyword_type", 1),
                    ("chunk_no", 1),
                ],
                "options": {"name": "keyword_gt_unique_idx", "unique": True},
            }
        ]

        for index in indexes:
            try:
                collection.create_index(index["keys"], **index["options"])
                logger.info(f"Created index: {index['options']['name']}")
            except Exception as e:
                logger.error(f"Failed to create index {index['options']['name']}: {e}")

    def drop_collection_indexes(self, collection_name: str):
        """Drop all indexes for a specific collection (except _id_)."""
        try:
            collection = self.db[collection_name]
            indexes = collection.index_information()

            # Count total indexes (excluding _id_ which cannot be dropped)
            droppable_indexes = [name for name in indexes.keys() if name != "_id_"]

            if not droppable_indexes:
                logger.info(f"No custom indexes found for {collection_name} collection")
                return

            logger.info(
                f"Dropping {len(droppable_indexes)} indexes from {collection_name} collection"
            )

            for index_name in droppable_indexes:
                try:
                    collection.drop_index(index_name)
                    logger.info(f"Dropped index: {index_name}")
                except Exception as e:
                    logger.warning(f"Failed to drop index {index_name}: {e}")

        except Exception as e:
            logger.warning(f"Could not drop indexes for {collection_name}: {e}")

    def drop_all_indexes(self):
        """Drop all custom indexes from all collections."""
        collections = [
            "manufacturers",
            "users",
            "manufacturer_user_forms",
            "binary_ground_truths",
            "concept_ground_truths",
            "keyword_ground_truths",
        ]

        logger.info("Dropping all existing custom indexes...")

        for collection_name in collections:
            self.drop_collection_indexes(collection_name)

        logger.info("Completed dropping existing indexes")

    def seed_all_indexes(self, drop_existing: bool = True):
        """Drop all existing indexes and create new ones for all collections.

        Args:
            drop_existing: Whether to drop existing indexes before creating new ones.
                          Defaults to True.
        """
        logger.info("Starting database index seeding process...")

        try:
            # Test connection
            self.client.admin.command("ping")
            logger.info("Successfully connected to MongoDB")

            # Drop all existing indexes first if requested
            if drop_existing:
                self.drop_all_indexes()
            else:
                logger.info("Skipping index dropping (drop_existing=False)")

            # Create indexes for each collection
            logger.info("Creating new indexes...")
            self.create_manufacturer_indexes()
            self.create_user_indexes()
            self.create_manufacturer_user_form_indexes()
            self.create_binary_ground_truth_indexes()
            self.create_concept_ground_truth_indexes()
            self.create_keyword_ground_truth_indexes()

            logger.info("Database index seeding completed successfully!")

        except Exception as e:
            logger.error(f"Failed to seed database indexes: {e}")
            raise
        finally:
            self.client.close()

    def list_existing_indexes(self):
        """List all existing indexes in the database collections."""
        collections = [
            "manufacturers",
            "users",
            "manufacturer_user_forms",
            "binary_ground_truths",
            "concept_ground_truths",
            "keyword_ground_truths",
        ]

        logger.info("Listing existing indexes...")

        for collection_name in collections:
            try:
                collection = self.db[collection_name]
                indexes = collection.index_information()
                logger.info(f"\n{collection_name} collection indexes:")
                for index_name, index_info in indexes.items():
                    logger.info(f"  - {index_name}: {index_info}")
            except Exception as e:
                logger.warning(f"Could not list indexes for {collection_name}: {e}")


def get_connection_string() -> str:
    """Get MongoDB connection string from environment variables."""
    # Try to get from common environment variable names
    connection_string = os.getenv("MONGO_DB_URI")
    if not connection_string:
        raise ValueError(
            "MongoDB connection string not found in environment variables."
        )
    return connection_string


def get_database_name() -> str:
    """Get database name from environment variables."""
    return os.getenv("MONGODB_DATABASE", "sudokn")  # default to 'sudokn'


def main():
    """Main execution function."""
    try:
        connection_string = get_connection_string()
        database_name = get_database_name()

        logger.info(f"Using database: {database_name}")
        logger.info(
            f"Connection string: {connection_string.replace(connection_string.split('@')[-1], '@***') if '@' in connection_string else connection_string}"
        )

        seeder = DatabaseIndexSeeder(connection_string, database_name)

        # Optionally list existing indexes first
        if os.getenv("LIST_INDEXES", "false").lower() == "true":
            seeder.list_existing_indexes()

        # Control whether to drop existing indexes
        drop_existing = os.getenv("DROP_EXISTING", "true").lower() == "true"

        # Create all indexes
        seeder.seed_all_indexes(drop_existing=drop_existing)

    except KeyboardInterrupt:
        logger.info("Index seeding interrupted by user")
    except Exception as e:
        logger.error(f"Index seeding failed: {e}")
        raise


if __name__ == "__main__":
    DOT_ENV_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), ".env"
    )

    logger.info(f"Loading environment variables from: {DOT_ENV_PATH}")
    load_dotenv(dotenv_path=DOT_ENV_PATH)
    main()
