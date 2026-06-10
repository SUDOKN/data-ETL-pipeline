from typing import Optional

from pymongo.errors import DuplicateKeyError

from core.models.field_types import MfgETLDType, MfgURLType
from core.models.db.manufacturer import Batch, Manufacturer
from core.models.queue_item import EmailUserErrand
from core.models.to_scrape_item import ToScrapeItem
from core.utils.time_util import get_current_time
from core.utils.aws.queue.priority_scrape_queue_util import (
    push_item_to_priority_scrape_queue,
)

from core.models.db.manufacturer_user_form import ManufacturerUserForm
from data_etl_app.services.knowledge.ontology_service import get_ontology_service


async def validate_and_create_from_manufacturer(
    manufacturer: Manufacturer,
) -> ManufacturerUserForm:
    """
    Creates a ManufacturerUserForm instance from a Manufacturer instance.

    Args:
        manufacturer (Manufacturer): The Manufacturer instance to create the form from.

    Verifies some of the optional fields in Manufacturer are not None before assignment. Raises AssertionError (manually to bypass optimized mode) if any of these conditions are not met:
        - is_manufacturer must not be None
        - is_contract_manufacturer must not be None
        - is_product_manufacturer must not be None

        - email_addresses must not be None, even if empty list
        - business_desc must not be None
        - products must not be None, even if empty list
        - certificates must not be None, even if empty list
        - industries must not be None, even if empty list
        - process_caps must not be None, even if empty list
        - material_caps must not be None, even if empty list

    Returns:
        ManufacturerUserForm: The created ManufacturerUserForm instance.
    """
    if manufacturer.is_manufacturer is None:
        raise AssertionError("is_manufacturer must not be None")
    # if manufacturer.is_contract_manufacturer is None:
    #     raise AssertionError("is_contract_manufacturer must not be None")
    # if manufacturer.is_product_manufacturer is None:
    #     raise AssertionError("is_product_manufacturer must not be None")
    # if manufacturer.email_addresses is None:
    #     raise AssertionError("email_addresses must not be None")
    if manufacturer.business_desc is None:
        raise AssertionError("business_desc must not be None")
    if manufacturer.addresses is None:
        # raise AssertionError("addresses must not be None")
        manufacturer.addresses = []
    else:
        for i, addr in enumerate(manufacturer.addresses):
            if addr is None:
                raise AssertionError("addresses must not contain None values")
            else:
                required_fields = [
                    "city",
                    "state",
                    "country",
                ]
                for field in required_fields:
                    if getattr(addr, field) is None:
                        raise AssertionError(f"address {i} is missing field {field}")

                # assert addr.address_lines is not None
                # if len(addr.address_lines) < 1:
                #     raise AssertionError(
                #         f"address {i} must have at least one address line"
                #     )

                # assert addr.phone_numbers is not None
                # if len(addr.phone_numbers) < 1:
                #     raise AssertionError(
                #         f"address {i} must have at least one phone number"
                #     )

    if manufacturer.products is None:
        raise AssertionError("products must not be None")
    if manufacturer.certificates is None:
        raise AssertionError("certificates must not be None")
    if manufacturer.industries is None:
        raise AssertionError("industries must not be None")
    if manufacturer.process_caps is None:
        raise AssertionError("process_caps must not be None")
    if manufacturer.material_caps is None:
        raise AssertionError("material_caps must not be None")

    # Extract ontology_version_id from one of the concept extraction results
    # All concept extractions should have the same ontology version
    ontology_version_id = manufacturer.certificates.metadata.ontology_version_id

    return ManufacturerUserForm(
        author_email="",  # to be filled later
        etld1=manufacturer.etld1,
        ontology_version_id=ontology_version_id,
        name=manufacturer.name,
        founded_in=manufacturer.founded_in,
        email_addresses=manufacturer.email_addresses,
        num_employees=manufacturer.num_employees,
        business_desc=manufacturer.business_desc.result,
        business_statuses=manufacturer.business_statuses,
        primary_naics=manufacturer.primary_naics,
        secondary_naics=manufacturer.secondary_naics,
        addresses=[addr for addr in manufacturer.addresses.result],
        products=manufacturer.products.results,
        certificates=manufacturer.certificates.results,
        industries=manufacturer.industries.results,
        process_caps=manufacturer.process_caps.results,
        material_caps=manufacturer.material_caps.results,
        notes=None,
    )


async def get_manufacturer_user_form_by_mfg_etld1(
    mfg_etld1: MfgETLDType,
) -> Optional[ManufacturerUserForm]:
    """
    Fetches the ManufacturerUserForm document for the given mfg_etld1.

    Args:
        mfg_etld1 (MfgETLDType): The effective top-level domain plus one of the manufacturer.

    Returns:
        Optional[ManufacturerUserForm]: The ManufacturerUserForm document if found, else None.
    """
    return await ManufacturerUserForm.find_one(ManufacturerUserForm.etld1 == mfg_etld1)


async def save_manufacturer_user_form(
    form: ManufacturerUserForm,
) -> ManufacturerUserForm:
    """
    Saves or updates the ManufacturerUserForm document for the given mfg_etld1.

    Args:
        mfg_etld1 (MfgETLDType): The effective top-level domain plus one of the manufacturer.
        form (ManufacturerUserForm): The ManufacturerUserForm document to be saved or updated.

    Returns:
        ManufacturerUserForm: The saved or updated ManufacturerUserForm document.
    """
    await form.save()
    return form


async def create_blank_manufacturer_user_form(
    *,
    author_email: str,
    mfg_etld1: MfgETLDType,
) -> ManufacturerUserForm:
    """
    Creates and persists a blank ManufacturerUserForm draft.

    This is used by the manual registration flow when the manufacturer does not
    already exist in Mongo or GraphDB.
    """
    # Get the latest ontology version
    ontology_service = await get_ontology_service()
    latest_ontology = await ontology_service.get_latest_ontology()

    draft = ManufacturerUserForm(
        author_email=author_email,
        mfg_etld1=mfg_etld1,
        ontology_version_id=latest_ontology.version_id,
        name=None,
        founded_in=None,
        email_addresses=None,
        num_employees=None,
        business_statuses=None,
        primary_naics=None,
        secondary_naics=None,
        addresses=[],
        business_desc=None,
        products=set(),
        certificates=[],
        industries=[],
        process_caps=[],
        material_caps=[],
        notes=None,
    )

    try:
        await draft.save()
        return draft
    except DuplicateKeyError:
        # Concurrent draft creation requests can race on the unique mfg_etld1 index.
        existing = await get_manufacturer_user_form_by_mfg_etld1(mfg_etld1)
        if existing:
            return existing
        raise


async def enqueue_manufacturer_for_priority_scrape(
    *,
    author_email: str,
    mfg_url: MfgURLType,
    title: str,
) -> None:
    current_timestamp = get_current_time()
    await push_item_to_priority_scrape_queue(
        ToScrapeItem(
            accessible_normalized_url=mfg_url,
            batch=Batch(
                title=title,
                timestamp=current_timestamp,
            ),
            email_errand=EmailUserErrand(user_email=author_email),
        ),
    )
