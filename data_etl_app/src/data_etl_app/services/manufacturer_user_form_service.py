from typing import Optional

from core.models.field_types import MfgETLDType
from core.models.db.manufacturer import Manufacturer

from core.models.db.manufacturer_user_form import ManufacturerUserForm


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
    if manufacturer.email_addresses is None:
        raise AssertionError("email_addresses must not be None")
    if manufacturer.business_desc is None:
        raise AssertionError("business_desc must not be None")
    if manufacturer.addresses is None:
        raise AssertionError("addresses must not be None")
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

    return ManufacturerUserForm(
        author_email="",  # to be filled later
        mfg_etld1=manufacturer.etld1,
        name=manufacturer.name,
        founded_in=manufacturer.founded_in,
        email_addresses=manufacturer.email_addresses,
        num_employees=manufacturer.num_employees,
        business_desc=manufacturer.business_desc,
        business_statuses=manufacturer.business_statuses,
        primary_naics=manufacturer.primary_naics,
        secondary_naics=manufacturer.secondary_naics,
        addresses=manufacturer.addresses,
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
    return await ManufacturerUserForm.find_one(
        ManufacturerUserForm.mfg_etld1 == mfg_etld1
    )


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
