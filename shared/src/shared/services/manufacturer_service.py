from datetime import datetime
from shared.models.db.manufacturer import Manufacturer
from shared.models.db.binary_classifier_result import (
    BinaryClassifierResult,
)

from data_etl_app.services.prompt_service import prompt_service
from data_etl_app.services.binary_classifier_service import (
    binary_classifier,
)


async def update_manufacturer(updated_at: datetime, manufacturer: Manufacturer):
    manufacturer.updated_at = updated_at
    manufacturer = Manufacturer.model_validate(manufacturer.model_dump())
    await manufacturer.save()


async def is_company_a_manufacturer(
    timestamp: datetime, manufacturer_url: str, text: str, debug: bool = False
) -> BinaryClassifierResult:
    return await binary_classifier(
        timestamp,
        "is_manufacturer",
        manufacturer_url,
        text,
        prompt_service.is_manufacturer_prompt,
        debug=debug,
    )
