from services.prompt_service import prompt_service
from services.binary_classifier_service import (
    binary_classifier,
)

from models.db.binary_classifier_result import (
    BinaryClassifierResult,
)


async def is_company_a_manufacturer(
    manufacturer_url: str, text: str, debug: bool = False
) -> BinaryClassifierResult:
    return await binary_classifier(
        "is_manufacturer",
        manufacturer_url,
        text,
        prompt_service.is_manufacturer_prompt,
        debug=debug,
    )
