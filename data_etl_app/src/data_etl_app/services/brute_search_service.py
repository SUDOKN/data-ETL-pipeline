import re
import logging

from data_etl_app.models.skos_concept import Concept

logger = logging.getLogger(__name__)


def word_regex(keyword: str):
    # (?<!\w) asserts that the preceding character (if any) is not a word character.
    # (?=\W|$) asserts that the following character is either a non-word character or the end of the string.
    return r"(?<!\w)" + re.escape(keyword) + r"(?=\W|$)"


# only considers concept and altLabels, ignores ancestors
def brute_search(text: str, concepts: set[Concept]) -> set[str]:
    found_brute_search_labels: set[str] = set()

    for c in concepts:
        for label in c.matchLabels:
            if re.search(word_regex(label), text, re.IGNORECASE):
                found_brute_search_labels.add(label)

    logger.info(
        f"Brute search found {len(found_brute_search_labels)}:{found_brute_search_labels} concepts in text."
    )

    return found_brute_search_labels
