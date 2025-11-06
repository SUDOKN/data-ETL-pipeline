import re
import logging

from data_etl_app.models.skos_concept import Concept

logger = logging.getLogger(__name__)


def word_regex(keyword: str):
    # (?<!\w) asserts that the preceding character (if any) is not a word character.
    # (?=\W|$) asserts that the following character is either a non-word character or the end of the string.
    return r"(?<!\w)" + re.escape(keyword) + r"(?=\W|$)"


# only considers concept and altLabels, ignores ancestors
def brute_search(text: str, concepts: set[Concept]) -> set[Concept]:
    found_brute_search_concepts: set[Concept] = set()

    for c in concepts:
        if any(re.search(word_regex(label.lower()), text) for label in c.matchLabels):
            found_brute_search_concepts.add(c)

    logger.debug(
        f"Brute search found {len(found_brute_search_concepts)}:{found_brute_search_concepts} concepts in text."
    )

    return found_brute_search_concepts
