import re

from data_etl_app.models.skos_concept import Concept


def keyword_regex(keyword: str):
    # (?<!\w) asserts that the preceding character (if any) is not a word character.
    # (?=\W|$) asserts that the following character is either a non-word character or the end of the string.
    return r"(?<!\w)" + re.escape(keyword) + r"(?=\W|$)"


# only considers concept and altLabels, ignores ancestors
def brute_search(
    text: str, concepts: list[Concept], debug: bool = False
) -> set[Concept]:
    brute_search_concepts: set[Concept] = set()

    for c in concepts:
        if any(
            re.search(keyword_regex(label.lower()), text) for label in c.matchLabels
        ):
            brute_search_concepts.add(c)

    if debug:
        print(
            f"brute_search_concepts {len(brute_search_concepts)}:{brute_search_concepts}"
        )

    return brute_search_concepts
