import httpx

from pure_utils.env_util import optional_env, require_env


def _graph_db_base_url() -> str:
    return require_env("GRAPH_DB_BASE_URL")


def _graph_db_update_timeout_seconds() -> float:
    return float(optional_env("GRAPH_DB_UPDATE_TIMEOUT_SECONDS", "120"))


class SPARQLQueryError(Exception):
    """Raised when a SPARQL query fails or returns invalid data."""

    pass


async def send_update_query_to_db(payload: str, debug: bool = False) -> None:
    """Send a SPARQL UPDATE (INSERT/DELETE) query to the /statements endpoint."""
    endpoint = f"{_graph_db_base_url().rstrip('/')}/statements"

    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-update",
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                endpoint,
                content=payload,
                headers=headers,
                timeout=_graph_db_update_timeout_seconds(),
            )
    except httpx.RequestError as e:
        raise SPARQLQueryError(f"Network error while querying {endpoint}") from e

    text = response.text

    if debug:
        print(f"Payload:\n{payload}")
    print(f"\n--- SPARQL UPDATE ---")
    print(f"Status: {response.status_code} | Success: {response.is_success}")
    print(f"Response Text:\n{text}")
    print(f"----------------------\n")

    if not response.is_success:
        raise SPARQLQueryError(f"GraphDB returned HTTP {response.status_code}: {text}")
