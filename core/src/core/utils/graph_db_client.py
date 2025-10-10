import os
import httpx

# === Configuration ===
GRAPH_DB_BASE_URL = os.getenv("GRAPH_DB_BASE_URL")
if not GRAPH_DB_BASE_URL:
    raise ValueError("GRAPH_DB_BASE_URL environment variable is not set")


class SPARQLQueryError(Exception):
    """Raised when a SPARQL query fails or returns invalid data."""

    pass


async def send_update_query_to_db(payload: str, debug: bool = False) -> None:
    """Send a SPARQL UPDATE (INSERT/DELETE) query to the /statements endpoint."""
    assert GRAPH_DB_BASE_URL is not None
    endpoint = f"{GRAPH_DB_BASE_URL.rstrip('/')}/statements"

    headers = {
        "Accept": "application/sparql-results+json",
        "Content-Type": "application/sparql-update",
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                endpoint, content=payload, headers=headers, timeout=10
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
