#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import ijson
import time
import json
from urllib.parse import quote

from typing import Tuple, List, Dict, Optional
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD

# --- RDF NAMESPACE SETUP ---
SDK = Namespace("http://asu.edu/semantics/SUDOKN/")
IOF_CORE = Namespace("https://spec.industrialontologies.org/ontology/core/Core/")
IOF_SCRO = Namespace(
    "https://spec.industrialontologies.org/ontology/supplychain/SupplyChain/"
)
XSD_NS = Namespace("http://www.w3.org/2001/XMLSchema#")
RDFS_NS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
BFO = Namespace("http://purl.obolibrary.org/obo/")

g = Graph()
g.bind("sdk", SDK)
g.bind("iof-core", IOF_CORE)
g.bind("iof-scro", IOF_SCRO)
g.bind("xsd", XSD_NS)
g.bind("rdfs", RDFS_NS)
g.bind("bfo", BFO)

# --- STATIC DATA MAPS (assuming data is in JSON) ---
CITY_GEOID_MAP = {}
STATE_DCIDS = {}
ZIP_CACHE = {}

US_STATE_ABBR = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
}


def _norm(val):
    return (val or "").strip().lower()


def _norm_tuple(*args):
    return tuple(_norm(x) for x in args)


def _lookup_dict_case_insensitive(dct, *lookup_key):
    normed = _norm_tuple(*lookup_key)
    for k in dct:
        if _norm_tuple(*k) == normed:
            return dct[k]
    return None


def _lookup_dict_case_insensitive_state(dct, state):
    nstate = _norm(state)
    for k in dct:
        if _norm(k) == nstate:
            return dct[k]
    return None


def state_to_abbr(state):
    if not state:
        return state
    return US_STATE_ABBR.get(state.strip(), state)


def uri_strip(val: str) -> str:
    if val is None:
        return ""
    cleaned = (
        str(val)
        .replace(" ", "-")
        .replace("/", "-")
        .replace("&", "-")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
        .replace('"', "")
        .replace("'", "")
        .replace("`", "")
    )
    # Now ensure the whole result is url-encoded (for any accidental bad chars)
    return quote(cleaned, safe="-._~")


# Simplified lookup functions that assume all data is in JSON
def lookup_zip(
    line1: Optional[str],
    city: Optional[str],
    state: Optional[str],
    lat: Optional[float] = None,
    lon: Optional[float] = None,
) -> str:
    # Return zip from JSON data or default value
    attempts = []
    if line1 and city and state:
        attempts.append((line1, city, state))
    if city and state:
        attempts.append(("", city, state))
    if city:
        attempts.append(("", city, ""))
    if state:
        attempts.append(("", "", state))

    for key in attempts:
        zipc = _lookup_dict_case_insensitive(ZIP_CACHE, *key)
        if zipc not in (None, "", "unknown"):
            return zipc

    # If not found, return default zip
    return "00000"


def resolve_state(state: str) -> Optional[str]:
    geoid = _lookup_dict_case_insensitive_state(STATE_DCIDS, state)
    if geoid:
        return geoid
    return "unknown"


def lookup_city_geoid(city: str, state: str) -> Optional[str]:
    geoid = _lookup_dict_case_insensitive(CITY_GEOID_MAP, city, state)
    if geoid:
        return geoid
    return "unknown"


def extract_state_geoid(city_geoid: str) -> Optional[str]:
    if not city_geoid or len(city_geoid) < 2:
        return None
    return city_geoid[:2]


def enrich_company_rdf(
    company, g, SDK, IOF_CORE, IOF_SCRO, RDFS_NS, XSD_NS, CITY_GEOID_MAP, STATE_DCIDS
):
    gid_val = (
        company.get("global_id") or company.get("GlobalID") or company.get("Global_Id")
    )
    if not gid_val:
        return
    gid_stripped = uri_strip(gid_val)
    subj = SDK[f"{gid_stripped}-company-inst"]
    g.add((subj, RDF.type, IOF_CORE.Manufacturer))

    # Name
    name = None
    is_m = company.get("is_manufacturer")
    if isinstance(is_m, dict) and is_m.get("name"):
        name = is_m.get("name")
    else:
        name = company.get("name") or company.get("Name")
    if name:
        g.add((subj, RDFS_NS.label, Literal(name)))

    # Industries
    inds = company.get("industries")
    industries = inds.get("results", []) if isinstance(inds, dict) else inds or []
    for ind in industries:
        if ind:
            ind_uri = SDK[f"{uri_strip(ind)}-inst"]
            g.add((subj, SDK.suppliesToIndustry, ind_uri))
            class_name = f"{''.join(e for e in ind.title() if e.isalnum())}Industry"
            g.add((ind_uri, RDF.type, SDK[class_name]))
            g.add((ind_uri, RDFS_NS.label, Literal(ind)))

    # Process capabilities
    pcaps = company.get("process_caps")
    process_caps = pcaps.get("results", []) if isinstance(pcaps, dict) else pcaps or []
    for pc in process_caps:
        if pc:
            gid_short = gid_stripped.split("_")[0]
            pc_uri = SDK[f"{gid_short}-{uri_strip(pc)}-inst"]
            class_name = f"{''.join(e for e in pc.title() if e.isalnum())}Capability"
            g.add((pc_uri, RDF.type, SDK[class_name]))
            g.add((pc_uri, RDFS_NS.label, Literal(pc)))
            g.add((subj, SDK.hasProcessCapability, pc_uri))

    # Conformity attestations
    certs_dict = company.get("conformity_attestations")
    certs = certs_dict.get("results", []) if isinstance(certs_dict, dict) else []
    for cert in certs:
        if cert:
            cert_uri = SDK[f"{gid_stripped}-{uri_strip(cert)}"]
            g.add((cert_uri, RDF.type, SDK[uri_strip(cert)]))
            g.add((cert_uri, RDFS_NS.label, Literal(cert)))
            g.add((subj, SDK.hasConformityAttestation, cert_uri))

    # Material capabilities
    mcaps = company.get("material_caps")
    mat_caps = mcaps.get("results", []) if isinstance(mcaps, dict) else mcaps or []
    for mc in mat_caps:
        if mc:
            mc_uri = SDK[f"{gid_stripped}-{uri_strip(mc)}-inst"]
            class_name = (
                f"{''.join(e for e in mc.title() if e.isalnum())}ProcessingCapability"
            )
            g.add((mc_uri, RDF.type, SDK[class_name]))
            g.add((mc_uri, RDFS_NS.label, Literal(mc)))
            g.add((subj, SDK.hasMaterialCapability, mc_uri))

    # Business statuses
    bs = company.get("business_statuses")
    statuses = bs.get("results", []) if isinstance(bs, dict) else bs or []
    for stat in statuses:
        if stat:
            stat_uri = SDK[f"{uri_strip(stat)}-inst"]
            class_name = (
                f"{''.join(e for e in stat.title() if e.isalnum())}BusinessStatus"
            )
            g.add((stat_uri, RDF.type, SDK[class_name]))
            g.add((stat_uri, RDFS_NS.label, Literal(stat)))
            g.add((subj, SDK.hasOwnershipStatusClassifier, stat_uri))

    # NAICS
    naics_list = company.get("naics") or []
    for naics in naics_list:
        if not naics or not isinstance(naics, dict):
            continue
        code = naics.get("code")
        desc = naics.get("desc")
        if code:
            naics_uri = SDK[f"naics-{code}-inst"]
            class_uri = SDK[f"NAICS{code}"]
            g.add((naics_uri, RDF.type, class_uri))
            g.add((subj, SDK.hasPrimaryNAICSClassifier, naics_uri))
            if desc:
                g.add((naics_uri, SDK.hasNAICSTextValue, Literal(desc)))
            try:
                g.add(
                    (
                        naics_uri,
                        SDK.hasNAICSCodeValue,
                        Literal(str(float(code)), datatype=XSD.string),
                    )
                )
            except Exception:
                pass

    # Employee count
    num_employees = company.get("num_employees") or company.get("Employees")
    if num_employees is not None:
        g.add(
            (
                subj,
                SDK.hasNumberOfEmployees,
                Literal(str(num_employees), datatype=XSD.string),
            )
        )

    # Products
    product_uris = []
    products = []
    prods = company.get("products_old") or company.get("products")
    if prods and isinstance(prods, dict):
        products = prods.get("results", [])
    elif prods and isinstance(prods, list):
        products = prods

    for prod in products or []:
        if prod:
            gid_short = gid_stripped.split("_")[0]
            prod_uri = SDK[f"{gid_short}-{uri_strip(prod)}-inst"]
            class_name = f"{''.join(e for e in prod.title() if e.isalnum())}Product"
            g.add((prod_uri, RDF.type, SDK[class_name]))
            g.add((prod_uri, RDFS_NS.label, Literal(prod)))
            g.add((subj, SDK.manufactures, prod_uri))
            product_uris.append(prod_uri)

    # Addresses - assume location data is embedded in JSON
    addresses = company.get("addresses", [])
    for i, addr in enumerate(addresses):
        if not addr:
            continue

        addr_uri = SDK[f"{gid_stripped}-addr-{i}-inst"]
        g.add((addr_uri, RDF.type, IOF_CORE.Address))
        g.add((subj, SDK.hasAddress, addr_uri))

        line1 = addr.get("line_1") or addr.get("line1")
        if line1:
            g.add((addr_uri, SDK.hasAddressLine1, Literal(line1)))

        city = addr.get("city")
        if city:
            g.add((addr_uri, SDK.hasCity, Literal(city)))

        state = addr.get("state")
        if state:
            g.add((addr_uri, SDK.hasState, Literal(state)))

        # Use embedded zip code or lookup from assumed JSON data
        zip_code = addr.get("zip") or addr.get("postal_code")
        if not zip_code:
            lat = addr.get("latitude")
            lon = addr.get("longitude")
            zip_code = lookup_zip(line1, city, state, lat, lon)
        if zip_code and zip_code != "unknown":
            g.add((addr_uri, SDK.hasZipCode, Literal(zip_code)))

        # Handle embedded location identifiers in JSON
        if city and state:
            city_geoid = addr.get("city_geoid") or lookup_city_geoid(city, state)
            if city_geoid and city_geoid != "unknown":
                city_uri = SDK[f"city-{city_geoid}-inst"]
                g.add((city_uri, RDF.type, SDK.City))
                g.add((city_uri, RDFS_NS.label, Literal(city)))
                g.add((city_uri, SDK.hasGeoID, Literal(city_geoid)))
                g.add((addr_uri, SDK.locatedInCity, city_uri))

            state_geoid = addr.get("state_geoid") or resolve_state(state)
            if state_geoid and state_geoid != "unknown":
                state_uri = SDK[f"state-{state_geoid}-inst"]
                g.add((state_uri, RDF.type, SDK.State))
                g.add((state_uri, RDFS_NS.label, Literal(state)))
                g.add((state_uri, SDK.hasGeoID, Literal(state_geoid)))
                g.add((addr_uri, SDK.locatedInState, state_uri))

        # Latitude and longitude from JSON
        lat = addr.get("latitude")
        lon = addr.get("longitude")
        if lat is not None:
            g.add((addr_uri, SDK.hasLatitude, Literal(str(lat), datatype=XSD.float)))
        if lon is not None:
            g.add((addr_uri, SDK.hasLongitude, Literal(str(lon), datatype=XSD.float)))


# Main processing - read from JSON file only
def main():
    start_time = time.time()

    # Assume all required data is embedded in the JSON file
    # Load any supplementary data from JSON if available
    try:
        with open("location_data.json", "r", encoding="utf-8") as f:
            location_data = json.load(f)
            CITY_GEOID_MAP.update(location_data.get("city_geoid_map", {}))
            STATE_DCIDS.update(location_data.get("state_dcids", {}))
            ZIP_CACHE.update(location_data.get("zip_cache", {}))
    except FileNotFoundError:
        print("location_data.json not found, using empty location data")

    # Process companies from JSON
    with open("sample2.json", encoding="utf-8") as f:
        for company in ijson.items(f, "item"):
            enrich_company_rdf(
                company,
                g,
                SDK,
                IOF_CORE,
                IOF_SCRO,
                RDFS_NS,
                XSD_NS,
                CITY_GEOID_MAP,
                STATE_DCIDS,
            )

    # Save the RDF graph
    with open("output-new.ttl", "w", encoding="utf-8") as f:
        f.write(g.serialize(format="turtle"))

    elapsed = time.time() - start_time
    print(f"TTL generation completed in {elapsed:.2f} seconds")
    print(f"Generated {len(g)} triples")


if __name__ == "__main__":
    main()
