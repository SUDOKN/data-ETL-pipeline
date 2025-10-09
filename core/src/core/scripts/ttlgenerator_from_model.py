#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
from typing import Optional
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, XSD
from pathlib import Path

from core.dependencies.load_core_env import load_core_env
from scraper_app.dependencies.load_scraper_env import load_scraper_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_scraper_env()
load_data_etl_env()
load_open_ai_app_env()

from core.utils.mongo_client import init_db
from core.dependencies.aws_clients import (
    cleanup_core_aws_clients,
    initialize_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    cleanup_data_etl_aws_clients,
    initialize_data_etl_aws_clients,
)

from data_etl_app.models.ontology import Ontology
from data_etl_app.services.knowledge.ontology_service import OntologyService
from core.models.db.manufacturer import Address, BusinessDescriptionResult
from data_etl_app.utils.rdf_to_graph_util import uri_strip
from data_etl_app.models.skos_concept import Concept
from data_etl_app.models.db.manufacturer_user_form import (
    ManufacturerUserForm,
)

logger = logging.getLogger(__name__)

# --- RDF NAMESPACE SETUP ---
SDK = Namespace("http://asu.edu/semantics/SUDOKN/")
IOF_CORE = Namespace("https://spec.industrialontologies.org/ontology/core/Core/")
IOF_SCRO = Namespace(
    "https://spec.industrialontologies.org/ontology/supplychain/SupplyChain/"
)
XSD_NS = Namespace("http://www.w3.org/2001/XMLSchema#")
RDFS_NS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
BFO = Namespace("http://purl.obolibrary.org/obo/")


def get_ownership_status_concept(ont_inst: OntologyService, label: str) -> Concept:
    concept = ont_inst.ownership_status_map[1].get(label)
    if not concept:
        raise ValueError(f"Ownership status '{label}' not found in ontology.")
    return concept


def get_naics_concept(ont_inst: OntologyService, code: str) -> Concept:
    concept = ont_inst.naics_code_map[1].get(code)
    if not concept:
        raise ValueError(f"NAICS code '{code}' not found in ontology.")
    return concept


def get_certificate_concept(ont_inst: OntologyService, label: str) -> Concept:
    concept = ont_inst.certificate_map[1].get(label)
    if not concept:
        raise ValueError(f"Certificate '{label}' not found in ontology.")
    return concept


def get_industry_concept(ont_inst: OntologyService, label: str) -> Concept:
    # print("Looking up industry concept for label:", label)
    # print(type(ont_inst.industry_map))
    # print(type(ont_inst.industry_map[1]))
    concept = ont_inst.industry_map[1].get(label)
    if not concept:
        raise ValueError(f"Industry '{label}' not found in ontology.")
    return concept


def get_process_cap_concept(ont_inst: OntologyService, label: str) -> Concept:
    concept = ont_inst.process_cap_map[1].get(label)
    if not concept:
        raise ValueError(f"Process capability '{label}' not found in ontology.")
    return concept


def get_material_cap_concept(ont_inst: OntologyService, label: str) -> Concept:
    concept = ont_inst.material_cap_map[1].get(label)
    if not concept:
        raise ValueError(f"Material capability '{label}' not found in ontology.")
    return concept


def add_mfg_name_triple(mfg_inst_uri: URIRef, mfg_name: Optional[str], g: Graph):
    if not mfg_name:
        raise ValueError("Manufacturer name cannot be empty")

    print(f"  with name: {mfg_name}")
    g.add((mfg_inst_uri, RDFS_NS.label, Literal(mfg_name)))


def add_founded_in_triple(mfg_inst_uri: URIRef, founded_in: Optional[int], g: Graph):
    if not founded_in:
        print(f"  skipping empty founded in year")
        return

    print(f"  founded in: {founded_in}")
    g.add(
        (
            mfg_inst_uri,
            SDK.hasOrganizationYearOfEstablishment,
            Literal(int(founded_in), datatype=XSD.int),
        )
    )


def add_email_addresses_triples(
    mfg_inst_uri: URIRef,
    email_addresses: Optional[list[str]],
    gid_stripped: str,
    g: Graph,
):
    if not email_addresses:
        print(f"  skipping empty email addresses")
        return

    for email in email_addresses:
        if not email:
            raise ValueError("Email address cannot be empty")
        print(f"  with email: {email}")
        # Create an EmailAddress individual
        email_inst_uri = SDK[f"{gid_stripped}-email-{uri_strip(email)}-inst"]
        g.add((email_inst_uri, RDF.type, SDK.EmailAddress))
        g.add((email_inst_uri, SDK.hasVirtualLocationIdentifierValue, Literal(email)))
        # link it
        g.add((mfg_inst_uri, SDK.hasEmailAddress, email_inst_uri))


def add_number_of_employees_triple(
    mfg_inst_uri: URIRef, num_employees: Optional[int], g: Graph
):
    if num_employees is None:
        print(f"  skipping empty number of employees")
        return

    print(f"  with number of employees: {num_employees}")
    g.add(
        (
            mfg_inst_uri,
            SDK.hasNumberOfEmployees,
            Literal(int(num_employees), datatype=XSD.int),
        )
    )


def add_business_status_triples(
    mfg_inst_uri: URIRef,
    ont_inst: OntologyService,
    status_labels: Optional[list[str]],
    g: Graph,
):
    if not status_labels:
        print(f"  skipping empty business ownership status")
        return
    for status_label in status_labels:
        if not status_label:
            raise ValueError("Business ownership status cannot be empty")
        print(f"  with ownership status: {status_label}")
        status_concept = get_ownership_status_concept(ont_inst, status_label)
        status_inst_uri = SDK[
            f"{uri_strip(status_concept.name)}-ownership-status-individual"
        ]
        g.add((status_inst_uri, RDF.type, status_concept.uri))
        g.add((mfg_inst_uri, SDK.hasOwnershipStatusClassifier, status_inst_uri))


def add_primary_naics_triple(
    mfg_inst_uri: URIRef,
    primary_naics: Optional[str],
    ont_inst: OntologyService,
    g: Graph,
):
    if not primary_naics:
        print(f"  skipping empty primary NAICS")
        return
    print(f"  with primary NAICS: {primary_naics}")
    naics_concept = get_naics_concept(ont_inst, "NAICS " + primary_naics)
    naics_ind_uri = SDK[f"{uri_strip(naics_concept.name)}-individual"]
    g.add((naics_ind_uri, RDF.type, naics_concept.uri))
    g.add((mfg_inst_uri, SDK.hasPrimaryNAICSClassifier, naics_ind_uri))


def add_secondary_naics_triple(
    mfg_inst_uri: URIRef,
    secondary_naics: Optional[list[str]],
    ont_inst: OntologyService,
    g: Graph,
):
    if not secondary_naics:
        print(f"  skipping empty secondary NAICS")
        return
    for naics_label in secondary_naics:
        if not naics_label:
            raise ValueError("Secondary NAICS code cannot be empty")
        print(f"  with secondary NAICS: {naics_label}")
        naics_concept = get_naics_concept(ont_inst, "NAICS " + naics_label)
        naics_ind_uri = SDK[f"{uri_strip(naics_concept.name)}-individual"]
        g.add((naics_ind_uri, RDF.type, naics_concept.uri))
        g.add((mfg_inst_uri, SDK.hasSecondaryNAICSClassifier, naics_ind_uri))


def add_address_triples(
    mfg_inst_uri: URIRef,
    addresses: Optional[list[Address]],
    gid_stripped: str,
    g: Graph,
):
    if not addresses:
        raise ValueError("Manufacturer must have at least one address")

    for i, addr in enumerate(addresses):
        if not addr:
            raise ValueError("Address cannot be empty")
        print(f"  with address: {addr}")
        # Create GeospatialSite
        geosite_inst_uri = SDK[f"{gid_stripped}-geosite-{i+1}-inst"]
        g.add((geosite_inst_uri, RDF.type, SDK.GeospatialSite))
        if addr.name:
            g.add(
                (geosite_inst_uri, RDFS_NS.label, Literal(addr.name))
            )  # link name to site

        # Address lines
        for idx, addr_line in enumerate(addr.address_lines or []):
            if addr_line:
                address_line_inst_uri = SDK[f"{gid_stripped}-address-line-{idx+1}-inst"]
                g.add((address_line_inst_uri, RDF.type, SDK.AddressLine))
                g.add(
                    (address_line_inst_uri, IOF_SCRO.hasTextValue, Literal(addr_line))
                )
                g.add(
                    (
                        address_line_inst_uri,
                        SDK.hasOrder,
                        Literal(idx + 1, datatype=XSD.int),
                    )
                )
                g.add(
                    (geosite_inst_uri, SDK.hasAddressLine, address_line_inst_uri)
                )  # link address line to site

        # City
        city_ind_uri = SDK[f"{uri_strip(addr.city)}-city-individual"]
        g.add((city_ind_uri, RDF.type, SDK.City))
        g.add((city_ind_uri, RDFS_NS.label, Literal(addr.city)))
        g.add((geosite_inst_uri, SDK.locatedInCity, city_ind_uri))  # link city to site

        # State
        state_ind_uri = SDK[f"{uri_strip(addr.state)}-state-individual"]
        g.add((state_ind_uri, RDF.type, SDK.State))
        g.add((state_ind_uri, RDFS_NS.label, Literal(addr.state)))
        g.add(
            (geosite_inst_uri, SDK.locatedInState, state_ind_uri)
        )  # link state to site

        # County - only if available
        if addr.county:
            county_ind_uri = SDK[f"{uri_strip(addr.county)}-county-individual"]
            g.add((county_ind_uri, RDF.type, SDK.County))
            g.add((county_ind_uri, RDFS_NS.label, Literal(addr.county)))
            g.add(
                (geosite_inst_uri, SDK.locatedInCounty, county_ind_uri)
            )  # link county to site

        # Postal Code
        g.add(
            (geosite_inst_uri, SDK.hasZipcodeValue, Literal(addr.postal_code))
        )  # link zipcode to site

        # Country
        country_ind_uri = SDK[f"{uri_strip(addr.country)}-country-individual"]
        g.add((country_ind_uri, RDF.type, SDK.Country))
        g.add((country_ind_uri, RDFS_NS.label, Literal(addr.country)))
        g.add(
            (geosite_inst_uri, SDK.locatedInCountry, country_ind_uri)
        )  # link country to site

        # GeospatialLocation for coordinates
        if addr.latitude is None or addr.longitude is None:
            raise ValueError(
                "Both latitude and longitude must be provided for an address"
            )
        elif not (-90 <= addr.latitude <= 90):
            raise ValueError("Latitude must be between -90 and 90 degrees")
        elif not (-180 <= addr.longitude <= 180):
            raise ValueError("Longitude must be between -180 and 180 degrees")

        geoloc_inst_uri = SDK[f"{gid_stripped}-geolocation-{i+1}-inst"]
        g.add(
            (geoloc_inst_uri, RDF.type, IOF_SCRO.GeospatialLocation)
        )  # GeospatialLocation
        g.add(
            (
                geoloc_inst_uri,
                SDK.hasLatitudeValue,
                Literal(addr.latitude, datatype=XSD.float),
            )
        )
        g.add(
            (
                geoloc_inst_uri,
                SDK.hasLongitudeValue,
                Literal(addr.longitude, datatype=XSD.float),
            )
        )
        g.add(
            (geosite_inst_uri, SDK.hasGeospatialLocation, geoloc_inst_uri)
        )  # link location to site

        for phone in addr.phone_numbers or []:
            if phone:
                g.add((geosite_inst_uri, SDK.hasPhoneNumberValue, Literal(phone)))
        for fax in addr.fax_numbers or []:
            if fax:
                g.add((geosite_inst_uri, SDK.hasFaxNumberValue, Literal(fax)))

        g.add(
            (mfg_inst_uri, SDK.organizationLocatedIn, geosite_inst_uri)
        )  # link site to manufacturer


def add_business_description_triples(
    mfg_inst_uri: URIRef,
    business_desc: Optional[BusinessDescriptionResult],
    gid_stripped: str,
    g: Graph,
):
    if not business_desc or not business_desc.description:
        print(f"  skipping empty business description")
        return
    print(f"  with business description: {business_desc.description}")
    desc_inst_uri = SDK[f"{gid_stripped}-business-description-inst"]
    g.add((desc_inst_uri, RDF.type, SDK.BusinessDescription))
    g.add(
        (
            desc_inst_uri,
            IOF_SCRO.hasTextValue,
            Literal(business_desc.description),
        )
    )
    g.add((mfg_inst_uri, SDK.hasBusinessDescription, desc_inst_uri))


def add_product_triples(
    mfg_inst_uri: URIRef, products: Optional[list[str]], gid_stripped: str, g: Graph
):
    if not products:
        print(f"  skipping empty products")
        return

    for prod in products:
        if not prod:
            raise ValueError("Product name cannot be empty")
        print(f"  with product: {prod}")
        prod_inst_uri = SDK[
            f"{gid_stripped}-{uri_strip(prod)}-product-inst"
        ]  # Changed to match your example

        # Add product as MaterialProduct
        g.add((prod_inst_uri, RDF.type, IOF_CORE.MaterialProduct))
        g.add((prod_inst_uri, RDFS_NS.label, Literal(prod)))
        g.add((mfg_inst_uri, SDK.manufactures, prod_inst_uri))


def add_certificate_triples(
    mfg_inst_uri: URIRef,
    certificates: Optional[list[str]],
    ont_inst: OntologyService,
    g: Graph,
):
    if not certificates:
        print(f"  skipping empty certificates")
        return

    for cert in certificates:
        if not cert:
            raise ValueError("Certificate name cannot be empty")
        print(f"  with certificate: {cert}")
        cert_concept = get_certificate_concept(ont_inst, cert)
        cert_ind_uri = SDK[f"{uri_strip(cert_concept.name)}-certificate-individual"]
        g.add((cert_ind_uri, RDF.type, cert_concept.uri))
        # g.add((cert_ind_uri, RDFS_NS.label, Literal(cert_concept.name)))
        g.add((mfg_inst_uri, SDK.hasCertificate, cert_ind_uri))


def add_industry_triples(
    mfg_inst_uri: URIRef,
    industries: Optional[list[str]],
    ont_inst: OntologyService,
    g: Graph,
):
    if not industries:
        print(f"  skipping empty industries")
        return

    for ind in industries:
        if not ind:
            raise ValueError("Industry cannot be empty")
        print(f"  with industry: {ind}")
        ind_concept = get_industry_concept(ont_inst, ind)
        industry_ind_uri = SDK[f"{uri_strip(ind_concept.name)}-industry-individual"]
        g.add((industry_ind_uri, RDF.type, ind_concept.uri))
        # g.add((industry_ind_uri, RDFS_NS.label, Literal(ind_concept.name)))
        g.add((mfg_inst_uri, SDK.suppliesToIndustry, industry_ind_uri))


def add_process_capability_triples(
    mfg_inst_uri: URIRef,
    gid_stripped: str,
    process_caps: Optional[list[str]],
    ont_inst: OntologyService,
    g: Graph,
):
    if not process_caps:
        print(f"  skipping empty process capabilities")
        return

    for pc in process_caps or []:
        if not pc:
            raise ValueError("Process capability cannot be empty")
        print(f"  with process capability: {pc}")
        pc_concept = get_process_cap_concept(ont_inst, pc)
        pc_inst_uri = SDK[
            f"{gid_stripped}-{uri_strip(pc_concept.name)}-process-capability-inst"
        ]
        g.add((pc_inst_uri, RDF.type, pc_concept.uri))
        g.add((mfg_inst_uri, SDK.hasProcessCapability, pc_inst_uri))


def add_material_capability_triples(
    mfg_inst_uri: URIRef,
    gid_stripped: str,
    material_caps: Optional[list[str]],
    ont_inst: OntologyService,
    g: Graph,
):
    if not material_caps:
        print(f"  skipping empty material capabilities")
        return

    for mc in material_caps or []:
        if not mc:
            raise ValueError("Material capability cannot be empty")
        print(f"  with material capability: {mc}")
        mc_concept = get_material_cap_concept(ont_inst, mc)
        mc_inst_uri = SDK[
            f"{gid_stripped}-{uri_strip(mc_concept.name)}-material-capability-inst"
        ]
        g.add((mc_inst_uri, RDF.type, mc_concept.uri))
        g.add((mfg_inst_uri, SDK.hasMaterialCapability, mc_inst_uri))


def enrich_manufacturer_rdf(ont_inst: OntologyService, mfg: ManufacturerUserForm, g):
    gid_val = str(mfg.mfg_etld1)
    if not gid_val:
        raise ValueError("ManufacturerUserForm must have a valid mfg_etld1")

    gid_stripped = uri_strip(gid_val)
    mfg_inst_uri = SDK[f"{gid_stripped}-company-inst"]
    print(f"Generating triples for {mfg_inst_uri}")

    g.add((mfg_inst_uri, RDF.type, IOF_CORE.Manufacturer))

    # Name
    add_mfg_name_triple(
        mfg_inst_uri,
        mfg.name or mfg.business_desc.name if mfg.business_desc else None,
        g,
    )
    add_founded_in_triple(mfg_inst_uri, mfg.founded_in, g)
    add_email_addresses_triples(mfg_inst_uri, mfg.email_addresses, gid_stripped, g)
    add_number_of_employees_triple(mfg_inst_uri, mfg.num_employees, g)
    add_business_status_triples(mfg_inst_uri, ont_inst, mfg.business_statuses, g)
    add_primary_naics_triple(mfg_inst_uri, mfg.primary_naics, ont_inst, g)
    add_secondary_naics_triple(mfg_inst_uri, mfg.secondary_naics, ont_inst, g)
    add_address_triples(mfg_inst_uri, mfg.addresses, gid_stripped, g)
    add_business_description_triples(mfg_inst_uri, mfg.business_desc, gid_stripped, g)
    add_product_triples(mfg_inst_uri, mfg.products, gid_stripped, g)
    add_certificate_triples(mfg_inst_uri, mfg.certificates, ont_inst, g)
    add_industry_triples(mfg_inst_uri, mfg.industries, ont_inst, g)
    add_process_capability_triples(
        mfg_inst_uri, gid_stripped, mfg.process_caps, ont_inst, g
    )
    add_material_capability_triples(
        mfg_inst_uri, gid_stripped, mfg.material_caps, ont_inst, g
    )


def generate_triples(
    ont_inst: OntologyService, manufacturers: list[ManufacturerUserForm]
):

    g = Graph()
    g.bind("sdk", SDK)
    g.bind("iof-core", IOF_CORE)
    g.bind("iof-scro", IOF_SCRO)
    g.bind("xsd", XSD_NS)
    g.bind("rdfs", RDFS_NS)
    g.bind("bfo", BFO)

    for mfg in manufacturers:
        enrich_manufacturer_rdf(ont_inst, mfg, g)

    with open("output.ttl", "w", encoding="utf-8") as f:
        f.write(g.serialize(format="turtle"))
    print(f"Generated {len(g)} triples from ManufacturerUserForm model.")


async def main():
    try:
        await init_db()

        # Initialize AWS clients
        await initialize_core_aws_clients()
        await initialize_data_etl_aws_clients()
        from data_etl_app.services.knowledge.ontology_service import (
            get_ontology_service,
        )

        ontology_file_path = (
            Path(__file__).resolve().parents[4] / "ontology/SUDOKN1.1/SUDOKN1_1.rdf"
        )

        print(f"Ontology file path: {ontology_file_path}")
        with open(ontology_file_path, "r", encoding="utf-8") as file:
            ontology_data = file.read()

        ont_inst = await get_ontology_service(
            Ontology(rdf=ontology_data, s3_version_id="local-test-version-id")
        )
        print("Ontology service initialized.")
        manufacturers = [
            ManufacturerUserForm(
                author_email="info@acmemfg.com",
                mfg_etld1="mfg-001",
                name="Acme Manufacturing",
                founded_in=1990,
                email_addresses=["info@acmemfg.com"],
                num_employees=150,
                business_statuses=["Disabled Veteran Owned"],
                primary_naics="332710",
                secondary_naics=["332312", "332313"],
                addresses=[
                    Address(
                        name="Main Plant",
                        address_lines=["123 Industrial Way"],
                        city="Metropolis",
                        state="CA",
                        county="Los Angeles",
                        postal_code="90001",
                        country="USA",
                        latitude=34.0522,
                        longitude=-118.2437,
                        phone_numbers=["+1-555-1234"],
                        fax_numbers=["+1-555-5678"],
                    )
                ],
                business_desc=BusinessDescriptionResult(
                    name="Acme Manufacturing",
                    description="Leading manufacturer of industrial components.",
                ),
                products=["Gears", "Sprockets"],
                certificates=["ISO 9001", "AS 9100"],
                industries=["Aerospace", "Automotive"],
                process_caps=["CNC Machining", "Gas Welding"],
                material_caps=["Aluminum", "Steel"],
                notes="Top-tier manufacturer with a focus on quality.",
            )
        ]
        print(f"Loaded {len(manufacturers)} manufacturers for RDF generation.")
        generate_triples(ont_inst, manufacturers)

    finally:
        # Cleanup AWS clients
        await cleanup_core_aws_clients()
        await cleanup_data_etl_aws_clients()


if __name__ == "__main__":
    try:
        print("Starting RDF validation script...")
        asyncio.run(main())

    except Exception as e:
        print(f"Error during validation: {e}")
        sys.exit(1)
    print("RDF validation completed successfully.")
    sys.exit(0)
