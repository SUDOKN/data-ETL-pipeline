from rdflib import URIRef
from rdflib import Namespace

from data_etl_app.utils.ttl_generator_util import uri_strip

SDK = Namespace("http://asu.edu/semantics/SUDOKN/")


def get_product_instance_uri(mfg_etld1_stripped: str, product_name: str) -> URIRef:
    return SDK[f"{mfg_etld1_stripped}-{uri_strip(product_name)}-product-instance"]


def get_mfg_instance_uri_and_stripped_etld1(mfg_etld1: str) -> tuple[URIRef, str]:
    stripped_etld1 = uri_strip(mfg_etld1)
    return SDK[f"{stripped_etld1}-company-instance"], stripped_etld1
