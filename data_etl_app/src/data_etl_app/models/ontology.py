from pydantic import BaseModel, ConfigDict


class Ontology(BaseModel):
    model_config = ConfigDict(frozen=True)

    s3_version_id: str
    rdf: str
