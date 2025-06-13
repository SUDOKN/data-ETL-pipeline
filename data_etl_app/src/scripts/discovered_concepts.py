"""
This script is used to extract new concepts that were discovered in the extraction process.
"""

import asyncio
import json

from models.skos_concept import Concept
from services.ontology_service import ontology_service
from utils.mongo_client import (
    init_db,
    Manufacturer,
)


class DiscoveredConcepts:
    def __init__(self):
        self.process_capabilities = {
            "mapped": self._flat_concepts_to_dict(
                ontology_service.process_capabilities
            ),
            "unmapped": set(),
        }
        self.material_capabilities = {
            "mapped": self._flat_concepts_to_dict(
                ontology_service.material_capabilities
            ),
            "unmapped": set(),
        }
        self.industries = {
            "mapped": self._flat_concepts_to_dict(ontology_service.industries),
            "unmapped": set(),
        }
        self.certificates = {
            "mapped": self._flat_concepts_to_dict(ontology_service.certificates),
            "unmapped": set(),
        }

    def _flat_concepts_to_dict(self, concepts: list[Concept]):
        concept_map = {}
        for concept in concepts:
            concept_map[concept.name] = {
                "name": concept.name,
                "altLabels": concept.altLabels.copy(),
                "newLabels": {},
            }
        return concept_map


class DiscoveredConceptsJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DiscoveredConcepts):

            def sort_new_labels(new_labels_dict):
                # Sort the dictionary by value (frequency) in decreasing order
                return dict(
                    sorted(
                        new_labels_dict.items(), key=lambda item: item[1], reverse=True
                    )
                )

            return {
                "process_capabilities": {
                    "mapped": {
                        k: {
                            "name": v["name"],
                            "altLabels": list(v["altLabels"]),
                            "newLabels": sort_new_labels(v["newLabels"]),
                        }
                        for k, v in obj.process_capabilities["mapped"].items()
                    },
                    "unmapped": list(obj.process_capabilities["unmapped"]),
                },
                "material_capabilities": {
                    "mapped": {
                        k: {
                            "name": v["name"],
                            "altLabels": list(v["altLabels"]),
                            "newLabels": sort_new_labels(v["newLabels"]),
                        }
                        for k, v in obj.material_capabilities["mapped"].items()
                    },
                    "unmapped": list(obj.material_capabilities["unmapped"]),
                },
                "industries": {
                    "mapped": {
                        k: {
                            "name": v["name"],
                            "altLabels": list(v["altLabels"]),
                            "newLabels": sort_new_labels(v["newLabels"]),
                        }
                        for k, v in obj.industries["mapped"].items()
                    },
                    "unmapped": list(obj.industries["unmapped"]),
                },
                "certificates": {
                    "mapped": {
                        k: {
                            "name": v["name"],
                            "altLabels": list(v["altLabels"]),
                            "newLabels": sort_new_labels(v["newLabels"]),
                        }
                        for k, v in obj.certificates["mapped"].items()
                    },
                    "unmapped": list(obj.certificates["unmapped"]),
                },
            }
        return super().default(obj)


async def fetch_discovered_concepts() -> DiscoveredConcepts:
    corrupt_mfgs = []
    discovered_concepts = DiscoveredConcepts()
    projection = {
        "process_caps.stats.mapping": 1,
        "process_caps.stats.unmapped_llm": 1,
        "material_caps.stats.mapping": 1,
        "material_caps.stats.unmapped_llm": 1,
        "industries.stats.mapping": 1,
        "industries.stats.unmapped_llm": 1,
        "certificates.stats.mapping": 1,
        "certificates.stats.unmapped_llm": 1,
        "_id": 1,  # Exclude MongoDB's default _id field if not needed
        "url": 1,
        "name": 1,
    }
    collection = Manufacturer.get_motor_collection()
    cursor = collection.find({"is_manufacturer.answer": True}, projection)
    count = 0
    async for doc in cursor:
        count += 1
        # print(f"Processing document {count}")
        process_caps = doc.get("process_caps", {})
        material_caps = doc.get("material_caps", {})
        industries = doc.get("industries", {})
        certificates = doc.get("certificates", {})

        for cap, new_labels in process_caps.get("stats", {}).get("mapping", {}).items():
            if cap in discovered_concepts.process_capabilities["mapped"]:
                # increment count for each new label
                label_counts = discovered_concepts.process_capabilities["mapped"][cap][
                    "newLabels"
                ]
                for lbl in new_labels:
                    label_counts[lbl] = label_counts.get(lbl, 0) + 1
                # print(cap)
                # print(discovered_concepts.process_capabilities["mapped"][cap])
            else:
                # if cap not in ["Vacuum Forming", "Mechanical"]:
                #     raise ValueError(
                #         f"Unexpected process capability: {cap} for {doc['name']} ({doc['url']})"
                #     )
                corrupt_mfgs.append(
                    {
                        "reason": "Unexpected process capability",
                        "capability": cap,
                        "manufacturer": {
                            "name": doc["name"],
                            "url": doc["url"],
                        },
                    }
                )
        for cap in process_caps.get("stats", {}).get("unmapped_llm", []):
            discovered_concepts.process_capabilities["unmapped"].add(cap)

        for cap, new_labels in (
            material_caps.get("stats", {}).get("mapping", {}).items()
        ):
            if cap in discovered_concepts.material_capabilities["mapped"]:
                # increment count for each new label
                label_counts = discovered_concepts.material_capabilities["mapped"][cap][
                    "newLabels"
                ]
                for lbl in new_labels:
                    label_counts[lbl] = label_counts.get(lbl, 0) + 1
            else:
                # raise ValueError(
                #     f"Unexpected material capability: {cap} for {doc['name']} ({doc['url']})"
                # )
                corrupt_mfgs.append(
                    {
                        "reason": "Unexpected material capability",
                        "capability": cap,
                        "manufacturer": {
                            "name": doc["name"],
                            "url": doc["url"],
                        },
                    }
                )
        for cap in material_caps.get("stats", {}).get("unmapped_llm", []):
            discovered_concepts.material_capabilities["unmapped"].add(cap)

        for industry, new_labels in (
            industries.get("stats", {}).get("mapping", {}).items()
        ):
            if industry in discovered_concepts.industries["mapped"]:
                # increment count for each new label
                label_counts = discovered_concepts.industries["mapped"][industry][
                    "newLabels"
                ]
                for lbl in new_labels:
                    label_counts[lbl] = label_counts.get(lbl, 0) + 1
            else:
                # if industry not in [
                #     "water treatment and management",
                #     "energy production",
                #     "food and beverage",
                #     "healthcare services",
                # ]:
                #     raise ValueError(
                #         f"Unexpected industry: {industry} for {doc['name']} ({doc['url']})"
                #     )
                corrupt_mfgs.append(
                    {
                        "reason": "Unexpected industry",
                        "industry": industry,
                        "manufacturer": {
                            "name": doc["name"],
                            "url": doc["url"],
                        },
                    }
                )
        for industry in industries.get("stats", {}).get("unmapped_llm", []):
            discovered_concepts.industries["unmapped"].add(industry)

        for cert, new_labels in (
            certificates.get("stats", {}).get("mapping", {}).items()
        ):
            if cert in discovered_concepts.certificates["mapped"]:
                # increment count for each new label
                label_counts = discovered_concepts.certificates["mapped"][cert][
                    "newLabels"
                ]
                for lbl in new_labels:
                    label_counts[lbl] = label_counts.get(lbl, 0) + 1
            else:
                # raise ValueError(
                #     f"Unexpected certificate: {cert} for {doc['name']} ({doc['url']})"
                # )
                corrupt_mfgs.append(
                    {
                        "reason": "Unexpected certificate",
                        "certificate": cert,
                        "manufacturer": {
                            "name": doc["name"],
                            "url": doc["url"],
                        },
                    }
                )
        for cert in certificates.get("stats", {}).get("unmapped_llm", []):
            discovered_concepts.certificates["unmapped"].add(cert)

        # print(discovered_concepts.process_capabilities["mapped"]["Fabricating"])
        # print(discovered_concepts.process_capabilities["mapped"]["Machining"])
        # break

    with open("corrupt_manufacturers.json", "w") as f:
        f.write(json.dumps(corrupt_mfgs, indent=2))

    return discovered_concepts


async def save_discovered_concepts(discovered_concepts: DiscoveredConcepts):
    dc_json = json.loads(
        json.dumps(discovered_concepts, cls=DiscoveredConceptsJSONEncoder)
    )
    # print(f'dc_json:{dc_json["process_capabilities"]["mapped"]["Fabricating"]}')
    with open("discovered_process_caps.json", "w") as f:
        f.write(json.dumps(dc_json["process_capabilities"]["mapped"]))
    with open("discovered_process_caps_unmapped.json", "w") as f:
        f.write(json.dumps(list(dc_json["process_capabilities"]["unmapped"])))

    with open("discovered_material_caps.json", "w") as f:
        f.write(json.dumps(dc_json["material_capabilities"]["mapped"]))
    with open("discovered_material_caps_unmapped.json", "w") as f:
        f.write(json.dumps(list(dc_json["material_capabilities"]["unmapped"])))

    with open("discovered_industries.json", "w") as f:
        f.write(json.dumps(dc_json["industries"]["mapped"]))
    with open("discovered_industries_unmapped.json", "w") as f:
        f.write(json.dumps(list(dc_json["industries"]["unmapped"])))

    with open("discovered_certificates.json", "w") as f:
        f.write(json.dumps(dc_json["certificates"]["mapped"]))
    with open("discovered_certificates_unmapped.json", "w") as f:
        f.write(json.dumps(list(dc_json["certificates"]["unmapped"])))


async def main():
    await init_db()
    dcs = await fetch_discovered_concepts()
    await save_discovered_concepts(dcs)


if __name__ == "__main__":
    asyncio.run(main())
