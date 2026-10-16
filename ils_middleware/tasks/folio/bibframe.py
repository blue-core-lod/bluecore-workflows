"""Maps a FOLIO Inventory Instance to a BIBFRAME Work and Instance and loads
them into Blue Core."""

import logging
import os
import uuid

import httpx
import rdflib
from airflow.sdk import Connection, task
from bibframe_json import CONTEXT_URL
from bluecore_models.bluecore_graph import save_graph
from bluecore_models.models.version import CURRENT_USER_ID
from bluecore_models.namespaces import BF
from bluecore_models.utils.graph import load_jsonld
from folioclient import FolioClient
from jsonpath_ng import parse
from sqlalchemy.orm import sessionmaker

from ils_middleware.tasks.bluecore import get_bluecore_db, get_engine
from ils_middleware.tasks.folio.inventory import lookup_by_local_id
from ils_middleware.tasks.keycloak import get_user_profile

logger = logging.getLogger(__name__)

# Keycloak groups of the institutions with a FOLIO tenant; each has an Airflow
# Connection named "{group}_folio", as the stanford and cornell DAGs use
FOLIO_INSTITUTIONS = ("cornell", "stanford")

# Named by URL rather than inline: load_jsonld only reads a context
# bibframe-json ships, which defines the bf, bflc, rdf and rdfs prefixes used
# below. With no @context at all the bf: keys would not expand to BIBFRAME.
BF_CONTEXT = CONTEXT_URL

# FolioClient reference-data properties needed to resolve FOLIO UUIDs to labels
REFERENCE_DATA_PROPERTIES = (
    "alt_title_types",
    "class_types",
    "contrib_name_types",
    "contributor_types",
    "identifier_types",
    "instance_formats",
    "instance_note_types",
    "instance_types",
    "modes_of_issuance",
)

# FOLIO labels that mean "no value recorded"
PLACEHOLDER_LABELS = {"", "unspecified", "undefined"}

CLASSIFICATION_TYPES: dict[str | None, str] = {
    "LC": "bf:ClassificationLcc",
    "LC (local)": "bf:ClassificationLcc",
    "Dewey": "bf:ClassificationDdc",
    "NLM": "bf:ClassificationNlm",
    "UDC": "bf:ClassificationUdc",
}

IDENTIFIER_TYPES: dict[str | None, str] = {
    "ISBN": "bf:Isbn",
    "ISMN": "bf:Ismn",
    "ISSN": "bf:Issn",
    "LCCN": "bf:Lccn",
    "OCLC": "bf:Oclc",
    "System control number": "bf:Local",
    "UPC": "bf:Upc",
}

AGENT_TYPES: dict[str | None, str] = {
    "Personal name": "bf:Person",
    "Corporate name": "bf:Organization",
}

PROVISION_ACTIVITY_TYPES = {
    "Publication": "bf:Publication",
    "Production": "bf:Production",
    "Distribution": "bf:Distribution",
    "Manufacture": "bf:Manufacture",
    "Copyright notice date": "bf:CopyrightDate",
}

SOURCE_OF_DESCRIPTION_NOTE = "Source of Description note"
SUMMARY_NOTE = "Summary"

# JSONPath expressions into a FOLIO Inventory Instance
ALTERNATIVE_TITLES = parse("$.alternativeTitles[*]")
CLASSIFICATIONS = parse("$.classifications[*]")
CONTRIBUTORS = parse("$.contributors[*]")
EDITIONS = parse("$.editions[*]")
ELECTRONIC_ACCESS = parse("$.electronicAccess[*]")
IDENTIFIERS = parse("$.identifiers[*]")
INSTANCE_FORMAT_IDS = parse("$.instanceFormatIds[*]")
INSTANCE_ID = parse("$.id")
INSTANCE_TYPE_ID = parse("$.instanceTypeId")
LANGUAGES = parse("$.languages[*]")
MODE_OF_ISSUANCE_ID = parse("$.modeOfIssuanceId")
NOTES = parse("$.notes[*]")
PHYSICAL_DESCRIPTIONS = parse("$.physicalDescriptions[*]")
PUBLICATION = parse("$.publication[*]")
SERIES = parse("$.series[*]")
SUBJECTS = parse("$.subjects[*]")
TITLE = parse("$.title")


def folio_institution_for(groups: list) -> str:
    """
    Returns the one FOLIO institution among a user's Keycloak groups
    """
    institutions = [group for group in groups if group in FOLIO_INSTITUTIONS]
    if len(institutions) != 1:
        raise ValueError(
            f"Expected one FOLIO institution in groups {groups}, found {institutions}"
        )
    return institutions[0]


def folio_client(institution: str) -> FolioClient:
    """
    Returns a FOLIO client from the institution's Airflow Connection, whose
    host is the API Gateway URL and whose extra holds the tenant
    """
    connection = Connection.get(f"{institution}_folio")
    return FolioClient(
        connection.host,
        connection.extra_dejson["tenant"],
        connection.login,
        connection.password,
    )


def _values(expression, record: dict) -> list:
    return [match.value for match in expression.find(record)]


def _first(expression, record: dict):
    values = _values(expression, record)
    return values[0] if values else None


def _text(value) -> str | None:
    """
    Subjects and series are plain strings in older FOLIO records and
    {"value": ..., "authorityId": ...} objects in newer ones
    """
    if isinstance(value, dict):
        return value.get("value")
    return value


def _label(reference_data: dict, prop: str, target_id: str | None) -> str | None:
    if target_id is None:
        return None
    return reference_data.get(prop, {}).get(target_id)


def _labeled(label: str | None) -> dict | None:
    if label is None or label.strip().lower() in PLACEHOLDER_LABELS:
        return None
    return {"rdfs:label": label}


def _mint_uri(folio_instance_id: str, kind: str) -> str:
    """
    Deterministic placeholder URI; Blue Core assigns the real URI on ingestion
    """
    return f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_URL, f'folio:{folio_instance_id}:{kind}')}"


def _compact(node: dict) -> dict:
    """Drops properties with no value so they aren't asserted empty"""
    return {key: value for key, value in node.items() if value not in (None, "", [])}


def _public_notes(folio_instance: dict, reference_data: dict) -> list:
    """Returns (note type label, note) pairs, skipping staff-only notes"""
    notes = []
    for note in _values(NOTES, folio_instance):
        if note.get("staffOnly") or not note.get("note"):
            continue
        note_type = _label(
            reference_data, "instance_note_types", note.get("instanceNoteTypeId")
        )
        notes.append((note_type, note["note"]))
    return notes


def _bf_note(note_type: str | None, note: str) -> dict:
    return _compact({"@type": "bf:Note", "bf:noteType": note_type, "rdfs:label": note})


def _titles(folio_instance: dict) -> list:
    titles = []
    main_title = _first(TITLE, folio_instance)
    if main_title:
        titles.append({"@type": "bf:Title", "bf:mainTitle": main_title})
    for alternative in _values(ALTERNATIVE_TITLES, folio_instance):
        if alternative.get("alternativeTitle"):
            titles.append(
                {
                    "@type": "bf:VariantTitle",
                    "bf:mainTitle": alternative["alternativeTitle"],
                }
            )
    return titles


def _contributions(folio_instance: dict, reference_data: dict) -> list:
    contributions = []
    for contributor in _values(CONTRIBUTORS, folio_instance):
        name_type = _label(
            reference_data,
            "contrib_name_types",
            contributor.get("contributorNameTypeId"),
        )
        role = contributor.get("contributorTypeText") or _label(
            reference_data, "contributor_types", contributor.get("contributorTypeId")
        )
        contributions.append(
            _compact(
                {
                    "@type": "bf:PrimaryContribution"
                    if contributor.get("primary")
                    else "bf:Contribution",
                    "bf:agent": {
                        "@type": AGENT_TYPES.get(name_type, "bf:Agent"),
                        "rdfs:label": contributor.get("name"),
                    },
                    "bf:role": _labeled(role.rstrip(" .").title()) if role else None,
                }
            )
        )
    return contributions


def _classifications(folio_instance: dict, reference_data: dict) -> list:
    classifications = []
    for classification in _values(CLASSIFICATIONS, folio_instance):
        if not classification.get("classificationNumber"):
            continue
        class_type = _label(
            reference_data, "class_types", classification.get("classificationTypeId")
        )
        classifications.append(
            {
                "@type": CLASSIFICATION_TYPES.get(class_type, "bf:Classification"),
                "bf:classificationPortion": classification["classificationNumber"],
            }
        )
    return classifications


def _series(folio_instance: dict) -> list:
    relations = []
    for series in _values(SERIES, folio_instance):
        title = _text(series)
        if not title:
            continue
        if isinstance(series, dict) and series.get("authorityId"):
            resource: dict = {"@type": "bf:Hub", "rdfs:label": title}
        else:
            resource = {
                "@type": "bf:Series",
                "bf:title": {"@type": "bf:Title", "bf:mainTitle": title},
            }
        relations.append({"@type": "bf:Relation", "bf:associatedResource": resource})
    return relations


def _identifiers(folio_instance: dict, reference_data: dict) -> list:
    # Not deduped: one value under several identifier types is a fact about the record
    identifiers = []
    for identifier in _values(IDENTIFIERS, folio_instance):
        if not identifier.get("value"):
            continue
        id_type = _label(
            reference_data, "identifier_types", identifier.get("identifierTypeId")
        )
        identifiers.append(
            _compact(
                {
                    "@type": IDENTIFIER_TYPES.get(id_type, "bf:Identifier"),
                    "rdf:value": identifier["value"],
                    "bf:source": _labeled(id_type),
                }
            )
        )
    return identifiers


def _media_carriers(folio_instance: dict, reference_data: dict) -> tuple[list, list]:
    media, carriers = [], []
    for format_id in _values(INSTANCE_FORMAT_IDS, folio_instance):
        label = _label(reference_data, "instance_formats", format_id)
        if not label:
            continue
        media_term, _, carrier_term = label.partition(" -- ")
        if node := _labeled(media_term):
            media.append(node)
        if node := _labeled(carrier_term):
            carriers.append(node)
    return media, carriers


def _provision_activities(folio_instance: dict) -> list:
    activities = []
    for publication in _values(PUBLICATION, folio_instance):
        role = publication.get("role")
        activity_type = PROVISION_ACTIVITY_TYPES.get(role, "bf:ProvisionActivity")
        if activity_type == "bf:CopyrightDate":
            activity = {"bflc:simpleDate": publication.get("dateOfPublication")}
        else:
            activity = {
                "bflc:simpleAgent": publication.get("publisher"),
                "bflc:simplePlace": publication.get("place"),
                "bflc:simpleDate": publication.get("dateOfPublication"),
            }
        activity = _compact(activity)
        if activity:
            activities.append({"@type": activity_type, **activity})
    return activities


def _electronic_locators(folio_instance: dict) -> list:
    return [
        _compact(
            {
                "rdf:value": access.get("uri"),
                "rdfs:label": access.get("linkText"),
                "bf:note": access.get("publicNote"),
            }
        )
        for access in _values(ELECTRONIC_ACCESS, folio_instance)
        if access.get("uri")
    ]


def folio_to_bf_work(folio_instance: dict, reference_data: dict) -> dict:
    """
    Maps a FOLIO Inventory Instance to a BIBFRAME Work in JSON-LD. The Work
    has no bf:hasInstance; the link is carried by the Instance's bf:instanceOf
    """
    notes = _public_notes(folio_instance, reference_data)
    content = _label(
        reference_data, "instance_types", _first(INSTANCE_TYPE_ID, folio_instance)
    )
    return _compact(
        {
            "@context": BF_CONTEXT,
            "@id": _mint_uri(_first(INSTANCE_ID, folio_instance), "work"),
            "@type": "bf:Work",
            "bf:content": _labeled(content),
            "bf:title": _titles(folio_instance),
            "bf:editionStatement": _values(EDITIONS, folio_instance),
            "bf:contribution": _contributions(folio_instance, reference_data),
            "bf:subject": [
                {"rdfs:label": _text(subject)}
                for subject in _values(SUBJECTS, folio_instance)
                if _text(subject)
            ],
            "bf:classification": _classifications(folio_instance, reference_data),
            "bf:language": [
                {"rdfs:label": language}
                for language in _values(LANGUAGES, folio_instance)
            ],
            "bf:relation": _series(folio_instance),
            "bf:summary": [
                {"@type": "bf:Summary", "rdfs:label": note}
                for note_type, note in notes
                if note_type == SUMMARY_NOTE
            ],
            "bf:note": [
                _bf_note(note_type, note)
                for note_type, note in notes
                if note_type not in (SUMMARY_NOTE, SOURCE_OF_DESCRIPTION_NOTE)
            ],
        }
    )


def folio_to_bf_instance(
    folio_instance: dict,
    reference_data: dict,
    work_uri: str | None = None,
) -> dict:
    """
    Maps a FOLIO Inventory Instance to a BIBFRAME Instance in JSON-LD. When
    saved in the same graph as the Work, Blue Core repoints bf:instanceOf at
    the Work URI it mints. There is no bf:adminMetadata: Blue Core replaces it
    with its own when it mints the Instance's URI
    """
    media, carriers = _media_carriers(folio_instance, reference_data)
    issuance = _label(
        reference_data, "modes_of_issuance", _first(MODE_OF_ISSUANCE_ID, folio_instance)
    )
    return _compact(
        {
            "@context": BF_CONTEXT,
            "@id": _mint_uri(_first(INSTANCE_ID, folio_instance), "instance"),
            "@type": "bf:Instance",
            "bf:instanceOf": {"@id": work_uri} if work_uri else None,
            "bf:title": _titles(folio_instance),
            "bf:identifiedBy": _identifiers(folio_instance, reference_data),
            "bf:media": media,
            "bf:carrier": carriers,
            "bf:issuance": _labeled(issuance),
            "bf:extent": [
                {"rdfs:label": extent}
                for extent in _values(PHYSICAL_DESCRIPTIONS, folio_instance)
            ],
            "bf:provisionActivity": _provision_activities(folio_instance),
            "bf:electronicLocator": _electronic_locators(folio_instance),
            "bf:note": [
                _bf_note(note_type, note)
                for note_type, note in _public_notes(folio_instance, reference_data)
                if note_type == SOURCE_OF_DESCRIPTION_NOTE
            ],
        }
    )


def reference_data_lookups(client) -> dict:
    """
    Builds {property: {id: name}} lookups from FolioClient reference data
    """
    return {
        prop: {row["id"]: row["name"] for row in getattr(client, prop)}
        for prop in REFERENCE_DATA_PROPERTIES
    }


@task
def folio_institution(user_uid: str) -> str:
    """
    Looks up the triggering user's Keycloak groups to find which institution's
    FOLIO to use
    """
    if not user_uid:
        raise ValueError("Missing user_uid, needed to pick the FOLIO institution")
    institution = folio_institution_for(get_user_profile(user_uid)["groups"])
    logger.info(f"Using {institution}_folio for user {user_uid}")
    return institution


@task
def generate_bibframe(folio_instance: dict, institution: str) -> list:
    """
    Takes a FOLIO Instance and generates BIBFRAME Work and Instance in JSON-LD
    format
    """
    reference_data = reference_data_lookups(folio_client(institution))
    work = folio_to_bf_work(folio_instance, reference_data)
    bf_instance = folio_to_bf_instance(
        folio_instance, reference_data, work_uri=work["@id"]
    )
    logger.info(
        f"Generated BIBFRAME Work and Instance for {folio_instance.get('hrid')}"
    )
    return [work, bf_instance]


@task
def ingest_into_bluecore(bibframe: list, user_uid: str | None = None) -> str:
    """
    Takes a list containing BIBFRAME Work and Instance and saves them to the
    Blue Core database, which mints Blue Core URIs for the placeholder ones.
    Returns the URI of the Instance that was saved
    """
    if user_uid:
        # Lets bluecore_models record who made the version
        CURRENT_USER_ID.set(user_uid)

    graph = load_jsonld(bibframe)

    session_maker = sessionmaker(bind=get_engine(get_bluecore_db()))
    bc_url = os.environ.get("AIRFLOW_VAR_BLUECORE_URL", "https://bcld.info")
    bc_graph = save_graph(session_maker, graph, namespace=bc_url, source="FOLIO")

    instance_uri = str(next(bc_graph.subjects(rdflib.RDF.type, BF.Instance)))
    logger.info(f"Saved {instance_uri} to Blue Core")
    return instance_uri


@task
def retrieve_folio_instance(hrid: str, institution: str) -> dict:
    """
    Given an HRID, retrieve a FOLIO Instance from the institution's FOLIO
    """
    folio_instance = lookup_by_local_id(hrid, folio_client(institution))
    if folio_instance is None:
        raise ValueError(f"No FOLIO Instance found for {hrid}")
    source_uri = folio_instance.get("sourceUri", "")
    if "bcld.info" in source_uri:
        raise ValueError(f"{hrid} already is in Blue Core with uri: {source_uri}")
    return folio_instance


@task
def update_source_uri(folio_instance: dict, bf_instance_uri: str, institution: str):
    """
    Updates FOLIO with the Instance URI. A failed update fails the task, so
    the run doesn't report success while FOLIO still lacks the link back
    """
    with folio_client(institution) as folio:
        folio_instance["sourceUri"] = bf_instance_uri
        try:
            folio.folio_put(
                f"/inventory/instances/{folio_instance['id']}", payload=folio_instance
            )
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Error setting sourceUri {bf_instance_uri} on FOLIO Instance "
                f"{folio_instance.get('hrid')} ({folio_instance['id']}): {e}"
            )
            raise
    logger.info(f"Set sourceUri of {folio_instance.get('hrid')} to {bf_instance_uri}")
