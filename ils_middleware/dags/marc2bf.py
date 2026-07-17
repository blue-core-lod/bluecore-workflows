import logging
import pathlib
import uuid

from datetime import datetime

from airflow.sdk import dag, task, get_current_context
from ils_middleware.tasks.bluecore import (
    delete_upload,
    get_bluecore_db,
    load
)
from ils_middleware.tasks.general.marc import (
    convert_to_xml,
    xslt_marc_to_bf
)

logger = logging.getLogger(__name__)

@dag(
    schedule=None,
    start_date=datetime(2026, 7, 7),
    catchup=False,
    tags=["transform", "record", "marc"],
    default_args={"owner": "airflow"},
)
def marc_to_bibframe():
    @task
    def setup() -> dict:
        context = get_current_context()
        params = context.get("params")
        if params is None:
            raise ValueError("Missing params")
        marc_file = params.get("marc_file", "")
        if not pathlib.Path(marc_file).exists():
            raise ValueError(f"{marc_file} does not exist")
        ingest = params.get("ingest", False)
        source_base_uri = params.get("source_base_uri", "http://id.loc.gov/resources")
        return {
            "marc_file": marc_file, 
            "ingest": ingest,
            "source_base_uri": source_base_uri
        }


    @task
    def to_xml(marc_file: str) -> str:
        """
        Convert MARC21 to MARC XML
        """
        marc_path = pathlib.Path(marc_file)
        match marc_path.suffix:
            case ".mrc" | ".marc" | ".dat":
                raw_xml = convert_to_xml(marc_file)

            case ".xml":
                raw_xml = marc_path.read_text()

            case _:
                raise ValueError(f"Unknown MARC file format: {marc_path.suffix}")
        return raw_xml
    

    @task
    def transform_to_bf(marc_xml: str, source_base_uri: str) -> str:
        """
        Transforms MARC XML to BIBFRAME RDF XML using LOC's marc2bibframe2 XSLT
        """
        bf_rdf_xml = xslt_marc_to_bf(marc_xml, source_base_uri)
        return bf_rdf_xml
    

    @task.branch
    def should_ingest(ingest: bool) -> str:
        if ingest:
            return "ingest"
        return "finish"
    
    @task
    def ingest(bf_rdf_xml: str, uploads_dir: str = "/opt/airflow/uploads"):
        """
        Saves BIBFRAME RDF to a temp file and loads into  
        """
        uploads_path = pathlib.Path(uploads_dir)
        upload_file = uploads_path / f"{uuid.uuid4()}.xml"
        with upload_file.open("w+") as fo:
            fo.write(bf_rdf_xml)
        return str(upload_file)

    @task(trigger_rule="none_failed_min_one_success")
    def finish(marc_file: str, bf_file: str | None = None):
        logger.info("Finished converting MARC to BIBFRAME")
        logger.info(f"Removing MARC file {marc_file}")
        delete_upload(marc_file)
        if bf_file:
            logger.info(f"Removing temp BIBFRAME file")
            delete_upload(bf_file)


    workflow_params = setup()
    marc_xml = to_xml(workflow_params["marc_file"])
    bf_xml = transform_to_bf(marc_xml, workflow_params["source_base_uri"])
    chosen_branch = should_ingest(workflow_params["ingest"])
    bf_file_ingest = ingest(bf_xml)
    finish_task = finish(workflow_params["marc_file"], bf_file_ingest)

    bf_xml >> chosen_branch >> [bf_file_ingest, finish_task]

    
marc_to_bibframe()









