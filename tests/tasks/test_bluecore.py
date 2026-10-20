import gzip
import json
import os
import tarfile
import zipfile

import pytest
import rdflib
from bibframe_json import CONTEXT_URL
from bluecore_models.utils.graph import BF, MADS
from rdflib import DCTERMS, XSD
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from ils_middleware.tasks import bluecore
from ils_middleware.tasks.bluecore import (
    batch_archived_files,
    delete_upload,
    get_bluecore_db,
    is_gzip_json_lines,
    is_zip,
    load_cbd_files,
    zip_to_tar_gz,
)


@pytest.fixture(autouse=True)
def clear_engine_cache():
    """Engines are cached per DB URL at module scope; reset between tests so a
    mocked engine from one test can't leak into the next."""
    bluecore._engines.clear()
    yield
    bluecore._engines.clear()


def test_batch_archived_files_no_file():
    with pytest.raises(FileNotFoundError, match="cbd.tar.gz does not exist"):
        batch_archived_files("cbd.tar.gz")


def test_batch_archived_files(tmp_path):
    test_cbd_files_dir = tmp_path / "data"
    test_cbd_files_dir.mkdir()
    for i in range(10_000):
        test_cbd_file = test_cbd_files_dir / f"20{i}.cbd.jsonld"
        test_cbd_file.write_text("{}")
    archived_file_path = tmp_path / "cbd.tar.gz"
    with tarfile.open(archived_file_path, "x:gz") as tar_file:
        tar_file.add(test_cbd_files_dir)

    test_batches = batch_archived_files(str(archived_file_path))
    assert len(test_batches) == 5
    assert len(test_batches[0]) == 2_000


def _build_json_lines_gz(tmp_path, total_lines=12):
    json_lines_path = tmp_path / "hubs.bibframe.jsonld.gz"
    with gzip.open(json_lines_path, "wt", encoding="utf-8") as fo:
        for i in range(total_lines):
            doc = {
                "@context": "http://id.loc.gov/resources/hubs/context.json",
                "@id": f"/resources/hubs/{i}",
                "@graph": [
                    {"@id": f"http://id.loc.gov/resources/hubs/{i}", "bf:code": str(i)}
                ],
            }
            fo.write(json.dumps(doc) + "\n")
    return json_lines_path


def test_is_gzip_json_lines(tmp_path):
    assert is_gzip_json_lines(_build_json_lines_gz(tmp_path))
    assert not is_gzip_json_lines(_build_cbd_archive(tmp_path))


def test_batch_archived_files_json_lines(tmp_path):
    json_lines_path = _build_json_lines_gz(tmp_path)

    test_batches = batch_archived_files(str(json_lines_path))

    assert test_batches == [["0-3"], ["3-6"], ["6-9"], ["9-12"]]


def test_batch_archived_files_fewer_files_than_batches(tmp_path):
    json_lines_path = _build_json_lines_gz(tmp_path, total_lines=3)

    assert batch_archived_files(str(json_lines_path)) == [["0-1"], ["1-2"], ["2-3"]]


def test_load_cbd_files_json_lines(mocker, tmp_path):
    json_lines_path = _build_json_lines_gz(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")

    save_graph_mock = mocker.patch("ils_middleware.tasks.bluecore.save_graph")

    errors = load_cbd_files(
        ["3-6"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(json_lines_path),
    )

    assert errors == []
    assert save_graph_mock.call_count == 3
    saved_codes = sorted(
        str(obj)
        for call in save_graph_mock.call_args_list
        for obj in call.args[1].objects()
    )
    assert saved_codes == ["3", "4", "5"]


def test_load_cbd_files_json_lines_bad_line(mocker, tmp_path):
    json_lines_path = tmp_path / "bad.jsonld.gz"
    with gzip.open(json_lines_path, "wt", encoding="utf-8") as fo:
        fo.write("{not json\n")

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    save_graph_mock = mocker.patch("ils_middleware.tasks.bluecore.save_graph")

    errors = load_cbd_files(
        ["0-1"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(json_lines_path),
    )

    assert errors == ["line 0"]
    assert save_graph_mock.call_count == 0


def test_delete_upload(tmp_path):
    upload_path = tmp_path / "sub_dir"
    os.mkdir(upload_path)
    upload_file = upload_path / "bf-record.jsonld"
    upload_file.touch()
    upload_file2 = upload_path / "bf-record2.jsonld"
    upload_file2.touch()

    assert upload_path.exists()
    assert upload_file.exists()
    assert upload_file2.exists()

    # It should delete the parent directory only if it's empty and remove_empty_parent is True
    delete_upload(str(upload_file))
    assert upload_path.exists()
    assert not upload_file.exists()

    # It should keep the parent directory as remove_empty_parent is not set
    delete_upload(str(upload_file2))
    assert upload_path.exists()
    assert not upload_file2.exists()

    # Delete the second file again with the remove_empty_parent flag set to True
    upload_file2.touch()
    delete_upload(str(upload_file2), remove_empty_parent=True)
    assert not upload_path.exists()


def test_is_zip():
    assert is_zip("test.zip")
    assert is_zip("test.tar.gz")
    assert not is_zip("test.txt")


class MockPostgresHook:
    def __init__(self, *args):
        self.sqlalchemy_url = make_url(
            "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore"
        )


@pytest.fixture
def mock_postgres_hook(mocker):
    mocker.patch(
        "ils_middleware.tasks.bluecore.PostgresHook",
        return_value=MockPostgresHook(),
    )
    return mocker


def test_get_bluecore_db(mock_postgres_hook):
    db_string = get_bluecore_db()

    assert db_string.startswith("postgresql://bluecore_admin")


def test_get_engine_caches_per_url(mocker):
    create_engine_mock = mocker.patch(
        "ils_middleware.tasks.bluecore.create_engine",
        side_effect=lambda url, **kwargs: mocker.MagicMock(name=url),
    )

    first = bluecore.get_engine("postgresql://host/db_a")
    again = bluecore.get_engine("postgresql://host/db_a")
    other = bluecore.get_engine("postgresql://host/db_b")

    # same URL reuses one engine, a different URL gets its own
    assert first is again
    assert first is not other
    assert create_engine_mock.call_count == 2


def _build_cbd_archive(tmp_path):
    cbd_file = tmp_path / "022.cbd.jsonld"
    cbd_file.write_text("{}")  # empty but valid JSON-LD
    archive_path = tmp_path / "cbd.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(cbd_file, arcname="022.cbd.jsonld")
    return archive_path


def test_load_cbd_files_with_deadlocks(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    sleep_mock = mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    deadlock = OperationalError("deadlock detected", None, Exception("deadlock"))
    save_graph_mock = mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        # raise twice, then succeed on the 3rd attempt
        side_effect=[deadlock, deadlock, None],
    )

    errors = load_cbd_files(
        ["022.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    assert errors == []
    assert save_graph_mock.call_count == 3  # retried until success
    assert sleep_mock.call_count == 2  # backoff between the 3 attempts


def test_load_cbd_files_persistent_deadlock(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    deadlock = OperationalError("deadlock detected", None, Exception("deadlock"))
    mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        side_effect=deadlock,  # always deadlocks
    )

    # after 3 failed attempts the loop re-raises
    with pytest.raises(OperationalError):
        load_cbd_files(
            ["022.cbd.jsonld"],
            "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
            "user-uid",
            str(archive_path),
        )


def test_load_cbd_files_non_deadlock_error(mocker, tmp_path):
    archive_path = _build_cbd_archive(tmp_path)

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    sleep_mock = mocker.patch("ils_middleware.tasks.bluecore.time.sleep")

    save_graph_mock = mocker.patch(
        "ils_middleware.tasks.bluecore.save_graph",
        side_effect=ValueError("bad graph"),
    )

    errors = load_cbd_files(
        ["022.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    # a non-deadlock error is recorded once and does not retry
    assert errors == ["022.cbd.jsonld"]
    assert save_graph_mock.call_count == 1
    assert sleep_mock.call_count == 0


FOREIGN_CONTEXT = "http://id.loc.gov/resources/works/context.json"


def test_prepare_jsonld_no_context_is_kept_absent():
    doc = {"@id": "http://id.loc.gov/resources/works/1", "code": ["1"]}

    assert bluecore._prepare_jsonld(doc) == doc


def test_prepare_jsonld_shipped_context_is_kept():
    doc = {"@context": CONTEXT_URL, "@id": "http://id.loc.gov/resources/works/1"}

    assert bluecore._prepare_jsonld(doc) == doc


def test_prepare_jsonld_foreign_context_is_replaced():
    doc = {"@context": FOREIGN_CONTEXT, "@id": "http://id.loc.gov/resources/works/1"}

    assert bluecore._prepare_jsonld(doc) == {
        "@context": CONTEXT_URL,
        "@id": "http://id.loc.gov/resources/works/1",
    }


def test_prepare_jsonld_graph_wrapper_id_is_dropped():
    graph = [{"@id": "http://id.loc.gov/resources/works/1"}]
    doc = {"@context": FOREIGN_CONTEXT, "@id": "/resources/works/1", "@graph": graph}

    assert bluecore._prepare_jsonld(doc) == {"@context": CONTEXT_URL, "@graph": graph}


def test_prepare_jsonld_list_is_prepared_member_by_member():
    docs = [
        {"@id": "http://id.loc.gov/resources/works/1"},
        {"@context": FOREIGN_CONTEXT, "@id": "http://id.loc.gov/resources/works/2"},
        "not a node object",
    ]

    assert bluecore._prepare_jsonld(docs) == [
        {"@id": "http://id.loc.gov/resources/works/1"},
        {"@context": CONTEXT_URL, "@id": "http://id.loc.gov/resources/works/2"},
        "not a node object",
    ]


def _build_tar_archive(tmp_path, members):
    """Builds a tar archive holding each member name with its JSON content"""
    archive_path = tmp_path / "cbd.tar.gz"
    with tarfile.open(archive_path, "w:gz") as tar:
        for name, content in members.items():
            member_path = tmp_path / name
            member_path.write_text(json.dumps(content))
            tar.add(member_path, arcname=name)
    return archive_path


def test_load_cbd_files_realistic_tar_members(mocker, tmp_path):
    work_1 = rdflib.URIRef("http://id.loc.gov/resources/works/1")
    work_2 = rdflib.URIRef("http://id.loc.gov/resources/works/2")
    work_3 = rdflib.URIRef("http://id.loc.gov/resources/works/3")
    archive_path = _build_tar_archive(
        tmp_path,
        {
            # id.loc.gov style: a foreign @context with an @id wrapping @graph
            "1.cbd.jsonld": {
                "@context": FOREIGN_CONTEXT,
                "@id": "/resources/works/1",
                "@graph": [{"@id": str(work_1), "@type": "Work", "code": ["1"]}],
            },
            # expanded JSON-LD: a top-level array with full IRIs and no @context
            "2.cbd.jsonld": [
                {
                    "@id": str(work_2),
                    "@type": [str(BF.Work)],
                    str(BF.code): [{"@value": "2"}],
                }
            ],
            # already framed with the bibframe-json context
            "3.cbd.jsonld": {
                "@context": CONTEXT_URL,
                "@id": str(work_3),
                "@type": "Work",
                "code": ["3"],
            },
        },
    )

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    save_graph_mock = mocker.patch("ils_middleware.tasks.bluecore.save_graph")

    errors = load_cbd_files(
        ["1.cbd.jsonld", "2.cbd.jsonld", "3.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    assert errors == []
    assert save_graph_mock.call_count == 3
    saved = [call.args[1] for call in save_graph_mock.call_args_list]
    for graph, work, code in zip(saved, [work_1, work_2, work_3], ["1", "2", "3"]):
        assert (work, rdflib.RDF.type, BF.Work) in graph
        assert (work, BF.code, rdflib.Literal(code)) in graph


HUBS_CONTEXT = "http://id.loc.gov/resources/hubs/context.json"
HUBS = rdflib.URIRef("http://id.loc.gov/resources/hubs")


def _hub_doc(hub_id):
    """A document shaped like a line of id.loc.gov's hubs.bibframe.jsonld.gz,
    using each prefix the real file needs that bibframe-json doesn't define"""
    hub = f"http://id.loc.gov/resources/hubs/{hub_id}"
    return {
        "@context": HUBS_CONTEXT,
        "@id": f"/resources/hubs/{hub_id}",
        "@graph": [
            {
                "@id": hub,
                "@type": ["bf:Hub", "bf:Work"],
                "bf:adminMetadata": {"@id": "_:admin"},
                "dcterms:isPartOf": {"@id": str(HUBS)},
                "madsrdf:hasRelatedAuthority": {"@id": f"{hub}/related"},
            },
            {
                "@id": "_:admin",
                "@type": "bf:AdminMetadata",
                "bf:date": {"@type": "xsd:dateTime", "@value": "2024-05-03T07:07:18"},
            },
        ],
    }


def _assert_hub_prefixes_resolved(graph, hub_id):
    hub = rdflib.URIRef(f"http://id.loc.gov/resources/hubs/{hub_id}")
    assert (hub, rdflib.RDF.type, BF.Hub) in graph
    assert (hub, DCTERMS.isPartOf, HUBS) in graph
    assert (hub, MADS.hasRelatedAuthority, rdflib.URIRef(f"{hub}/related")) in graph
    dates = list(graph.objects(predicate=BF.date))
    assert dates == [rdflib.Literal("2024-05-03T07:07:18", datatype=XSD.dateTime)]
    # no CURIE was left unexpanded as an IRI with a made-up scheme
    for s, p, o in graph:
        for node in (p, o, getattr(o, "datatype", None)):
            assert not str(node).startswith(("dcterms:", "madsrdf:", "xsd:"))


def test_parse_jsonld_hubs_context_resolves_prefixes(mocker):
    # the held copy of the context is used, never the network
    urlopen_mock = mocker.patch("rdflib.parser._urlopen", side_effect=AssertionError)
    load_jsonld_spy = mocker.spy(bluecore, "load_jsonld")

    graph = bluecore._parse_jsonld(_hub_doc("1"))

    _assert_hub_prefixes_resolved(graph, "1")
    # bluecore_models' prefixes are still bound after rdflib parsed with a context
    assert dict(graph.namespaces())["bf"] == rdflib.URIRef(str(BF))
    urlopen_mock.assert_not_called()
    load_jsonld_spy.assert_not_called()


def test_parse_jsonld_other_documents_use_load_jsonld(mocker):
    load_jsonld_spy = mocker.spy(bluecore, "load_jsonld")
    doc = {"@context": CONTEXT_URL, "@id": "http://id.loc.gov/resources/works/1"}

    bluecore._parse_jsonld(doc)

    load_jsonld_spy.assert_called_once_with(doc)


def test_load_cbd_files_json_lines_hubs_context(mocker, tmp_path):
    json_lines_path = tmp_path / "hubs.bibframe.jsonld.gz"
    with gzip.open(json_lines_path, "wt", encoding="utf-8") as fo:
        for hub_id in ("1", "2"):
            fo.write(json.dumps(_hub_doc(hub_id)) + "\n")

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    save_graph_mock = mocker.patch("ils_middleware.tasks.bluecore.save_graph")

    errors = load_cbd_files(
        ["0-2"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(json_lines_path),
    )

    assert errors == []
    saved = [call.args[1] for call in save_graph_mock.call_args_list]
    assert len(saved) == 2
    for graph, hub_id in zip(saved, ("1", "2")):
        _assert_hub_prefixes_resolved(graph, hub_id)


def test_load_cbd_files_tar_member_with_hubs_context(mocker, tmp_path):
    archive_path = _build_tar_archive(tmp_path, {"1.cbd.jsonld": _hub_doc("1")})

    mocker.patch("ils_middleware.tasks.bluecore.create_engine")
    mocker.patch("ils_middleware.tasks.bluecore.sessionmaker")
    save_graph_mock = mocker.patch("ils_middleware.tasks.bluecore.save_graph")

    errors = load_cbd_files(
        ["1.cbd.jsonld"],
        "postgresql://bluecore_admin:bluecore_admin@localhost/bluecore",
        "user-uid",
        str(archive_path),
    )

    assert errors == []
    assert save_graph_mock.call_count == 1
    _assert_hub_prefixes_resolved(save_graph_mock.call_args.args[1], "1")


def test_zip_to_tar_gz(tmp_path):
    cbd_file_path = tmp_path / "022.cbd.jsonld"
    cbd_file_path.write_text("{}")

    zip_file_path = tmp_path / "cbd.zip"
    with zipfile.ZipFile(zip_file_path, "w") as zip_file:
        zip_file.write(cbd_file_path)

    tar_file_name = zip_to_tar_gz(str(zip_file_path))
    assert not zip_file_path.exists()
    assert tar_file_name.endswith("cbd.tar.gz")
