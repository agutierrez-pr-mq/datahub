"""
Tests for column-level lineage from Debezium source connectors to Kafka topics.

Relational Debezium connectors write change-event envelopes ({before, after, source,
op, ts_ms}), so table columns land under `after.` on the topic unless the
ExtractNewRecordState SMT unwraps them. When the topic's schema is in DataHub, the
downstream schemaField URNs must use the topic's stored (v2) field paths.
"""

from typing import List, Optional
from unittest.mock import Mock

from datahub.emitter.mce_builder import schema_field_urn_to_key
from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    NumberTypeClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    UnionTypeClass,
)

MYSQL_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,inventory.customers,PROD)"
TOPIC = "dbserver1.inventory.customers"

ENVELOPE = f"[version=2.0].[type={TOPIC}.Envelope]"
AFTER = f"{ENVELOPE}.[type=union].[type={TOPIC}.Value].after"
BEFORE = f"{ENVELOPE}.[type=union].[type={TOPIC}.Value].before"
KEY = f"[version=2.0].[key=True].[type={TOPIC}.Key]"

# Field paths as the Kafka source stores them for an Avro Debezium envelope,
# in schema order: key first, a nullable column expands to parent + union variants.
ENVELOPE_FIELD_PATHS = [
    f"{KEY}.[type=int].id",
    f"{ENVELOPE}.[type=union].before",
    f"{BEFORE}.[type=int].id",
    f"{BEFORE}.[type=string].email",
    f"{BEFORE}.[type=union].nickname",
    f"{BEFORE}.[type=union].[type=null].nickname",
    f"{BEFORE}.[type=union].[type=string].nickname",
    f"{ENVELOPE}.[type=union].after",
    f"{AFTER}.[type=int].id",
    f"{AFTER}.[type=string].email",
    f"{AFTER}.[type=union].nickname",
    f"{AFTER}.[type=union].[type=null].nickname",
    f"{AFTER}.[type=union].[type=string].nickname",
    f"{ENVELOPE}.[type=string].op",
]

UNWRAPPED = f"[version=2.0].[type={TOPIC}.Value]"
UNWRAPPED_FIELD_PATHS = [
    f"{KEY}.[type=int].id",
    f"{UNWRAPPED}.[type=int].id",
    f"{UNWRAPPED}.[type=string].email",
    f"{UNWRAPPED}.[type=union].nickname",
    f"{UNWRAPPED}.[type=union].[type=string].nickname",
]

SOURCE_SCHEMA = {"id": "INT", "email": "VARCHAR", "nickname": "VARCHAR"}


def _schema_field(field_path: str) -> SchemaFieldClass:
    leaf_type = field_path.rsplit(".", 2)[-2]
    data_type = (
        NumberTypeClass()
        if leaf_type == "[type=int]"
        else UnionTypeClass()
        if leaf_type == "[type=union]"
        else StringTypeClass()
    )
    return SchemaFieldClass(
        fieldPath=field_path,
        type=SchemaFieldDataTypeClass(type=data_type),
        nativeDataType="avro",
        isPartOfKey="[key=True]" in field_path,
    )


def _topic_schema(field_paths: List[str]) -> SchemaMetadataClass:
    return SchemaMetadataClass(
        schemaName=f"{TOPIC}-value",
        platform="urn:li:dataPlatform:kafka",
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        fields=[_schema_field(p) for p in field_paths],
    )


def _make_connector(
    connector_class: str,
    transforms: Optional[dict] = None,
    topic_field_paths: Optional[List[str]] = None,
) -> BaseConnector:
    manifest = ConnectorManifest(
        name="inventory-connector",
        type="source",
        config={"connector.class": connector_class, **(transforms or {})},
        tasks=[],
    )
    config = KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083",
        cluster_name="test",
        use_schema_resolver=True,
        schema_resolver_finegrained_lineage=True,
    )
    connector = BaseConnector(manifest, config, KafkaConnectSourceReport())

    resolver = Mock()
    resolver.resolve_table.return_value = (MYSQL_URN, dict(SOURCE_SCHEMA))
    if topic_field_paths is None:
        resolver.graph = None
    else:
        resolver.graph.get_aspect.return_value = _topic_schema(topic_field_paths)
    connector.schema_resolver = resolver
    return connector


def _downstream_paths(lineages: Optional[List[FineGrainedLineageClass]]) -> dict:
    """Map each upstream column to its downstream topic field path."""
    assert lineages is not None
    result = {}
    for lineage in lineages:
        assert lineage.upstreams is not None and lineage.downstreams is not None
        upstream = schema_field_urn_to_key(lineage.upstreams[0])
        downstream = schema_field_urn_to_key(lineage.downstreams[0])
        assert upstream is not None and downstream is not None
        result[upstream.fieldPath] = downstream.fieldPath
    return result


def _extract(connector: BaseConnector) -> Optional[List[FineGrainedLineageClass]]:
    return connector._extract_fine_grained_lineage(
        source_dataset="inventory.customers",
        source_platform="mysql",
        target_dataset=TOPIC,
        target_platform="kafka",
    )


def test_envelope_without_topic_schema_targets_after_struct():
    connector = _make_connector("io.debezium.connector.mysql.MySqlConnector")

    paths = _downstream_paths(_extract(connector))

    assert paths == {
        "id": "after.id",
        "email": "after.email",
        "nickname": "after.nickname",
    }


def test_envelope_resolves_to_topic_v2_field_paths():
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        topic_field_paths=ENVELOPE_FIELD_PATHS,
    )

    paths = _downstream_paths(_extract(connector))

    # `after`, not `before`; value `id`, not the key `id`; the union parent,
    # not its `[type=null]` / `[type=string]` variants.
    assert paths == {
        "id": f"{AFTER}.[type=int].id",
        "email": f"{AFTER}.[type=string].email",
        "nickname": f"{AFTER}.[type=union].nickname",
    }
    assert not connector.report.warnings


def test_extract_new_record_state_maps_columns_one_to_one():
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        transforms={
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.add.fields": "op,source.ts_ms",
        },
    )

    paths = _downstream_paths(_extract(connector))

    assert paths == {"id": "id", "email": "email", "nickname": "nickname"}


def test_extract_new_record_state_resolves_unwrapped_v2_paths():
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        transforms={
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        },
        topic_field_paths=UNWRAPPED_FIELD_PATHS,
    )

    paths = _downstream_paths(_extract(connector))

    assert paths == {
        "id": f"{UNWRAPPED}.[type=int].id",
        "email": f"{UNWRAPPED}.[type=string].email",
        "nickname": f"{UNWRAPPED}.[type=union].nickname",
    }


def test_extract_field_after_on_debezium_source_maps_one_to_one():
    # Previously produced no lineage: source columns never started with `after.`.
    connector = _make_connector(
        "io.debezium.connector.postgresql.PostgresConnector",
        transforms={
            "transforms": "unwrap",
            "transforms.unwrap.type": "org.apache.kafka.connect.transforms.ExtractField$Value",
            "transforms.unwrap.field": "after",
        },
    )

    paths = _downstream_paths(_extract(connector))

    assert paths == {"id": "id", "email": "email", "nickname": "nickname"}


def test_replace_field_on_envelope_does_not_touch_row_columns():
    # Without unwrapping, ReplaceField$Value only sees top-level envelope fields.
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        transforms={
            "transforms": "drop",
            "transforms.drop.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.drop.exclude": "email",
        },
    )

    paths = _downstream_paths(_extract(connector))

    assert paths["email"] == "after.email"


def test_columns_missing_from_topic_schema_are_skipped_and_reported():
    # A topic whose stored schema belongs to another table shares only `id`.
    other_table = [p for p in ENVELOPE_FIELD_PATHS if p.endswith(".id")]
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        topic_field_paths=other_table,
    )

    paths = _downstream_paths(_extract(connector))

    assert paths == {"id": f"{AFTER}.[type=int].id"}
    warnings = list(connector.report.warnings)
    assert len(warnings) == 1
    assert "missing=2/3" in str(warnings[0].context)


def test_column_case_mismatch_resolves_case_insensitively():
    connector = _make_connector(
        "io.debezium.connector.mysql.MySqlConnector",
        topic_field_paths=[p.replace(".email", ".EMAIL") for p in ENVELOPE_FIELD_PATHS],
    )

    paths = _downstream_paths(_extract(connector))

    assert paths["email"] == f"{AFTER}.[type=string].EMAIL"


def test_non_debezium_connector_keeps_one_to_one_mapping():
    connector = _make_connector("io.confluent.connect.jdbc.JdbcSourceConnector")

    paths = _downstream_paths(_extract(connector))

    assert paths == {"id": "id", "email": "email", "nickname": "nickname"}


def test_mongodb_connector_is_not_treated_as_envelope():
    connector = _make_connector("io.debezium.connector.mongodb.MongoDbConnector")

    assert not connector._emits_debezium_envelope()


def test_sink_side_unwrap_promotes_after_fields():
    connector = _make_connector(
        "com.snowflake.kafka.connector.SnowflakeSinkConnector",
        transforms={
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        },
    )

    mapping = connector._apply_field_transforms(
        ["before.id", "after.id", "after.email", "op"]
    )

    assert mapping == {"after.id": "id", "after.email": "email"}
