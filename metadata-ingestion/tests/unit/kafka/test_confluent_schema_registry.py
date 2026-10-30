import json
import unittest
from typing import Dict, List, Optional
from unittest.mock import patch

import pytest
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
    SchemaReference,
)

from datahub.ingestion.source.confluent_schema_registry import ConfluentSchemaRegistry
from datahub.ingestion.source.kafka.kafka import KafkaSourceConfig, KafkaSourceReport


class ConfluentSchemaRegistryTest(unittest.TestCase):
    def test_get_schema_str_replace_confluent_ref_avro(self):
        schema_str_orig = """
        {
          "fields": [
            {
              "name": "my_field1",
              "type": "TestTopic1"
            }
          ],
          "name": "TestTopic1Val",
          "namespace": "io.acryl",
          "type": "record"
        }
        """
        schema_str_ref = """
        {
          "doc": "Sample schema to help you get started.",
          "fields": [
            {
              "doc": "The int type is a 32-bit signed integer.",
              "name": "my_field1",
              "type": "int"
            }
          ],
          "name": "TestTopic1",
          "namespace": "io.acryl",
          "type": "record"
        }
    """

        schema_str_final = (
            """
    {
      "fields": [
        {
          "name": "my_field1",
          "type": """
            + schema_str_ref
            + """
            }
          ],
          "name": "TestTopic1Val",
          "namespace": "io.acryl",
          "type": "record"
        }
        """
        )

        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, KafkaSourceReport()
        )

        def new_get_latest_version(subject_name: str) -> RegisteredSchema:
            return RegisteredSchema(
                schema_id=1,
                guid=None,
                schema=Schema(schema_str=schema_str_ref, schema_type="AVRO"),
                subject="test",
                version=1,
            )

        with patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = (
                confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                    # The external reference would match by name.
                    schema=Schema(
                        schema_str=schema_str_orig,
                        schema_type="AVRO",
                        references=[
                            SchemaReference(
                                name="TestTopic1", subject="schema_subject_1", version=1
                            )
                        ],
                    )
                )
            )
            assert schema_str == ConfluentSchemaRegistry._compact_schema(
                schema_str_final
            )

        with patch.object(
            confluent_schema_registry.schema_registry_client,
            "get_latest_version",
            new_get_latest_version,
        ):
            schema_str = (
                confluent_schema_registry.get_schema_str_replace_confluent_ref_avro(
                    # The external reference would match by subject.
                    schema=Schema(
                        schema_str=schema_str_orig,
                        schema_type="AVRO",
                        references=[
                            SchemaReference(
                                name="schema_subject_1", subject="TestTopic1", version=1
                            )
                        ],
                    )
                )
            )
            assert schema_str == ConfluentSchemaRegistry._compact_schema(
                schema_str_final
            )

    def test_get_schema_fields_with_hyphenated_namespace(self):
        """Test that schemas with hyphens in namespace (e.g., from Debezium CDC)
        are parsed successfully by default, matching Java Schema Registry client behavior."""
        debezium_schema_str = json.dumps(
            {
                "type": "record",
                "name": "Value",
                "namespace": "my-debezium-topic.public.users",
                "fields": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "string"},
                ],
            }
        )

        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        confluent_schema_registry = ConfluentSchemaRegistry.create(
            kafka_source_config, KafkaSourceReport()
        )

        schema = Schema(schema_str=debezium_schema_str, schema_type="AVRO")
        fields = confluent_schema_registry._get_schema_fields(
            topic="my-debezium-topic.public.users",
            schema=schema,
            is_key_schema=False,
        )

        assert len(fields) == 2
        field_names = [f.fieldPath for f in fields]
        assert any("id" in name for name in field_names)
        assert any("name" in name for name in field_names)

    def test_init_subjects_connectivity_failure_is_reported(self):
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()

        with patch(
            "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient"
        ) as mock_client_cls:
            mock_client_cls.return_value.get_subjects.side_effect = OSError(
                "registry unreachable"
            )
            registry = ConfluentSchemaRegistry.create(kafka_source_config, report)

        # A registry we can't reach at startup must be tallied, not swallowed.
        assert report.schema_registry_connectivity_failures == 1
        assert registry.get_subjects() == []

    def test_batch_connectivity_failure_is_reported(self):
        kafka_source_config = KafkaSourceConfig.model_validate(
            {
                "connection": {
                    "bootstrap": "localhost:9092",
                    "schema_registry_url": "http://localhost:8081",
                },
            }
        )
        report = KafkaSourceReport()
        registry = ConfluentSchemaRegistry.create(kafka_source_config, report)

        with patch.object(
            registry,
            "_get_schema_and_fields",
            side_effect=OSError("registry unreachable"),
        ):
            result = registry.get_schema_and_fields_batch(["topic-a"])

        # Connectivity failures must be tallied and the topic left schemaless.
        assert report.schema_registry_connectivity_failures == 1
        assert result["topic-a"].schema is None
        assert result["topic-a"].fields == []


def _registry_with_subjects(
    subjects: List[str], config: Optional[Dict[str, object]] = None
) -> ConfluentSchemaRegistry:
    kafka_source_config = KafkaSourceConfig.model_validate(
        {
            "connection": {
                "bootstrap": "localhost:9092",
                "schema_registry_url": "http://localhost:8081",
            },
            **(config or {}),
        }
    )
    with patch(
        "datahub.ingestion.source.confluent_schema_registry.SchemaRegistryClient"
    ) as mock_client_cls:
        mock_client_cls.return_value.get_subjects.return_value = subjects
        return ConfluentSchemaRegistry.create(kafka_source_config, KafkaSourceReport())


# Recent Confluent Schema Registry versions list subjects sorted, which puts
# "orders-archive-*" ahead of "orders-key" and "orders-value".
_SORTED_DASH_SIBLINGS = sorted(
    ["orders-key", "orders-value", "orders-archive-key", "orders-archive-value"]
)
# Registries that list in insertion or hash order can put any sibling first.
_UNSORTED_UNDERSCORE_SIBLINGS = [
    "orders_archive-value",
    "orders_archive-key",
    "orders-value",
    "orders-key",
]


@pytest.mark.parametrize(
    "subjects, config, topic, is_key_schema, expected",
    [
        pytest.param(
            _SORTED_DASH_SIBLINGS,
            None,
            "orders",
            False,
            "orders-value",
            id="exact-value-beats-sorted-dash-sibling",
        ),
        pytest.param(
            _SORTED_DASH_SIBLINGS,
            None,
            "orders",
            True,
            "orders-key",
            id="exact-key-beats-sorted-dash-sibling",
        ),
        pytest.param(
            _SORTED_DASH_SIBLINGS,
            None,
            "orders-archive",
            False,
            "orders-archive-value",
            id="dash-sibling-keeps-its-own-subject",
        ),
        pytest.param(
            _UNSORTED_UNDERSCORE_SIBLINGS,
            None,
            "orders",
            False,
            "orders-value",
            id="exact-value-beats-unsorted-underscore-sibling",
        ),
        pytest.param(
            sorted(["orders-com.example.Order-value", "orders-value"]),
            None,
            "orders",
            False,
            "orders-value",
            id="exact-value-beats-record-subject-listed-first",
        ),
        pytest.param(
            ["orders_archive-value", "orders-com.example.Order-value"],
            None,
            "orders",
            False,
            "orders-com.example.Order-value",
            id="record-subject-matches-without-exact-subject",
        ),
        pytest.param(
            # String-keyed producers register no key subject.
            ["orders-value", "orders_archive-key"],
            None,
            "orders",
            True,
            None,
            id="missing-key-subject-not-taken-from-sibling",
        ),
        pytest.param(
            ["orders_archive-value"],
            None,
            "orders",
            False,
            None,
            id="schemaless-topic-not-given-sibling-subject",
        ),
        pytest.param(
            ["a.b.c.d.qa-value"],
            None,
            "a.b.c.d",
            False,
            None,
            id="environment-suffixed-subject-not-matched",
        ),
        pytest.param(
            # Confluent's own TopicRecordNameStrategy subject has no -key/-value suffix.
            ["orders-com.example.Order", "orders_archive-value"],
            None,
            "orders",
            False,
            None,
            id="unsuffixed-record-subject-falls-through-to-none",
        ),
        pytest.param(
            # The exact subject of topic "orders-".
            ["orders--value"],
            None,
            "orders",
            False,
            None,
            id="empty-record-name-not-matched",
        ),
        pytest.param(
            ["orders-value", "mapped-value"],
            {"topic_subject_map": {"orders-value": "mapped-value"}},
            "orders",
            False,
            "mapped-value",
            id="topic-subject-map-overrides-exact-subject",
        ),
        pytest.param(
            sorted(["orders-com.example.Order-value", "orders-value"]),
            {"disable_topic_record_naming_strategy": True},
            "orders",
            False,
            "orders-value",
            id="record-naming-disabled-exact-subject",
        ),
        pytest.param(
            ["orders-com.example.Order-value"],
            {"disable_topic_record_naming_strategy": True},
            "orders",
            False,
            None,
            id="record-naming-disabled-no-fallback",
        ),
    ],
)
def test_get_subject_for_topic(
    subjects: List[str],
    config: Optional[Dict[str, object]],
    topic: str,
    is_key_schema: bool,
    expected: Optional[str],
) -> None:
    registry = _registry_with_subjects(subjects, config)

    assert registry._get_subject_for_topic(topic, is_key_schema) == expected


if __name__ == "__main__":
    unittest.main()
