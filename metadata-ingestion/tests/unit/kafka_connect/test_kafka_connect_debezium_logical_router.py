"""Tests for Debezium ByLogicalTableRouter / ToLogicalTopicRouter support."""

from typing import Dict, List, Optional, Tuple
from unittest.mock import patch

import pytest

from datahub.ingestion.source.kafka_connect.common import (
    SOURCE,
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.connector_registry import ConnectorRegistry
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    DebeziumLogicalTopicRouterPlugin,
    TransformConfig,
    TransformPluginRegistry,
    get_transform_pipeline,
)

BY_LOGICAL = "io.debezium.transforms.ByLogicalTableRouter"
TO_LOGICAL = "io.debezium.transforms.ToLogicalTopicRouter"

MYSQL_CONFIG: Dict[str, str] = {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "topic.prefix": "dbserver1",
    "table.include.list": "inventory.orders,inventory.customers",
    "transforms": "Reroute",
    "transforms.Reroute.type": BY_LOGICAL,
    "transforms.Reroute.topic.regex": r"dbserver1\.(.*)\.(.*)",
    "transforms.Reroute.topic.replacement": "cdc-$1-$2",
}


def _router(regex: str, replacement: str) -> TransformConfig:
    return TransformConfig(
        name="Reroute",
        type=BY_LOGICAL,
        config={"topic.regex": regex, "topic.replacement": replacement},
    )


def _lineages(
    connector_config: Dict[str, str], topic_names: List[str]
) -> List[Tuple[str, Optional[str], str]]:
    manifest = ConnectorManifest(
        name="inventory-cdc",
        type=SOURCE,
        config=connector_config,
        tasks=[],
        topic_names=topic_names,
    )
    connector = ConnectorRegistry.get_connector_for_manifest(
        manifest,
        KafkaConnectSourceConfig(connect_uri="http://localhost:8083", env="PROD"),
        KafkaConnectSourceReport(),
    )
    assert connector is not None
    return [
        (lin.source_platform, lin.source_dataset, lin.target_dataset)
        for lin in connector.extract_lineages()
    ]


class TestDebeziumLogicalTopicRouterPlugin:
    @pytest.mark.parametrize("type_", [BY_LOGICAL, TO_LOGICAL])
    def test_supported_types(self, type_: str) -> None:
        assert DebeziumLogicalTopicRouterPlugin.supports_transform_type(type_)
        assert isinstance(
            TransformPluginRegistry().get_plugin(type_),
            DebeziumLogicalTopicRouterPlugin,
        )

    def test_shards_merge_into_one_topic(self) -> None:
        result = DebeziumLogicalTopicRouterPlugin().apply_forward(
            [
                "dbserver1.inventory.customers_shard1",
                "dbserver1.inventory.customers_shard2",
                "dbserver1.inventory.orders",
            ],
            _router(r"(.*)customers_shard(.*)", "$1customers_all_shards"),
        )
        assert result == [
            "dbserver1.inventory.customers_all_shards",
            "dbserver1.inventory.customers_all_shards",
            "dbserver1.inventory.orders",
        ]

    def test_requires_full_match(self) -> None:
        # Debezium uses matcher.matches(): a partial match must not rename.
        result = DebeziumLogicalTopicRouterPlugin().apply_forward(
            ["dbserver1.inventory.orders"], _router(r"inventory", "renamed")
        )
        assert result == ["dbserver1.inventory.orders"]

    def test_empty_result_keeps_topic(self) -> None:
        # Debezium logs "Routing regex returned an empty topic name,
        # propagating original record" and keeps the topic.
        result = DebeziumLogicalTopicRouterPlugin().apply_forward(
            ["dbserver1.inventory.orders"],
            _router(r"dbserver1\.inventory\.orders(.*)", "$1"),
        )
        assert result == ["dbserver1.inventory.orders"]

    @pytest.mark.parametrize(
        "options",
        [
            {},
            {"topic.regex": ".*"},
            {"topic.regex": ".*", "topic.replacement": ""},
            {"topic.regex": ".*", "topic.replacement": "  "},
        ],
        ids=["no_options", "no_replacement", "empty_replacement", "blank_replacement"],
    )
    def test_missing_or_blank_options_are_a_noop(self, options: Dict[str, str]) -> None:
        # Debezium refuses to start the connector in all of these cases.
        config = TransformConfig(name="Reroute", type=BY_LOGICAL, config=options)
        result = DebeziumLogicalTopicRouterPlugin().apply_forward(
            ["dbserver1.inventory.orders"], config
        )
        assert result == ["dbserver1.inventory.orders"]

    @patch(
        "datahub.ingestion.source.kafka_connect.transform_plugins.JAVA_REGEX_AVAILABLE",
        False,
    )
    def test_requires_jpype(self) -> None:
        with pytest.raises(
            ValueError, match="Debezium logical topic router requires JPype"
        ):
            DebeziumLogicalTopicRouterPlugin().apply_forward(
                ["dbserver1.inventory.orders"], _router(".*", "renamed")
            )


class TestTransformPipelineWithLogicalTopicRouter:
    def test_chained_with_regex_router(self) -> None:
        result = get_transform_pipeline().apply_forward(
            ["dbserver1.inventory.customers_shard1"],
            {
                "transforms": "Reroute,Prefix",
                "transforms.Reroute.type": TO_LOGICAL,
                "transforms.Reroute.topic.regex": r"(.*)customers_shard(.*)",
                "transforms.Reroute.topic.replacement": "$1customers_all_shards",
                "transforms.Prefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.Prefix.regex": "(.*)",
                "transforms.Prefix.replacement": "cdc.$1",
            },
        )
        assert result.topics == ["cdc.dbserver1.inventory.customers_all_shards"]
        assert result.warnings == []

    def test_invalid_regex_leaves_topics_unchanged(self) -> None:
        result = get_transform_pipeline().apply_forward(
            ["dbserver1.inventory.orders"],
            {
                "transforms": "Reroute",
                "transforms.Reroute.type": BY_LOGICAL,
                "transforms.Reroute.topic.regex": "[bad(",
                "transforms.Reroute.topic.replacement": "renamed",
            },
        )
        assert result.topics == ["dbserver1.inventory.orders"]
        assert "PatternSyntaxException" in result.warnings[0]


class TestDebeziumConnectorWithLogicalTopicRouter:
    @pytest.mark.parametrize(
        "topic_names",
        [[], ["cdc-inventory-orders", "cdc-inventory-customers"]],
        ids=["without_topic_list", "with_topic_list"],
    )
    def test_lineage_targets_routed_topic(self, topic_names: List[str]) -> None:
        assert _lineages(MYSQL_CONFIG, topic_names) == [
            ("mysql", "inventory.orders", "cdc-inventory-orders"),
            ("mysql", "inventory.customers", "cdc-inventory-customers"),
        ]

    def test_shards_merge_into_one_topic(self) -> None:
        config = {
            **MYSQL_CONFIG,
            "table.include.list": "inventory.customers_shard1,inventory.customers_shard2",
            "transforms.Reroute.topic.regex": r"(.*)customers_shard(.*)",
            "transforms.Reroute.topic.replacement": "$1customers_all_shards",
        }
        target = "dbserver1.inventory.customers_all_shards"
        assert _lineages(config, [target]) == [
            ("mysql", "inventory.customers_shard1", target),
            ("mysql", "inventory.customers_shard2", target),
        ]

    def test_confluent_cloud_cdc_connector(self) -> None:
        # Confluent Cloud's managed Debezium connectors accept only
        # ToLogicalTopicRouter, and ingestion gets no per-connector topic list.
        config = {
            "connector.class": "PostgresCdcSourceV2",
            "topic.prefix": "dbserver1",
            "database.dbname": "shop",
            "table.include.list": "public.orders_shard1,public.orders_shard2",
            "transforms": "Reroute",
            "transforms.Reroute.type": TO_LOGICAL,
            "transforms.Reroute.topic.regex": r"(.*)orders_shard\d+",
            "transforms.Reroute.topic.replacement": "$1orders_all_shards",
        }
        target = "dbserver1.public.orders_all_shards"
        assert _lineages(config, []) == [
            ("postgres", "shop.public.orders_shard1", target),
            ("postgres", "shop.public.orders_shard2", target),
        ]
