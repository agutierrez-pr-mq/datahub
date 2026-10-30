"""Tests for Debezium ByLogicalTableRouter / ToLogicalTopicRouter support."""

from unittest.mock import Mock

import jpype
import jpype.imports  # enables `from java...` imports
import pytest

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
from datahub.ingestion.source.kafka_connect.transform_plugins import (
    DebeziumLogicalTopicRouterPlugin,
    TransformConfig,
    TransformPluginRegistry,
    get_transform_pipeline,
)

BY_LOGICAL = "io.debezium.transforms.ByLogicalTableRouter"
TO_LOGICAL = "io.debezium.transforms.ToLogicalTopicRouter"


@pytest.fixture(scope="module", autouse=True)
def ensure_jvm_started():
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())
    yield


def _router(regex: str, replacement: str, type_: str = BY_LOGICAL) -> TransformConfig:
    return TransformConfig(
        name="Reroute",
        type=type_,
        config={"topic.regex": regex, "topic.replacement": replacement},
    )


class TestDebeziumLogicalTopicRouterPlugin:
    @pytest.mark.parametrize("type_", [BY_LOGICAL, TO_LOGICAL])
    def test_supported_types(self, type_: str) -> None:
        assert DebeziumLogicalTopicRouterPlugin.supports_transform_type(type_)
        assert isinstance(
            TransformPluginRegistry().get_plugin(type_),
            DebeziumLogicalTopicRouterPlugin,
        )

    def test_applied_automatically(self) -> None:
        assert DebeziumLogicalTopicRouterPlugin.should_apply_automatically()

    def test_shards_merge_into_one_topic(self) -> None:
        plugin = DebeziumLogicalTopicRouterPlugin()
        result = plugin.apply_forward(
            [
                "dbserver.inventory.customers_shard1",
                "dbserver.inventory.customers_shard2",
            ],
            _router(r"(.*)customers_shard(.*)", "$1customers_all_shards"),
        )
        assert result == ["dbserver.inventory.customers_all_shards"] * 2

    def test_requires_full_match(self) -> None:
        # Debezium uses matcher.matches(): a partial match must not rename.
        plugin = DebeziumLogicalTopicRouterPlugin()
        result = plugin.apply_forward(
            ["dbserver.inventory.orders"], _router(r"inventory", "renamed")
        )
        assert result == ["dbserver.inventory.orders"]

    def test_empty_replacement_keeps_topic(self) -> None:
        plugin = DebeziumLogicalTopicRouterPlugin()
        result = plugin.apply_forward(["a.b.c"], _router(r".*", ""))
        assert result == ["a.b.c"]

    def test_missing_config_is_noop(self) -> None:
        plugin = DebeziumLogicalTopicRouterPlugin()
        config = TransformConfig(name="Reroute", type=BY_LOGICAL, config={})
        assert plugin.apply_forward(["a.b.c"], config) == ["a.b.c"]

    def test_pipeline_has_no_unknown_transform_warning(self) -> None:
        result = get_transform_pipeline().apply_forward(
            ["dbserver.inventory.orders"],
            {
                "transforms": "Reroute",
                "transforms.Reroute.type": BY_LOGICAL,
                "transforms.Reroute.topic.regex": r"dbserver\.(.*)\.(.*)",
                "transforms.Reroute.topic.replacement": "cdc-$1-$2",
            },
        )
        assert result.topics == ["cdc-inventory-orders"]
        assert result.warnings == []


class TestDebeziumConnectorWithLogicalTopicRouter:
    def _connector(self, topic_names: list) -> DebeziumSourceConnector:
        manifest = ConnectorManifest(
            name="inventory-cdc",
            type="source",
            config={
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "topic.prefix": "dbserver",
                "table.include.list": "inventory.orders,inventory.customers",
                "transforms": "Reroute",
                "transforms.Reroute.type": BY_LOGICAL,
                "transforms.Reroute.topic.regex": r"dbserver\.(.*)\.(.*)",
                "transforms.Reroute.topic.replacement": "cdc-$1-$2",
            },
            tasks=[],
            topic_names=topic_names,
        )
        config = Mock(spec=KafkaConnectSourceConfig)
        config.use_schema_resolver = False
        config.schema_resolver_expand_patterns = True
        config.schema_resolver_finegrained_lineage = True
        config.env = "PROD"
        return DebeziumSourceConnector(
            manifest, config, Mock(spec=KafkaConnectSourceReport)
        )

    def test_lineage_targets_routed_topic(self) -> None:
        connector = self._connector(["cdc-inventory-orders", "cdc-inventory-customers"])
        lineages = connector.extract_lineages()

        assert [(lin.source_dataset, lin.target_dataset) for lin in lineages] == [
            ("inventory.orders", "cdc-inventory-orders"),
            ("inventory.customers", "cdc-inventory-customers"),
        ]
        assert all(lin.source_platform == "mysql" for lin in lineages)

    def test_unrouted_topics_no_longer_used(self) -> None:
        # Before this change the default dbserver.* names were emitted. With only
        # the routed topics in the runtime list, nothing should point at dbserver.*.
        connector = self._connector(["cdc-inventory-orders"])
        lineages = connector.extract_lineages()

        assert [lin.target_dataset for lin in lineages] == ["cdc-inventory-orders"]
