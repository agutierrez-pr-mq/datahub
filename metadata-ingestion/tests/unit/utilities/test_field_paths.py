import pytest

from datahub.ingestion.extractor.json_schema_util import JsonSchemaTranslator
from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


@pytest.mark.parametrize(
    "field_path, expected",
    [
        ("simple.path", "simple.path"),
        ("[version=2.0].[type=Record].[type=string].name", "name"),
        # JSON Schema titles keep dots, e.g. Debezium <prefix>.<db>.<table>.Envelope
        ("[version=2.0].[type=com.example.Order].[type=string].order_id", "order_id"),
        (
            (
                "[version=2.0].[type=dbserver1.inventory.customers.Envelope]"
                ".[type=union].[type=dbserver1.inventory.customers.Value]"
                ".after.[type=integer].id"
            ),
            "after.id",
        ),
        (
            "[version=2.0].[type=Order].[type=number].price[usd]_total",
            "price[usd]_total",
        ),
        ("[version=2.0].[type=R].a]b.[type=string].c", "a]b.c"),
    ],
)
def test_get_simple_field_path_from_v2_field_path(
    field_path: str, expected: str
) -> None:
    assert get_simple_field_path_from_v2_field_path(field_path) == expected


def test_simple_field_paths_from_json_schema_with_dotted_titles() -> None:
    # Shape of a Debezium change event serialized by Confluent JsonSchemaConverter
    schema = {
        "type": "object",
        "title": "dbserver1.inventory.customers.Envelope",
        "properties": {
            "after": {
                "oneOf": [
                    {"type": "null"},
                    {
                        "type": "object",
                        "title": "dbserver1.inventory.customers.Value",
                        "properties": {"id": {"type": "integer"}},
                    },
                ]
            },
        },
    }
    paths = {
        get_simple_field_path_from_v2_field_path(f.fieldPath)
        for f in JsonSchemaTranslator.get_fields_from_schema(schema)
    }
    assert paths == {"after", "after.id"}
