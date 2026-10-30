import pytest

from datahub.utilities.urns.field_paths import get_simple_field_path_from_v2_field_path


@pytest.mark.parametrize(
    "field_path, expected",
    [
        ("simple.path", "simple.path"),
        ("[version=2.0].[type=Record].[type=string].name", "name"),
        ("[version=2.0].[key=True].[type=Key].[type=int].id", "id"),
        (
            "[version=2.0].[type=Record].[type=array].[type=string].tags",
            "tags",
        ),
        # Record names with dots (Debezium default: <server>.<db>.<table>.Envelope)
        (
            (
                "[version=2.0].[type=dbserver1.inventory.customers.Envelope].[type=union]"
                ".[type=dbserver1.inventory.customers.Value].after.[type=int].id"
            ),
            "after.id",
        ),
        (
            (
                "[version=2.0].[type=t.Envelope].[type=io.debezium.connector.mysql.Source]"
                ".source.[type=integer].ts_ms"
            ),
            "source.ts_ms",
        ),
    ],
)
def test_get_simple_field_path_from_v2_field_path(
    field_path: str, expected: str
) -> None:
    assert get_simple_field_path_from_v2_field_path(field_path) == expected
