import pytest
from unittest.mock import MagicMock, patch
from postgres_cdc.pg_replication.helpers import MessageConsumer, Relation
from postgres_cdc.pg_replication.decoders import ColumnType

@pytest.fixture
def consumer():
    ops = {"insert": True, "update": True, "delete": True, "truncate": False}
    return MessageConsumer(upto_lsn=100, pub_ops=ops)

def test_force_append_only_logic(consumer):
    """
    Ensure the pipeline enforces 'append' mode and disables 'hard_delete' hints
    to preserve historical data (soft-delete), overriding default dlt behavior.
    """
    mock_relation = MagicMock(spec=Relation)
    mock_relation.relation_id = 123
    mock_relation.relation_name = "test_table"
    mock_relation.columns = [
        ColumnType(part_of_pkey=1, name="id", type_id=23, atttypmod=-1),
        ColumnType(part_of_pkey=0, name="name", type_id=25, atttypmod=-1)
    ]

    with patch("postgres_cdc.pg_replication.helpers.dlt.mark.make_hints") as mock_hints, \
         patch("postgres_cdc.pg_replication.helpers._to_dlt_column_schema", return_value={"data_type": "text"}):
        
        consumer.process_relation(mock_relation)

        # Check write_disposition override
        call_kwargs = mock_hints.call_args[1] 
        assert call_kwargs["write_disposition"] == "append"

        # Check hard_delete disabled
        schema_columns = consumer.last_table_schema[123]["columns"]
        assert schema_columns["deleted_ts"].get("hard_delete") is not True