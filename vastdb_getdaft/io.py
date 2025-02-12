from typing import Dict, Any
import vastdb
import pyarrow as pa
import daft

def read_parquet_table(config: Dict[str, Any], table_name: str = None) -> pa.Table:
    """
    Reads data from VAST DB as a PyArrow Table.
    
    Args:
        config: Dictionary containing VAST DB connection details.
        table_name: (Optional) Table name to override config['table'].

    Returns:
        pa.Table: A PyArrow Table containing the data.
    """
    table_name = table_name or config.get('table')

    if not table_name:
        raise ValueError("Table name must be provided either in config or as an argument.")

    session = vastdb.connect(
        endpoint=config['endpoint'],
        access=config['access'],
        secret=config['secret']
    )
    
    with session.transaction() as tx:
        bucket = tx.bucket(config['bucket'])
        schema = bucket.schema(config['schema'])
        table = schema.table(table_name)

        return table.select().read_all()

def read_record_batch_reader(config: Dict[str, Any], table_name: str = None) -> pa.RecordBatchReader:
    """
    Reads data from VAST DB as a PyArrow RecordBatchReader.
    
    Args:
        config: Dictionary containing VAST DB connection details.
        table_name: (Optional) Table name to override config['table'].

    Returns:
        pa.RecordBatchReader: A PyArrow RecordBatchReader containing the data.
    """
    table_name = table_name or config.get('table')

    if not table_name:
        raise ValueError("Table name must be provided either in config or as an argument.")

    session = vastdb.connect(
        endpoint=config['endpoint'],
        access=config['access'],
        secret=config['secret']
    )
    
    with session.transaction() as tx:
        bucket = tx.bucket(config['bucket'])
        schema = bucket.schema(config['schema'])
        table = schema.table(table_name)

        return table.select()
