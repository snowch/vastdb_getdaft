import vastdb
import daft
import pyarrow as pa
from typing import Dict, Any

def read_from_vastdb(config: Dict[str, Any], table_name: str = None) -> daft.DataFrame:
    """
    Read data from VAST DB into a Daft DataFrame.
    
    Args:
        config: Dictionary containing:
            - endpoint: VAST DB endpoint URL
            - access: AWS access key ID
            - secret: AWS secret access key
            - bucket: Bucket name
            - schema: Schema name
            - table (optional): Default table name
        table_name: (Optional) Table name to override config['table'].
        
    Returns:
        daft.DataFrame
    """
    # Use table_name if provided, otherwise fall back to config
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

        paTable: pa.Table = table.select().read_all()
        return daft.from_arrow(table)

