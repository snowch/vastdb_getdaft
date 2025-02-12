import vastdb
import daft
import pyarrow as pa
from typing import Iterator, Dict, Any

def read_from_vastdb(config: Dict[str, Any], table_name: str) -> daft.DataFrame:
    """
    Read data from VAST DB into a Daft DataFrame.
    
    Args:
        config: Dictionary containing:
            - endpoint: VAST DB endpoint URL
            - access: AWS access key ID
            - secret: AWS secret access key
            - bucket: Bucket name
            - schema: Schema name
        table_name: Table name
            
    Returns:
        daft.DataFrame
    """
    def vast_table_reader() -> pa.Table:
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

    return daft.from_arrow(vast_table_reader())
