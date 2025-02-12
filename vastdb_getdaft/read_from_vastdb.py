import vastdb
import daft
import pyarrow as pa
from typing import Iterator, Dict, Any

def read_from_vastdb(config: Dict[str, Any]) -> daft.DataFrame:
    """
    Read data from VAST DB into a Daft DataFrame.
    
    Args:
        config: Dictionary containing:
            - endpoint: VAST DB endpoint URL
            - access: AWS access key ID
            - secret: AWS secret access key
            - bucket: Bucket name
            - schema: Schema name
            - table: Table name
            
    Returns:
        daft.DataFrame
    """
    def record_batch_generator() -> Iterator[pa.RecordBatch]:
        session = vastdb.connect(
            endpoint=config['endpoint'],
            access=config['access'],
            secret=config['secret']
        )
        
        with session.transaction() as tx:
            bucket = tx.bucket(config['bucket'])
            schema = bucket.schema(config['schema'])
            table = schema.table(config['table'])
            
            # Stream data using VAST DB's select method
            reader = table.select()
            return reader.read_all()
            
            # # Yield record batches
            # while True:
            #     try:
            #         batch = reader.read_next_batch()
            #         if batch is None:
            #             break
            #         yield batch
            #     except Exception as e:
            #         print(f"Error reading batch: {e}")
            #         break
    
    # Create Daft DataFrame from the record batch generator
    return daft.from_arrow(record_batch_generator())
