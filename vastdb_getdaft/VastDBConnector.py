from typing import Any, Dict, List, Optional, Iterator
from daft.connectors import Connector
from daft.logical.builder import ExpressionBuilder
import vastdb
import pyarrow as pa
from ibis import _
from datetime import date, time, datetime
from decimal import Decimal

class VastDBConnector(Connector):
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the VAST DB connector.
        
        Args:
            config: Dictionary containing:
                - endpoint: VAST DB endpoint URL
                - access: AWS access key ID
                - secret: AWS secret access key
                - bucket: Bucket name
                - schema: Schema name
                - table: Table name
        """
        super().__init__()
        self.config = config
        self._validate_config()
        self.session = None
        self.table = None
        self._connect()
        
    def _validate_config(self):
        """Validate the provided configuration."""
        required_keys = ['endpoint', 'access', 'secret', 'bucket', 'schema', 'table']
        for key in required_keys:
            if key not in self.config:
                raise ValueError(f"Missing required configuration key: {key}")
    
    def _connect(self):
        """Establish connection to VAST DB and get table reference."""
        self.session = vastdb.connect(
            endpoint=self.config['endpoint'],
            access=self.config['access'],
            secret=self.config['secret']
        )
        
        with self.session.transaction() as tx:
            bucket = tx.bucket(self.config['bucket'])
            schema = bucket.schema(self.config['schema'])
            self.table = schema.table(self.config['table'])
    
    def _convert_filter(self, filter_expr: ExpressionBuilder) -> Any:
        """
        Convert Daft filter expressions to VAST DB predicates using ibis expressions.
        
        Args:
            filter_expr: The Daft filter expression to convert
            
        Returns:
            VAST DB compatible predicate
        """
        if not filter_expr:
            return None
            
        def convert_single_predicate(expr):
            """Convert a single predicate expression."""
            op = expr.op
            col = expr.col
            val = expr.val
            
            # Get column reference
            col_ref = getattr(_, col)
            
            # Handle different operation types
            if op == "eq":
                return col_ref == val
            elif op == "gt":
                return col_ref > val
            elif op == "lt":
                return col_ref < val
            elif op == "ge":
                return col_ref >= val
            elif op == "le":
                return col_ref <= val
            elif op == "ne":
                return col_ref != val
            elif op == "isin":
                return col_ref.isin(val)
            elif op == "isnull":
                return col_ref.isnull()
            elif op == "notnull":
                return ~col_ref.isnull()
            elif op == "startswith":
                return col_ref.startswith(val)
            elif op == "contains":
                return col_ref.contains(val)
            else:
                raise ValueError(f"Unsupported operation: {op}")
                
        def combine_predicates(predicates, operator):
            """Combine multiple predicates with AND or OR."""
            if not predicates:
                return None
            
            result = predicates[0]
            for pred in predicates[1:]:
                if operator == "and":
                    result = result & pred
                elif operator == "or":
                    result = result | pred
                    
            return result
            
        def process_expression(expr):
            """Process a complex expression tree."""
            if hasattr(expr, "op"):
                # Single predicate
                return convert_single_predicate(expr)
            elif hasattr(expr, "predicates"):
                # Compound predicate
                converted_preds = [process_expression(p) for p in expr.predicates]
                return combine_predicates(converted_preds, expr.operator)
            else:
                raise ValueError(f"Unsupported expression type: {type(expr)}")
                
        try:
            return process_expression(filter_expr)
        except Exception as e:
            raise ValueError(f"Failed to convert filter expression: {str(e)}")
    
    def scan(
        self,
        projection: Optional[List[str]] = None,
        filter_expr: Optional[ExpressionBuilder] = None,
        **kwargs
    ) -> Iterator[pa.RecordBatch]:
        """
        Scan data from VAST DB.
        
        Args:
            projection: List of columns to retrieve
            filter_expr: Filter expression to apply
            **kwargs: Additional scan parameters
            
        Returns:
            Iterator of PyArrow RecordBatch objects
        """
        if not self.session:
            self._connect()
            
        with self.session.transaction() as tx:
            predicate = self._convert_filter(filter_expr) if filter_expr else None
            
            reader = self.table.select(
                columns=projection,
                predicate=predicate
            )
            
            while True:
                try:
                    batch = reader.read_next_batch()
                    if batch is None:
                        break
                    yield batch
                except StopIteration:
                    break

    def get_schema(self) -> Dict[str, Any]:
        """
        Get the schema of the VAST DB table.
        
        Returns:
            Dictionary describing the table schema
        """
        if not self.session:
            self._connect()
            
        supported_types = {
            pa.int8(),
            pa.int16(),
            pa.int32(),
            pa.int64(),
            pa.float32(),
            pa.float64(),
            pa.string(),
            pa.bool_(),
            pa.decimal128(38, 10),  # Example precision/scale
            pa.binary(),
            pa.date32(),
            pa.time32('ms'),
            pa.time64('us'),
            pa.timestamp('us')
        }
            
        with self.session.transaction() as tx:
            columns = self.table.columns()
            
            schema_info = {
                "columns": []
            }
            
            for col in columns:
                if col.type not in supported_types:
                    raise ValueError(f"Unsupported column type {col.type} for column {col.name}")
                    
                schema_info["columns"].append({
                    "name": col.name,
                    "type": str(col.type)
                })
                
            return schema_info
