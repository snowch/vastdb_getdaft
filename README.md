# vastdb_getdaft

## Installation

```bash
!pip3 install --upgrade --quiet git+https://github.com/snowch/vastdb_getdaft.git --use-pep517
```

## Example

```python
import daft
from vastdb_getdaft import VastDBConnector

# Configure your connector
config = {
    "endpoint": "http://vip-pool.v123-xy.VastENG.lab",
    "access": "YOUR_AWS_ACCESS_KEY_ID",
    "secret": "YOUR_AWS_SECRET_ACCESS_KEY",
    "bucket": "bucket-name",
    "schema": "schema-name",
    "table": "table-name"
}

# Create connector instance
connector = VastDBConnector(config)

# Use it directly with Daft
df = daft.read(connector)

# Use it like any other Daft dataframe
filtered_df = df.filter(daft.col("some_column") > 10)
result = filtered_df.collect()
```
