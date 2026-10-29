import io

import pyarrow as pa
import pyarrow.parquet as pq


def records_to_parquet_bytes(
    records: list[dict],
    compression: str = "snappy",
) -> bytes:
    """Convert a list of dicts to Parquet bytes (schema inferred by pyarrow)."""
    table = pa.Table.from_pylist(records)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression=compression)
    return buffer.getvalue()
