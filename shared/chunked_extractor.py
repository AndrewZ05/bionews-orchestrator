"""
Chunked Parquet sink for memory-safe record accumulation.

Replaces the ``all_records = []; all_records.extend(batch)`` anti-pattern
found in every extractor.  Records are flushed to numbered Parquet chunk
files whenever the buffer exceeds a configurable threshold, then merged
into a single output file on finalisation.

Uses ``shared.gcs_pipeline.extract_to_local_parquet()`` for each chunk.
"""

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

_DEFAULT_FLUSH_THRESHOLD = 50_000


class ChunkedParquetSink:
    """
    Accumulates records and flushes to Parquet every *flush_threshold* rows.

    Usage::

        sink = ChunkedParquetSink(
            source='wordpress', table='posts',
            execution_id=exec_id, bq_client=bq_client,
            production_table_id='project.dataset.posts',
            rebuild_mode=False,
        )
        for batch in extraction_batches:
            sink.add(batch)
        parquet_path, total_rows = sink.finalize()
    """

    def __init__(
        self,
        source: str,
        table: str,
        execution_id: str,
        bq_client: Any,
        production_table_id: str,
        rebuild_mode: bool = False,
        flush_threshold: int = _DEFAULT_FLUSH_THRESHOLD,
    ):
        self.source = source
        self.table = table
        self.execution_id = execution_id
        self.bq_client = bq_client
        self.production_table_id = production_table_id
        self.rebuild_mode = rebuild_mode
        self.flush_threshold = flush_threshold

        self.buffer: List[Dict[str, Any]] = []
        self.chunk_files: List[str] = []
        self.total_rows = 0
        self._chunk_index = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, records: List[Dict[str, Any]]) -> None:
        """Add records to the buffer, flushing if threshold is exceeded."""
        self.buffer.extend(records)
        while len(self.buffer) >= self.flush_threshold:
            self._flush()

    def finalize(self) -> Tuple[Optional[str], int]:
        """
        Flush remaining buffer and return ``(parquet_path, total_rows)``.

        If multiple chunks were written they are merged into one file.
        Returns ``(None, 0)`` if no records were accumulated.
        """
        # Flush any remaining records
        if self.buffer:
            self._flush()

        if not self.chunk_files:
            return None, 0

        # Single chunk – just return it directly
        if len(self.chunk_files) == 1:
            return self.chunk_files[0], self.total_rows

        # Multiple chunks – merge into single Parquet file
        merged_path = self._merge_chunks()
        return merged_path, self.total_rows

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _flush(self) -> None:
        """Write current buffer to a numbered Parquet chunk file."""
        if not self.buffer:
            return

        from shared.gcs_pipeline import extract_to_local_parquet

        chunk_records = self.buffer[:self.flush_threshold]
        self.buffer = self.buffer[self.flush_threshold:]

        row_count = len(chunk_records)
        logger.debug(
            f"Flushing chunk {self._chunk_index} for {self.table}: "
            f"{row_count:,} records"
        )

        path = extract_to_local_parquet(
            data=chunk_records,
            source=self.source,
            table=f"{self.table}_chunk{self._chunk_index}",
            job_id=self.execution_id,
            bq_client=self.bq_client,
            production_table_id=self.production_table_id,
            rebuild_mode=self.rebuild_mode,
        )

        if path:
            self.chunk_files.append(path)
            self.total_rows += row_count
            self._chunk_index += 1
        else:
            logger.warning(
                f"Chunk {self._chunk_index} for {self.table} produced no output"
            )

    def _merge_chunks(self) -> str:
        """Merge multiple Parquet chunk files into a single file."""
        tables = []
        for chunk_path in self.chunk_files:
            try:
                tables.append(pq.read_table(chunk_path))
            except Exception as e:
                logger.warning(f"Failed to read chunk {chunk_path}: {e}")

        if not tables:
            return self.chunk_files[0]  # fallback

        import pyarrow as pa
        merged_table = pa.concat_tables(tables, promote_options='default')

        # Write merged file
        merged_dir = os.path.dirname(self.chunk_files[0])
        merged_path = os.path.join(merged_dir, f"{self.table}_merged.parquet")
        pq.write_table(merged_table, merged_path)

        # Clean up chunk files
        for chunk_path in self.chunk_files:
            try:
                os.remove(chunk_path)
            except OSError:
                pass

        logger.info(
            f"Merged {len(self.chunk_files)} chunks into {merged_path} "
            f"({self.total_rows:,} total rows)"
        )
        return merged_path


__all__ = [
    'ChunkedParquetSink',
]
