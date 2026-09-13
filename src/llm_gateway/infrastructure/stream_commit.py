"""Content-free first-delta reservation using the Invocation transaction fence."""

from uuid import UUID

from llm_gateway.domain.streaming import StreamDelta


class PostgresStreamCommit:
    def __init__(self, store, call_id):
        if not isinstance(call_id, UUID) or call_id.version != 4:
            raise ValueError("Invocation UUID required")
        self._store, self._call_id = store, call_id

    async def first_delta(self, number: int, event: StreamDelta) -> None:
        if (type(number) is not int or not 1 <= number <= 3 or not isinstance(event, StreamDelta)
                or event.sequence != 1 or len(event.resolved_model) > 256):
            raise ValueError("First business delta of a valid Attempt required")
        async with self._store.transaction() as connection:
            await connection.execute("""
                INSERT INTO invocation_stream_commit(call_id,number,resolved_model,delta_kind)
                VALUES (%s,%s,%s,%s)
                """, (self._call_id, number, event.resolved_model, event.kind))
        # Returning here means COMMIT succeeded. It says nothing about receipt.
