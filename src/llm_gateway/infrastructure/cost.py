"""Pricing and initial accrual writes inside the owning Attempt transaction."""

from datetime import datetime

from llm_gateway.domain.cost import PricingBasis, calculate_cost
from llm_gateway.domain.invocation import InvocationPersistenceUnavailable
from llm_gateway.domain.model import Usage


async def lock_attempt_pricing(connection, call_id, number, binding_id):
    cursor = await connection.execute("""
        SELECT i.configuration_revision,c.snapshot FROM model_invocation i
        JOIN config_revision c ON c.revision=i.configuration_revision WHERE i.call_id=%s
        """, (call_id,))
    row = await cursor.fetchone()
    if row is None:
        raise InvocationPersistenceUnavailable()
    try:
        resource_id = row[1]["provider_model_bindings"][binding_id]["pricing_table"]
        table = row[1]["pricing_tables"][resource_id]
        if table["unit"] != "per_million_tokens" or table["rounding"] != "half_even_12dp":
            raise ValueError()
        rates = table["rates"]
        pricing = PricingBasis(str(row[0]), resource_id, table["currency"],
            datetime.fromisoformat(table["effective_from"].replace("Z", "+00:00")),
            rates["input"], rates["output"], rates["cached_input"], rates["reasoning_output"])
    except (KeyError, TypeError, ValueError, AttributeError):
        raise InvocationPersistenceUnavailable() from None
    cursor = await connection.execute("""
        INSERT INTO attempt_pricing(call_id,number,configuration_revision,pricing_resource_id,currency,
            effective_from,input_rate,output_rate,cached_input_rate,reasoning_output_rate)
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,%s WHERE %s::timestamptz <= statement_timestamp()
        RETURNING number
        """, (call_id, number, int(pricing.configuration_revision), pricing.resource_id, pricing.currency,
              pricing.effective_from, *pricing.rates, pricing.effective_from))
    if await cursor.fetchone() is None:
        raise InvocationPersistenceUnavailable()


async def accrue_attempt_cost(connection, call_id, number, usage: Usage):
    cursor = await connection.execute("""
        SELECT configuration_revision,pricing_resource_id,currency,effective_from,input_rate,output_rate,
            cached_input_rate,reasoning_output_rate FROM attempt_pricing WHERE call_id=%s AND number=%s
        """, (call_id, number))
    row = await cursor.fetchone()
    if row is None:
        raise InvocationPersistenceUnavailable()
    cost = calculate_cost(PricingBasis(str(row[0]), *row[1:]), usage)
    await connection.execute("""
        INSERT INTO cost_accrual(call_id,number,usage_source,input_cost,output_cost,cached_cost,
            reasoning_cost,total_cost,certainty,completeness) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (call_id, number, cost.usage_source, cost.input_cost, cost.output_cost, cost.cached_cost,
              cost.reasoning_cost, cost.total_cost, cost.certainty, cost.completeness))


async def summarize_invocation_cost(connection, call_id):
    """Append terminal totals from all accruals; unknown is never zero."""
    await connection.execute("""
        INSERT INTO invocation_cost_summary(call_id,currency,attempt_count,input_cost,output_cost,cached_cost,
            reasoning_cost,total_cost,completeness,certainty)
        SELECT c.call_id,p.currency,count(*),
            CASE WHEN count(c.input_cost)=count(*) THEN sum(c.input_cost) END,
            CASE WHEN count(c.output_cost)=count(*) THEN sum(c.output_cost) END,
            CASE WHEN count(c.cached_cost)=count(*) THEN sum(c.cached_cost) END,
            CASE WHEN count(c.reasoning_cost)=count(*) THEN sum(c.reasoning_cost) END,
            CASE WHEN count(c.total_cost)=count(*) THEN sum(c.total_cost) END,
            CASE WHEN bool_and(c.completeness='complete') THEN 'complete'
                 WHEN bool_and(c.completeness='unavailable') THEN 'unavailable' ELSE 'partial' END,
            CASE WHEN bool_and(c.certainty='unavailable') THEN 'unavailable' ELSE 'estimated' END
        FROM cost_accrual c JOIN attempt_pricing p USING(call_id,number)
        WHERE c.call_id=%s GROUP BY c.call_id,p.currency
        """, (call_id,))
