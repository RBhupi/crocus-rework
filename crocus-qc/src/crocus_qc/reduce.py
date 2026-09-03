"""Stage 1: reduce raw high-frequency Parquet to a dense 10-second statistical product.

The raw dataset is tens of billions of rows, so this stage is the only thing that touches
it and it must reduce it in a single pass: one ``read_parquet``, one ``GROUP BY``, every
variable and every statistic computed together. Nothing returns rows to Python -- the
statement ends in ``COPY ... TO``.

**Stage 1 performs no QA/QC.** No physical bounds, no instrument bounds, no spike or
flatline detection, no flags, no bitmask, no neighbouring-bucket arithmetic. The only
value-level preprocessing is normalizing the known missing sentinel to NULL, which is
required for the statistics to be correct at all.

The generated statement has the shape::

    raw   -> scan the one required partition, normalize missing values
    agg   -> one GROUP BY into 10-second UTC buckets, all statistics at once
    grid  -> 8640 bucket starts for the UTC day
    dense -> grid LEFT JOIN agg
"""

from __future__ import annotations

from datetime import date as Date
from datetime import datetime, timedelta, timezone

from .config import MISSING_SENTINEL, SENTINEL_TOLERANCE, AggregationPeriod, VariableSpec

__all__ = ["build_stage1_sql", "session_setup_sql", "raw_glob", "output_columns"]


def sql_literal(text: str) -> str:
    """Single-quote a SQL string literal, escaping embedded quotes."""
    return "'" + text.replace("'", "''") + "'"


def session_setup_sql(threads: int, memory_limit: str, temp_dir: str) -> str:
    """Session pragmas applied to every DuckDB connection.

    ``TimeZone`` is pinned to UTC so ``TIMESTAMPTZ`` literals and ``time_bucket``
    behave identically regardless of the compute node's local timezone -- without it
    the same input produces different output on different HPC nodes.
    """
    return "\n".join(
        [
            "SET TimeZone = 'UTC';",
            f"SET threads = {int(threads)};",
            f"SET memory_limit = {sql_literal(memory_limit)};",
            f"SET temp_directory = {sql_literal(temp_dir)};",
            "SET preserve_insertion_order = false;",
        ]
    )


#: The subdirectory of a dataset version root that holds the Hive-partitioned facts.
#:
#: ``--dataset`` names the version root (``.../wxt-aqt-production-v5``) rather than the
#: facts directory itself, for two reasons: it is the path every other document and
#: config in this project already writes, and it makes the "never write inside the raw
#: dataset" guard cover the whole versioned tree instead of only its facts subtree.
FACTS_DIR = "facts"


def raw_glob(dataset_root: str, sensor: str, vsn: str, day: Date) -> str:
    """Hive path for exactly one work unit, under a dataset version root.

    Every partition key is pinned except ``instrument``, which is an artefact of the
    ingest layout rather than part of the work unit. Naming the path this precisely is
    the strongest possible partition pruning: DuckDB opens only these files.
    """
    return (
        f"{dataset_root.rstrip('/')}/{FACTS_DIR}/sensor={sensor}/vsn={vsn}"
        f"/instrument=*/date={day:%Y-%m-%d}/*.parquet"
    )


def _day_bounds(day: Date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1)


def _ts(moment: datetime) -> str:
    return f"TIMESTAMPTZ '{moment:%Y-%m-%d %H:%M:%S}+00'"


# --------------------------------------------------------------------------------------
# Row selection
# --------------------------------------------------------------------------------------


def _present(spec: VariableSpec) -> str:
    """Predicate selecting this variable's rows that carry an actual measurement.

    The raw table is long-format, so a variable is identified by the
    ``measurement``/``field``/``value_type`` triple. "Present" additionally excludes
    missing values -- NULL for numerics (the numeric sentinel is already mapped to NULL
    in the ``raw`` CTE) and the variable's declared missing strings for text.

    This is missing-value normalization, not quality control: it decides what *is* an
    observation, not whether an observation is good.
    """
    parts = [
        f"measurement = {sql_literal(spec.measurement)}",
        f"field = {sql_literal(spec.field)}",
        f"value_type = {sql_literal(spec.value_type)}",
    ]
    if spec.is_string:
        parts.append("vs IS NOT NULL")
        parts.extend(f"vs <> {sql_literal(text)}" for text in spec.missing_strings)
    else:
        parts.append("v IS NOT NULL")
    return " AND ".join(parts)


# --------------------------------------------------------------------------------------
# Aggregation blocks, one per aggregation method
# --------------------------------------------------------------------------------------


def _spread(where: str, expression: str) -> str:
    """Wrap a spread statistic so it is NULL until two samples support it.

    Every spread expression here reports 0.0 for a single sample rather than NULL --
    ``STDDEV_POP`` by definition, and the circular form because one direction gives a
    resultant length of exactly 1, so ``LN(1)`` is 0. Zero is not the truth: it claims
    the instrument held perfectly steady, which a downstream ``raw_low_stdev`` check
    cannot distinguish from a genuinely stuck sensor. NULL says what is actually known.

    The count is recomputed here rather than referenced as ``{name}_n_samples`` because
    a SELECT list cannot refer to its own aliases; DuckDB evaluates both from the same
    grouped scan, so this costs no extra pass.
    """
    return f"CASE WHEN COUNT(*) FILTER (WHERE {where}) >= 2 THEN {expression} END"


def _aggregation_block(spec: VariableSpec) -> list[str]:
    """The grouped expressions for one variable, all evaluated in the same scan."""
    name, where = spec.name, _present(spec)
    source = "vs" if spec.is_string else "v"
    columns = [f"COUNT(*) FILTER (WHERE {where})::UINTEGER AS {name}_n_samples"]

    if spec.aggregation == "mean":
        columns.insert(0, f"AVG(v) FILTER (WHERE {where}) AS {name}")
        columns += [
            f"MIN(v) FILTER (WHERE {where}) AS {name}_raw_min",
            f"MAX(v) FILTER (WHERE {where}) AS {name}_raw_max",
            f"{_spread(where, f'STDDEV_POP(v) FILTER (WHERE {where})')} AS {name}_raw_std",
        ]
    elif spec.aggregation == "circular_mean":
        # A plain mean is wrong on a circular domain: 359 and 1 average to 180 (south)
        # instead of 0 (north). ATAN2 of the mean sine and mean cosine is correct, and
        # returns (-180, 180]; the double modulo normalizes into [0, 360) despite
        # DuckDB's signed float modulo. MIN/MAX are undefined on a circle and omitted.
        sin_mean = f"AVG(SIN(RADIANS(v))) FILTER (WHERE {where})"
        cos_mean = f"AVG(COS(RADIANS(v))) FILTER (WHERE {where})"
        resultant = f"SQRT(POWER({sin_mean}, 2) + POWER({cos_mean}, 2))"
        columns.insert(
            0, f"((DEGREES(ATAN2({sin_mean}, {cos_mean})) % 360.0) + 360.0) % 360.0 AS {name}"
        )
        # Circular standard deviation from the mean resultant length R. R is bounded by
        # [0, 1] by definition, but neither bound survives floating point: as directions
        # cancel R tends to 0 and LN(0) diverges, and summing many identical unit vectors
        # rounds R to 1 + 2e-16, which makes -2*LN(R) negative and SQRT raise. LEAST caps
        # R at 1 (a calm bucket has zero spread) and GREATEST floors it above 0.
        r_clamped = f"GREATEST(LEAST({resultant}, 1.0), 1e-15)"
        columns.append(
            f"{_spread(where, f'DEGREES(SQRT(-2.0 * LN({r_clamped})))')} "
            f"AS {name}_raw_std"
        )
    elif spec.aggregation == "mode":
        columns.insert(0, f"MODE(v) FILTER (WHERE {where}) AS {name}")
    elif spec.aggregation == "last":
        # Latest observation in the bucket by its real timestamp, not by file order.
        columns.insert(0, f"MAX_BY({source}, obs_time) FILTER (WHERE {where}) AS {name}")
    else:  # pragma: no cover - load_profile rejects anything else
        raise ValueError(f"unsupported aggregation {spec.aggregation!r} for {spec.name!r}")

    return columns


def output_columns(spec: VariableSpec) -> list[str]:
    """Product column names for one variable, in output order.

    Statistics that are not scientifically meaningful for the variable's aggregation
    method are absent rather than present-and-null.
    """
    columns = [spec.name, f"{spec.name}_n_samples"]
    if spec.aggregation == "mean":
        columns += [f"{spec.name}_raw_min", f"{spec.name}_raw_max", f"{spec.name}_raw_std"]
    elif spec.aggregation == "circular_mean":
        columns.append(f"{spec.name}_raw_std")
    return columns


def _dense_projection(spec: VariableSpec) -> list[str]:
    """Columns after the LEFT JOIN onto the dense grid.

    A bucket with no observations still produces a row: the count falls back to zero and
    every statistic stays NULL. Nothing is interpolated and no timestamp is invented.
    """
    return [
        f"COALESCE(a.{name}, 0)::UINTEGER AS {name}"
        if name.endswith("_n_samples")
        else f"a.{name}"
        for name in output_columns(spec)
    ]


# --------------------------------------------------------------------------------------
# Statement assembly
# --------------------------------------------------------------------------------------


def build_stage1_sql(
    *,
    dataset_root: str,
    sensor: str,
    vsn: str,
    day: Date,
    variables: tuple[VariableSpec, ...],
    period: AggregationPeriod,
    output_path: str,
) -> str:
    """Build the single Stage 1 statement: raw Parquet in, 10-second Parquet out."""
    if not variables:
        raise ValueError("stage 1 requires at least one variable")

    start, end = _day_bounds(day)
    last_bucket_start = end - timedelta(seconds=period.seconds)
    measurements = ", ".join(
        sql_literal(m) for m in sorted({s.measurement for s in variables})
    )
    indent = ",\n        "

    aggregates = indent.join(c for s in variables for c in _aggregation_block(s))
    dense = indent.join(c for s in variables for c in _dense_projection(s))

    return f"""
COPY (
    WITH raw AS (
        SELECT
            time AS obs_time,
            measurement,
            field,
            value_type,
            CASE
                WHEN value_float64 IS NULL THEN NULL
                WHEN abs(value_float64 - ({MISSING_SENTINEL!r})) < {SENTINEL_TOLERANCE!r} THEN NULL
                ELSE value_float64
            END AS v,
            value_string AS vs
        FROM read_parquet(
            {sql_literal(raw_glob(dataset_root, sensor, vsn, day))},
            hive_partitioning = true,
            union_by_name = false
        )
        WHERE time >= {_ts(start)}
          AND time <  {_ts(end)}
          AND measurement IN ({measurements})
    ),
    agg AS (
        SELECT
            time_bucket(INTERVAL '{period.raw}', obs_time, {_ts(start)}) AS bucket,
            {aggregates}
        FROM raw
        GROUP BY bucket
    ),
    grid AS (
        SELECT t AS bucket
        FROM generate_series(
            {_ts(start)},
            {_ts(last_bucket_start)},
            INTERVAL '{period.raw}'
        ) s(t)
    )
    SELECT
        g.bucket AS time,
        {dense}
    FROM grid g
    LEFT JOIN agg a ON a.bucket = g.bucket
    ORDER BY g.bucket
) TO {sql_literal(output_path)} (FORMAT parquet, COMPRESSION zstd);
""".strip()
