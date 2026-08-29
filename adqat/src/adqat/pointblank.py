from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pointblank as pb
import polars as pl
import pyarrow as pa

from adqat.config import CheckDefinition, ResolvedConfig


class PointblankError(RuntimeError):
    """Raised when the Pointblank engine cannot complete a stage."""


@dataclass(frozen=True)
class EngineResult:
    findings: pl.DataFrame
    check_results: pl.DataFrame


@dataclass(frozen=True)
class StepContext:
    number: int
    variable: str
    check: CheckDefinition
    bit: int


def run_pointblank(
    data: pl.DataFrame,
    key_schema: pa.Schema,
    config: ResolvedConfig,
    run_id: str,
) -> EngineResult:
    try:
        validation: Any = pb.Validate(data=data, tbl_name=config.work_unit.id)
        contexts: list[StepContext] = []
        step_number = 0
        for variable_name, variable in config.profile.variables.items():
            observed_column = (
                "observed_value_string" if variable.data_type == "string" else "observed_value"
            )
            for check in variable.checks:
                step_number += 1
                preprocess = _variable_filter(variable_name)
                thresholds = pb.Thresholds(**check.thresholds) if check.thresholds else None
                if check.method == "col_vals_not_null":
                    validation = validation.col_vals_not_null(
                        columns=observed_column,
                        pre=preprocess,
                        thresholds=thresholds,
                        brief=check.id,
                    )
                elif check.method == "col_vals_between":
                    arguments = dict(check.args)
                    arguments["na_pass"] = True
                    validation = validation.col_vals_between(
                        columns=observed_column,
                        pre=preprocess,
                        thresholds=thresholds,
                        brief=check.id,
                        **arguments,
                    )
                else:  # pragma: no cover - Pydantic prevents this
                    raise PointblankError(f"unsupported Pointblank method {check.method!r}")
                contexts.append(
                    StepContext(
                        step_number,
                        variable_name,
                        check,
                        config.rules.flags[check.flag].bit,
                    )
                )

        validation = validation.interrogate()
        report = {
            int(row["step_number"]): row
            for row in validation.get_dataframe_report(tbl_type="polars").to_dicts()
        }
        extracts = validation.get_data_extracts()
    except Exception as error:
        raise PointblankError(f"Pointblank execution failed: {error}") from error

    finding_frames: list[pl.DataFrame] = []
    result_rows: list[dict[str, Any]] = []
    key_names = key_schema.names
    for context in contexts:
        row = report[context.number]
        extract = extracts.get(context.number)
        if isinstance(extract, pl.DataFrame) and extract.height:
            finding_frames.append(
                extract.select(
                    *key_names,
                    pl.lit(context.variable).alias("variable"),
                    pl.lit(context.check.id).alias("check_id"),
                    pl.lit(context.bit, dtype=pl.UInt8).alias("bit"),
                    pl.col("observed_value").cast(pl.Float64),
                    pl.col("observed_value_string").cast(pl.String),
                    pl.lit(None, dtype=pl.Float64).alias("score"),
                    pl.lit(run_id).alias("run_id"),
                    pl.lit(config.work_unit.id).alias("work_unit_id"),
                )
            )
        units = int(row["units"] or 0)
        passed = int(row["pass_n"] or 0)
        failed = int(row["failed_n"] or 0)
        result_rows.append(
            {
                "check_id": context.check.id,
                "variable": context.variable,
                "flag_name": context.check.flag,
                "bit": context.bit,
                "engine": "pointblank",
                "processor": None,
                "units_tested": units,
                "units_passed": passed,
                "units_failed": failed,
                "fraction_failed": float(row["failed_pct"] or 0.0),
                "warning": bool(row["warning"] or False),
                "error": bool(row["error"] or False),
                "critical": bool(row["critical"] or False),
                "config_hash": config.config_hash,
            }
        )

    findings = (
        pl.concat(finding_frames, how="vertical_relaxed")
        if finding_frames
        else _empty_findings(data, key_names)
    )
    check_results = pl.DataFrame(
        result_rows,
        schema={
            "check_id": pl.String,
            "variable": pl.String,
            "flag_name": pl.String,
            "bit": pl.UInt8,
            "engine": pl.String,
            "processor": pl.String,
            "units_tested": pl.Int64,
            "units_passed": pl.Int64,
            "units_failed": pl.Int64,
            "fraction_failed": pl.Float64,
            "warning": pl.Boolean,
            "error": pl.Boolean,
            "critical": pl.Boolean,
            "config_hash": pl.String,
        },
    )
    return EngineResult(findings, check_results)


def _variable_filter(variable_name: str) -> Callable[[pl.DataFrame], pl.DataFrame]:
    def preprocess(frame: pl.DataFrame) -> pl.DataFrame:
        return frame.filter(pl.col("variable") == variable_name)

    return preprocess


def _empty_findings(data: pl.DataFrame, key_names: list[str]) -> pl.DataFrame:
    schema = pl.Schema({name: data.schema[name] for name in key_names})
    schema.update(
        {
            "variable": pl.String(),
            "check_id": pl.String(),
            "bit": pl.UInt8(),
            "observed_value": pl.Float64(),
            "observed_value_string": pl.String(),
            "score": pl.Float64(),
            "run_id": pl.String(),
            "work_unit_id": pl.String(),
        }
    )
    return pl.DataFrame(schema=schema)
