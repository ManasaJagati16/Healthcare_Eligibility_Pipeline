import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# Logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("eligibility-pipeline")

standard_columns = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]

# Creating Spark Session

def build_spark(app_name: str, spark_conf: Optional[Dict[str, str]] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    # Optional configs from config file to avoid hardcoding
    if spark_conf:
        for k, v in spark_conf.items():
            builder = builder.config(k, v)

    return builder.getOrCreate()


# Reading data from partner files

def read_partner_file(
    spark: SparkSession,
    file_path: str,
    delimiter: str,
    has_header: bool = True,
    read_options: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    """
    Read delimited file
    mode=PERMISSIVE keeps malformed rows from crashing the job
    capture corrupt rows in a dedicated column for visibility
    """
    reader = (
        spark.read.format("csv")
        .option("header", str(has_header).lower())
        .option("sep", delimiter)
        .option("mode", "PERMISSIVE")
        .option("enforceSchema", "false")           
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )

    if read_options:
        for k, v in read_options.items():
            reader = reader.option(k, v)

    return reader.load(file_path)


# Standardizing and validating column formats

def _parse_dob_to_date(dob_col: F.Column, dob_formats: Optional[List[str]]) -> F.Column:
    """
    Parse dob using a list of formats.
    Returns DateType column or NULL if no format matches.
    """
    # Default formats (not partner-hardcoded). Partners can override via config.
    formats = dob_formats or ["MM/dd/yyyy", "yyyy-MM-dd", "MM-dd-yyyy", "yyyy/MM/dd"]
    parsed_exprs = [F.to_date(F.trim(dob_col), fmt) for fmt in formats]
    return F.coalesce(*parsed_exprs)


def validated_partner_df(
    df: DataFrame,
    column_mapping: Dict[str, str],
    partner_code: str,
    dob_formats: Optional[List[str]] = None,
    drop_missing_external_id: bool = True,
) -> Tuple[DataFrame, DataFrame]:
    """
    Returns:
      validated_df that pass validation rules
      rejected_df with rows  that fail validation (missing external_id, corrupt rows, bad date, etc.)
    """

    # Rename partner columns -> standard columns using mapping (config-driven)
    for partner_col, std_col in column_mapping.items():
        if partner_col in df.columns:
            df = df.withColumnRenamed(partner_col, std_col)

    # Ensure standard columns exist (so unionByName always works)
    for col in ["external_id", "first_name", "last_name", "dob", "email", "phone"]:
        if col not in df.columns:
            df = df.withColumn(col, F.lit(None).cast("string"))

    # Track parse issues in a simple reject_reason column
    df = df.withColumn("partner_code", F.lit(partner_code))

    # Mark corrupt rows (if Spark had to shove malformed rows into _corrupt_record)
    has_corrupt_col = "_corrupt_record" in df.columns
    if has_corrupt_col:
        df = df.withColumn(
            "reject_reason",
            F.when(F.col("_corrupt_record").isNotNull(), F.lit("MALFORMED_ROW")).otherwise(F.lit(None)),
        )
    else:
        df = df.withColumn("reject_reason", F.lit(None))

    # external_id: trim and null if empty (bonus validation)
    df = df.withColumn("external_id", F.nullif(F.trim(F.col("external_id").cast("string")), F.lit("")))
    df = df.withColumn(
        "reject_reason",
        F.when((F.col("reject_reason").isNull()) & (F.col("external_id").isNull()), F.lit("MISSING_EXTERNAL_ID"))
         .otherwise(F.col("reject_reason")),
    )

    # Names -> Title Case (required) 
    df = df.withColumn(
        "first_name",
        F.when(F.col("first_name").isNull(), F.lit(None))
         .otherwise(F.initcap(F.trim(F.col("first_name")))),
    ).withColumn(
        "last_name",
        F.when(F.col("last_name").isNull(), F.lit(None))
         .otherwise(F.initcap(F.trim(F.col("last_name")))),
    )

    # Email -> lowercase (required)
    df = df.withColumn(
        "email",
        F.when(F.col("email").isNull(), F.lit(None))
         .otherwise(F.lower(F.trim(F.col("email")))),
    )

    # DOB -> DateType parsed, then formatted ISO YYYY-MM-DD (required)
    # Handle incorrect date formats:if it doesn't parse, set NULL and flag
    dob_date = _parse_dob_to_date(F.col("dob"), dob_formats)
    df = df.withColumn("dob_date", dob_date)

    df = df.withColumn(
        "reject_reason",
        F.when((F.col("reject_reason").isNull()) & (F.col("dob").isNotNull()) & (F.col("dob_date").isNull()),
               F.lit("BAD_DOB_FORMAT"))
         .otherwise(F.col("reject_reason")),
    )

    df = df.withColumn("dob", F.date_format(F.col("dob_date"), "yyyy-MM-dd")).drop("dob_date")

    # Phone -> XXX-XXX-XXXX (required)
    digits = F.regexp_replace(F.trim(F.col("phone").cast("string")), r"\D", "")
    df = df.withColumn(
        "phone",
        F.when(
            F.length(digits) == 10,
            F.concat_ws("-", F.substring(digits, 1, 3), F.substring(digits, 4, 3), F.substring(digits, 7, 4)),
        ).otherwise(F.lit(None)),
    )

    # Select standard output columns
    std_df = df.select(*standard_columns, "reject_reason")

    # Split accepted vs rejected (bonus validation + error handling) 
    rejected_df = std_df.filter(F.col("reject_reason").isNotNull())
    accepted_df = std_df.filter(F.col("reject_reason").isNull()).drop("reject_reason")

    if drop_missing_external_id:
        # missing external_id already ends up rejected, so accepted_df is safe
        pass

    return accepted_df, rejected_df

# Output

def write_output(unified_df: DataFrame, config: Dict[str, Any]) -> None:
    """
    Output writer toggle:
    writer: "spark" or "pandas"
    """
    out_cfg = config.get("output", {})
    writer_type = out_cfg.get("writer", "spark").lower()
    output_path = out_cfg.get("path")

    if not output_path:
        raise ValueError("Missing output.path in config")

    if writer_type == "pandas":
        # Windows-safe: To avoid Hadoop commit protocol
        try:
            import pandas as pd 
        except Exception as e:
            raise RuntimeError(
                "output.writer is 'pandas' but pandas is not installed."
                "Install with: python -m pip install pandas"
            ) from e

        out_file = Path(output_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        unified_df.toPandas().to_csv(out_file, index=False)
        logger.info("Wrote output using pandas to %s", out_file)

    else:
        # Spark distributed write (preferred in production)
        fmt = out_cfg.get("format", "csv")
        mode = out_cfg.get("mode", "overwrite")
        options = out_cfg.get("options", {"header": "true"})

        writer = unified_df.write.mode(mode).format(fmt)
        for k, v in options.items():
            writer = writer.option(k, v)

        writer.save(output_path)
        logger.info("Wrote output using Spark to %s (format=%s)", output_path, fmt)


def write_rejects(rejected_df: DataFrame, config: Dict[str, Any]) -> None:
    """
    Write rejected rows for visibility (doesn't affect main output).
    If rejected_path is absent, it won't write.
    """
    out_cfg = config.get("output", {})
    rejected_path = out_cfg.get("rejected_path")
    if not rejected_path:
        # skip
        return

    writer_type = out_cfg.get("writer", "spark").lower()

    if writer_type == "pandas":
        # store rejects as a CSV file path
        out_file = Path(rejected_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        rejected_df.toPandas().to_csv(out_file, index=False)
        logger.info("Wrote rejected rows (pandas) to %s", out_file)
    else:
        fmt = out_cfg.get("rejected_format", out_cfg.get("format", "csv"))
        mode = out_cfg.get("mode", "overwrite")
        options = out_cfg.get("rejected_options", out_cfg.get("options", {"header": "true"}))

        writer = rejected_df.write.mode(mode).format(fmt)
        for k, v in options.items():
            writer = writer.option(k, v)

        writer.save(rejected_path)
        logger.info("Wrote rejected rows (spark) to %s", rejected_path)


# Pipeline Runner

def run_pipeline(config_path: str) -> None:
    cfg = json.loads(Path(config_path).read_text(encoding="utf-8"))

    spark = build_spark(
        app_name=cfg.get("app_name", "Healthcare Eligibility Pipeline"),
        spark_conf=cfg.get("spark_conf"),
    )

    # Read each partner’s input file with validation and fail fast if configuration or file access is invalid

    partners = cfg.get("partners", {})
    if not isinstance(partners, dict) or not partners:
        spark.stop()
        raise ValueError("Config must contain non-empty 'partners' dict")

    unified_df: Optional[DataFrame] = None
    all_rejected: Optional[DataFrame] = None

    for partner_key, p_cfg in partners.items():
        try:
            raw = read_partner_file(
                spark=spark,
                file_path=p_cfg["file_path"],
                delimiter=p_cfg["delimiter"],
                has_header=p_cfg.get("has_header", True),
                read_options=p_cfg.get("read_options"),
            )
        except Exception as e:
            spark.stop()
            raise RuntimeError(f"Failed to read partner '{partner_key}' from {p_cfg.get('file_path')}") from e

        accepted_df, rejected_df = validated_partner_df(
            df=raw,
            column_mapping=p_cfg["column_mapping"],
            partner_code=p_cfg["partner_code"],
            dob_formats=p_cfg.get("dob_formats"),
            drop_missing_external_id=True, 
        )

        # Incrementally union accepted and rejected records across all partners into consolidated DataFrames

        unified_df = accepted_df if unified_df is None else unified_df.unionByName(accepted_df)
        all_rejected = rejected_df if all_rejected is None else all_rejected.unionByName(rejected_df)

        # Log per-partner data quality metrics showing how many records were accepted vs rejected

        logger.info(
            "Partner %s: accepted=%d rejected=%d",
            partner_key,
            accepted_df.count(),
            rejected_df.count(),
        )

    if unified_df is None:
        spark.stop()
        raise RuntimeError("No data processed. Check input paths and config.")

    # Main output
    write_output(unified_df, cfg)

    # Optional rejected output
    if all_rejected is not None:
        write_rejects(all_rejected, cfg)

    spark.stop()


if __name__ == "__main__":
    run_pipeline(config_path="config/partners.json")
