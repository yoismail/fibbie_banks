import logging
from pathlib import Path
from pyspark.sql.functions import sha2, concat_ws, col, year, month, dayofmonth, dayofweek, date_format, when, lit, to_date
from pyspark.sql.types import DecimalType  # Required for numeric type
from etl.clean import main as clean_main
from etl.logger import setup_logging, section, timed

setup_logging()


# Validate Schema
def validate_schema(df, expected_columns: list[str]) -> None:
    """Validate the schema of the DataFrame against expected columns."""
    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        logging.error(f"Missing columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    logging.info("✅ Schema validation passed.")


def make_surrogate_key(natural_key_cols: list[str]):
    """
    Build a deterministic surrogate key from a list of natural-key columns.
    Converts values to string ONLY for hashing — original types remain unchanged.
    """
    cols_for_hash = [col(c).cast("string") for c in natural_key_cols]
    return sha2(concat_ws("|", *cols_for_hash), 256)


# ------------------------------------------------------------------------------
# 📅 NEW: Date Dimension — FIXED: handles TIMESTAMP → DATE conversion
# ------------------------------------------------------------------------------
def dim_date(df):
    """
    Create date dimension table with standard attributes.
    Generates one row per unique transaction date.
    """
    section("Creating dim_date table")

    # ✅ Convert TIMESTAMP → DATE for matching
    df = df.withColumn("transaction_date_only",
                       to_date(col("transaction_date")))

    # Get unique dates only
    df_dates = df.select("transaction_date_only").distinct()

    # Build date attributes
    df_date_dim = df_dates \
        .withColumn("date_key", date_format("transaction_date_only", "yyyyMMdd").cast("INT")) \
        .withColumn("full_date", col("transaction_date_only")) \
        .withColumn("year", year("transaction_date_only")) \
        .withColumn("month", month("transaction_date_only")) \
        .withColumn("month_name", date_format("transaction_date_only", "MMMM")) \
        .withColumn("quarter", date_format("transaction_date_only", "Q").cast("INT")) \
        .withColumn("day_of_month", dayofmonth("transaction_date_only")) \
        .withColumn("day_name", date_format("transaction_date_only", "EEEE")) \
        .withColumn("is_weekend", when(dayofweek("transaction_date_only").isin(1, 7), lit(True)).otherwise(lit(False)))

    # Final schema
    df_date_dim = df_date_dim.select(
        "date_key", "full_date", "year", "month",
        "month_name", "quarter", "day_of_month", "day_name", "is_weekend"
    )

    logging.info("✅ dim_date table created.")
    return df_date_dim.dropDuplicates(["date_key"])


# Dim and fact table transformations
def dim_customer(df):
    """Create the dim_customer dimension table with deterministic surrogate keys."""
    section("Creating dim_customer table")

    natural_key_cols = [
        "customer_name",
        "customer_address",
        "customer_city",
        "customer_state",
        "customer_country",
        "email",
        "phone_number"
    ]

    validate_schema(df, natural_key_cols)

    df = df.withColumn("customer_id", make_surrogate_key(natural_key_cols))
    df = df.select("customer_id", *natural_key_cols)

    logging.info("✅ dim_customer table created.")
    return df.dropDuplicates(["customer_id"])


def dim_employee(df):
    """Create the dim_employee table with deterministic surrogate keys."""
    section("Creating dim_employee table")

    natural_key_cols = [
        "company",
        "job_title",
        "gender",
        "marital_status"
    ]

    validate_schema(df, natural_key_cols)

    df = df.withColumn("employee_id", make_surrogate_key(natural_key_cols))
    df = df.select("employee_id", *natural_key_cols)

    logging.info("✅ dim_employee table created.")
    return df.dropDuplicates(["employee_id"])


def dim_transaction(df, dim_date_df):
    """
    Create the dim_transaction dimension table.
    ⚠️ CRITICAL: Ensures `amount` stays DECIMAL/NUMERIC to match PostgreSQL.
    ⚠️ Column order matches exactly what load.py expects.
    ➕ Added date_key from dim_date — FIXED join type
    """
    section("Creating dim_transaction table")

    natural_key_cols = [
        "transaction_date",
        "transaction_type",
        "amount"
    ]

    validate_schema(df, natural_key_cols)

    # ✅ Convert TIMESTAMP → DATE to match dim_date
    df = df.withColumn("transaction_date_only",
                       to_date(col("transaction_date")))

    # ✅ Join on DATE type — guaranteed match
    df = df.join(
        dim_date_df.select("date_key", "full_date").withColumnRenamed(
            "full_date", "transaction_date_only"),
        on="transaction_date_only",
        how="left"
    )

    # Create ID — casting happens INSIDE the hash only
    df = df.withColumn("transaction_id", make_surrogate_key(natural_key_cols))

    # ✅ date_key will NEVER be null now
    df = df.select(
        "transaction_id",
        "transaction_date",
        "date_key",
        "transaction_type",
        col("amount").cast(DecimalType(18, 2))  # FORCE NUMERIC TYPE HERE
    )

    logging.info("✅ dim_transaction table created — amount is NUMERIC.")
    return df.dropDuplicates(["transaction_id"])


def fact_table(df, dim_cust, dim_trans, dim_emp):
    """Create the fact table by joining with pre‑built dimension tables."""
    section("Creating fact_table table")

    expected_cols = [
        "transaction_id",
        "customer_id",
        "employee_id",
        "date_key",
        "credit_card_number",
        "iban",
        "currency_code",
        "random_number",
        "category",
        "group",
        "is_active",
        "last_updated",
        "description"
    ]

    df = df.join(
        dim_cust,
        on=["customer_name", "customer_address", "customer_city",
            "customer_state", "customer_country", "email", "phone_number"],
        how="left"
    ) \
        .join(
        # ✅ Join only on natural keys — date_key/transaction_id come from dim
        dim_trans,
        on=["transaction_date", "transaction_type", "amount"],
        how="left"
    ) \
        .join(
        dim_emp,
        on=["company", "job_title", "gender", "marital_status"],
        how="left"
    )

    df = df.select(*expected_cols)
    validate_schema(df, expected_cols)

    logging.info("✅ fact_table created successfully.")
    return df.dropDuplicates(["transaction_id"])


@timed
def main():
    df_cleaned, spark = clean_main()

    # ✅ NEW: Build date dimension FIRST
    dim_date_df = dim_date(df_cleaned)

    # Build dimensions
    dim_cust = dim_customer(df_cleaned)
    dim_trans = dim_transaction(df_cleaned, dim_date_df)  # Pass date dim in
    dim_emp = dim_employee(df_cleaned)

    # Build fact
    fact = fact_table(df_cleaned, dim_cust, dim_trans, dim_emp)

    return {
        "dim_date": dim_date_df,      # NEW
        "dim_customer": dim_cust,
        "dim_transaction": dim_trans,
        "dim_employee": dim_emp,
        "fact_transactions": fact
    }, spark


if __name__ == "__main__":
    spark = None
    try:
        tables, spark = main()

    except Exception as e:
        logging.error(f"❌ An error occurred during transformation: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logging.info("✅ Spark session stopped successfully.")
