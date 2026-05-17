import logging
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import DecimalType
from etl.extract import main as extract_main
from etl.logger import setup_logging, section, timed
setup_logging()


def normalize_columns(df):
    """Normalize column names to be lowercase and snake_case."""
    for col_name in df.columns:
        new_col = (col_name
                   .strip()
                   .lower()
                   .replace(" ", "_")
                   .replace("-", "_"))
        df = df.withColumnRenamed(col_name, new_col)
    return df


def cast_amount_to_numeric(df, column_name="amount"):
    """
    Cast the 'amount' column to DECIMAL(18,2).

    Source CSV may contain amounts with currency symbols, thousands separators,
    or stray whitespace (e.g. "$1,250.50", " €99.99 "). We strip everything
    that isn't a digit, decimal point, or minus sign before casting.

    DECIMAL(18,2) chosen for currency: 16 digits of magnitude + 2 of cents.
    Maps directly to PostgreSQL NUMERIC(18,2) in the warehouse — money stays
    money through the entire pipeline.

    Rows where the cleaned value can't be parsed as a number will get NULL
    after the cast. We log the count so silent corruption is impossible.
    """
    section(f"Casting '{column_name}' to NUMERIC(18,2)")

    if column_name not in df.columns:
        logging.warning(
            f"⚠️ Column '{column_name}' not found — skipping cast.")
        return df

    before_nulls = df.filter(col(column_name).isNull()).count()

    # Strip anything that isn't a digit, dot, or minus sign
    df = df.withColumn(
        column_name,
        regexp_replace(col(column_name).cast("string"), r"[^0-9.\-]", "")
    )

    # Cast cleaned string to DECIMAL(18,2)
    df = df.withColumn(column_name, col(column_name).cast(DecimalType(18, 2)))

    after_nulls = df.filter(col(column_name).isNull()).count()
    new_nulls = after_nulls - before_nulls

    if new_nulls > 0:
        logging.warning(
            f"⚠️ {new_nulls} rows became NULL during amount cast — "
            f"likely unparseable values in source CSV"
        )
    else:
        logging.info(f"✅ '{column_name}' cast to NUMERIC(18,2) cleanly.")

    return df


@timed
def clean_data(df):
    """Clean the extracted data and log insights."""

    section("Handling Missing Values - Initial Count")
    missing_counts = {c: df.filter(
        col(c).isNull()).count() for c in df.columns}
    for c, count in missing_counts.items():
        logging.info(f"Nulls in '{c}': {count}")

    section("Filling Missing Values")
    df = df.fillna({
        "customer_name": "Unknown",
        "customer_address": "Unknown",
        "customer_city": "Unknown",
        "customer_state": "Unknown",
        "customer_country": "Unknown",
        "company": "Unknown",
        "job_title": "Unknown",
        "email": "Unknown",
        "phone_number": "Unknown",
        "credit_card_number": 0,
        "iban": "Unknown",
        "currency_code": "Unknown",
        "random_number": 0.0,
        "category": "Unknown",
        "group": "Unknown",
        "is_active": "Unknown",
        "description": "Unknown",
        "gender": "Unknown",
        "marital_status": "Unknown"
    })
    logging.info("Missing values filled with defaults.")

    section("Dropping Missing Values")
    before_drop = df.count()
    df = df.dropna(subset=["last_updated"])
    after_drop = df.count()
    logging.info(
        f"Dropped {before_drop - after_drop} rows where 'last_updated' was null.")

    # Cast amount to NUMERIC(18,2) AFTER fillna so any NULL handling above runs
    # first, but BEFORE the final null re-check so we see the true post-cast state
    df = cast_amount_to_numeric(df, "amount")

    section("Handling Missing Values - After Filling, Dropping & Casting")
    missing_counts_after = {c: df.filter(
        col(c).isNull()).count() for c in df.columns}
    for c, count in missing_counts_after.items():
        logging.info(f"Nulls in '{c}': {count}")

    section("Removing Duplicates")
    before_count = df.count()
    df_cleaned = df.dropDuplicates()
    after_count = df_cleaned.count()
    logging.info(f"Removed {before_count - after_count} duplicate rows.")

    return df_cleaned


def main():
    # Step 1: Get data from extract.py
    df, spark = extract_main()

    # Step 2: Normalize column names before cleaning
    df = normalize_columns(df)

    # Step 3: Clean the extracted DataFrame and then transform it
    df_cleaned = clean_data(df)

    # Return both cleaned DataFrame and Spark session for further use
    return df_cleaned, spark


if __name__ == "__main__":

    spark = None  # Initialize to be safe

    try:
        df_cleaned, spark = main()

    except Exception as e:
        logging.error(
            f"❌ An error occurred during extraction or transformation: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logging.info("✅ Spark session stopped successfully.")
