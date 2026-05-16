import logging
from etl.extract import main as extract_main
from etl.logger import setup_logging, section, timed
setup_logging()


def normalize_columns(df):
    """Normalize column names to be lowercase and snake_case."""
    for col in df.columns:
        new_col = (col
                   .strip()
                   .lower()
                   .replace(" ", "_")
                   .replace("-", "_"))
        df = df.withColumnRenamed(col, new_col)
    return df


@timed
def clean_data(df):
    """Clean the extracted data and log insights."""

    section("Handling Missing Values - Initial Count")
    missing_counts = {col: df.filter(
        df[col].isNull()).count() for col in df.columns}
    for col, count in missing_counts.items():
        logging.info(f"Nulls in '{col}': {count}")

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

    section("Handling Missing Values - After Filling & Dropping")
    missing_counts_after = {col: df.filter(
        df[col].isNull()).count() for col in df.columns}
    for col, count in missing_counts_after.items():
        logging.info(f"Nulls in '{col}': {count}")

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
