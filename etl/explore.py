import logging
from etl.extract import CSV_FILE_PATH, main as extract_main
from etl.logger import setup_logging, section, timed
setup_logging()


@timed
def explore_data(df):
    """Explore the extracted data and log insights."""

    section("Schema Overview")
    df.printSchema()

    section("Sample Data")
    df.show(5)

    section("Row & Column Info")
    logging.info(f"Total Rows: {df.count()}")
    logging.info(f"Total Columns: {len(df.columns)}")

    section("Checking for Null Values")
    for col in df.columns:
        logging.info(
            f"Nulls in '{col}': {df.filter(df[col].isNull()).count()}")

    section("Unique Transaction Types")
    df.select("Transaction_Type").distinct().show()

    return df  # Return the DataFrame for potential further use


if __name__ == "__main__":
    spark = None  # Initialize to be safe

    try:
        # Step 1: Get data and session from extract.py
        df, spark = extract_main()

        # Step 2: Execute exploration on the extracted DataFrame
        explore_data(df)

    except FileNotFoundError:
        logging.error(f"❌ CSV file not found at path: {CSV_FILE_PATH}")
        raise  # Keep error visible

    except Exception as e:
        logging.error(
            f"❌ An error occurred during extraction or exploration: {e}")
        raise  # Never hide errors

    finally:
        # Spark stops ONLY AFTER everything is done
        if spark is not None:
            spark.stop()
            logging.info("✅ Spark session stopped successfully.")
