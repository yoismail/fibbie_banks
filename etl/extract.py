from pyspark.sql import SparkSession
from pathlib import Path
import logging
from etl.logger import setup_logging, section, timed
setup_logging()


CSV_FILE_PATH = Path("dataset/fibbie_bank_transactions.csv")


def create_spark_session():
    """
    Create and return a SparkSession.

    Returns:
    SparkSession: A SparkSession object.
    """
    section("Creating Spark Session")
    spark = SparkSession.builder \
        .appName("FibbieBankETL") \
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()
    logging.info("Spark Session created successfully.")
    return spark


def read_csv_to_spark(spark, file_path):
    """
    Read CSV file into a Spark DataFrame.

    Parameters:
    spark (SparkSession): The SparkSession to use for reading the CSV.
    file_path (str): The path to the CSV file.

    Returns:
    DataFrame: A Spark DataFrame containing the data from the CSV file.
    """
    section(f"Reading CSV file: {file_path}")
    df = spark.read.csv(str(file_path), header=True, inferSchema=True)
    logging.info(
        f"CSV file '{file_path}' read into Spark DataFrame successfully.")
    return df


@timed
def main():
    # Create Spark session
    spark = create_spark_session()
    # Read CSV file into Spark DataFrame
    df = read_csv_to_spark(spark, CSV_FILE_PATH)

    return df, spark


if __name__ == "__main__":
    spark = None
    try:
        df, spark = main()
        df.printSchema()
        df.show(5)

    except Exception as e:
        logging.error(f"❌ An error occurred during extraction: {e}")
        raise

    finally:
        if spark is not None:
            # Stop the Spark session after use
            spark.stop()
            logging.info("✅ Spark session stopped successfully.")
