from pyspark.sql import SparkSession
import pandas as pd
from pathlib import Path


CSV_FILE_PATH = Path("dataset/fibbie_bank_transactions.csv")


def create_spark_session():
    """
    Create and return a SparkSession.

    Returns:
    SparkSession: A SparkSession object.
    """
    spark = SparkSession.builder.appName("FibbieBankETL").getOrCreate()
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
    df = spark.read.csv(str(file_path), header=True, inferSchema=True)
    return df


def main():
    # Create Spark session
    spark = create_spark_session()

    # Read CSV file into Spark DataFrame
    df = read_csv_to_spark(spark, CSV_FILE_PATH)

    # Show the schema and a sample of the data
    df.printSchema()
    df.show(5)


if __name__ == "__main__":
    main()
