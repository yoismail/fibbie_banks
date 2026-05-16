import logging
from pathlib import Path
from pyspark.sql.functions import monotonically_increasing_id
from etl.clean import main as clean_main
from etl.logger import setup_logging, section, timed
setup_logging()

OUTPUT_PATH = Path("dataset/star_schema")


# Validate Schema
def validate_schema(df, expected_columns: list[str]) -> None:
    """Validate the schema of the DataFrame against expected columns."""
    missing_cols = [col for col in expected_columns if col not in df.columns]

    if missing_cols:
        logging.error(f"Missing columns: {missing_cols}")
        raise ValueError(f"Missing required columns: {missing_cols}")

    logging.info("✅ Schema validation passed.")


# Dim and fact table transformations
def dim_customer(df):
    """Create the dim_customer dimension table with unique customer IDs."""
    section("Creating dim_customer table")

    expected_cols = [
        "customer_name",
        "customer_address",
        "customer_city",
        "customer_state",
        "customer_country",
        "email",
        "phone_number"
    ]

    # Ensure all required fields exist
    validate_schema(df, expected_cols)

    # Generate unique surrogate key and select final columns
    df = df.withColumn("customer_id", monotonically_increasing_id())
    df = df.select("customer_id", *expected_cols)

    logging.info("✅ dim_customer table created with surrogate keys.")

    # Remove duplicates to ensure one record per customer
    return df.dropDuplicates()


def dim_employee(df):
    """Create the dim_employee table."""
    section("Creating dim_employee table")

    expected_cols = [
        "company",
        "job_title",
        "gender",
        "marital_status"
    ]

    # Ensure all required fields exist
    validate_schema(df, expected_cols)

    # Generate unique surrogate key and select final columns
    df = df.withColumn("employee_id", monotonically_increasing_id())
    df = df.select("employee_id", *expected_cols)

    logging.info("✅ dim_employee table created with surrogate keys.")

    # Remove duplicates to ensure one record per employee
    return df.dropDuplicates()


def dim_transaction(df):
    """Create the dim_transaction dimension table."""
    section("Creating dim_transaction table")

    expected_cols = [
        "transaction_date",
        "transaction_type",
        "amount"
    ]

    # Ensure all required fields exist
    validate_schema(df, expected_cols)

    # Generate unique surrogate key and select final columns
    df = df.withColumn("transaction_id", monotonically_increasing_id())
    df = df.select("transaction_id", *expected_cols)

    logging.info("✅ dim_transaction table created with surrogate keys.")

    # Remove duplicates to ensure one record per transaction
    return df.dropDuplicates()


def fact_table(df, dim_cust, dim_trans, dim_emp):
    """
    Create the fact table by joining with pre‑built dimension tables.
    Uses surrogate keys for fast, reliable joins.
    """
    section("Creating fact_table table")

    expected_cols = [
        "transaction_id",
        "customer_id",
        "employee_id",
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

    # Join ON REAL COLUMNS (natural keys)
    df = df.join(
        dim_cust,
        on=["customer_name", "customer_address", "customer_city",
            "customer_state", "customer_country", "email", "phone_number"],
        how="left"
    ) \
        .join(
        dim_trans,
        on=["transaction_date", "transaction_type", "amount"],
        how="left"
    ) \
        .join(
        dim_emp,
        on=["company", "job_title", "gender", "marital_status"],
        how="left"
    )

    # Select exactly the final schema we want
    df = df.select(*expected_cols)

    # Ensure everything exists as expected
    validate_schema(df, expected_cols)

    logging.info(
        "✅ fact_table created successfully with all required columns.")

    # Remove duplicate transactions if any exist
    return df.dropDuplicates()


def write_parquet(df, path):
    """Write the DataFrame to Parquet format."""
    section(f"Writing DataFrame to Parquet at: {path}")
    path = Path(path)

    path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"✅ Ready - output directory: {path.parent}")

    df.write.mode("overwrite").parquet(str(path))
    logging.info(f"✅ DataFrame written to Parquet successfully at: {path}")


@timed
def main():
    df_cleaned, spark = clean_main()

    # 1. Build dimensions ONCE
    dim_cust = dim_customer(df_cleaned)
    dim_trans = dim_transaction(df_cleaned)
    dim_emp = dim_employee(df_cleaned)

    # 2. Build fact table using pre‑built dimensions
    fact = fact_table(df_cleaned, dim_cust, dim_trans, dim_emp)

    # 3. Write dimension and fact tables to Parquet - would uncomment when ready
    """ 
    write_parquet(dim_cust, OUTPUT_PATH / "dim_customer")
    write_parquet(dim_trans, OUTPUT_PATH / "dim_transaction")
    write_parquet(dim_emp, OUTPUT_PATH / "dim_employee")
    write_parquet(fact, OUTPUT_PATH / "fact_transactions")
    """

    return {
        "dim_customer": dim_cust,
        "dim_transaction": dim_trans,
        "dim_employee": dim_emp,
        "fact_transactions": fact
    }, spark


if __name__ == "__main__":
    spark = None
    try:
        tables, spark = main()
        """
        for name, table in tables.items():
            logging.info(f"\n📦 {name} schema:")
            table.printSchema()
            logging.info(f"\n📊 {name} sample data:")
            table.show(5, truncate=False)  # ✅ Show full content
        """  # Would uncomment when ready
    except Exception as e:
        logging.error(f"❌ An error occurred during transformation: {e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logging.info("✅ Spark session stopped successfully.")
