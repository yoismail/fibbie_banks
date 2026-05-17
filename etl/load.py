import logging
from sqlalchemy import create_engine, text
from etl.db_config import DB_CONFIG
from etl.logger import setup_logging, section, timed
from etl.transform import main as transform_main

setup_logging()


def get_db_engine():
    """Create SQLAlchemy engine using values from .env"""
    try:
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}",
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={"connect_timeout": 60}
        )
        logging.info("✅ Database engine created")
        return engine
    except Exception as e:
        logging.error(f"❌ Failed to create engine: {e}")
        return None


def ensure_schema_exists(engine, schema_name="analytics"):
    """Automatically create schema if it does not exist"""
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
        logging.info(f"✅ Schema {schema_name} is ready")
    except Exception as e:
        logging.error(f"❌ Failed to create schema {schema_name}: {e}")
        raise SystemExit(1)


def load_spark_to_postgres_incremental(
    spark_df,
    table_name,
    engine,
    schema_name="analytics",
    key_columns=None
):
    """
    Incrementally load a Spark DataFrame into Postgres via a JDBC temp table
    + LEFT JOIN merge. Only rows whose surrogate key isn't already in the
    target table get appended.

    Schema is OWNED BY create_tables.sql — this function assumes the target
    table already exists with the correct types, primary keys, foreign keys,
    and indexes. If the target table is missing, the function hard-fails with
    a clear message telling the operator to run create_tables.sql first.

    This separation keeps ETL responsibilities clean: DDL is declarative and
    versioned in SQL; the Python pipeline only handles data movement.
    """

    if spark_df is None or engine is None:
        logging.error(
            f"❌ Missing data or engine for {schema_name}.{table_name} — STOPPING")
        raise SystemExit(1)

    # Default unique keys per table
    if key_columns is None:
        key_map = {
            "dim_date": ["date_key"],
            "dim_customer": ["customer_id"],
            "dim_transaction": ["transaction_id"],
            "dim_employee": ["employee_id"],
            "fact_transactions": ["transaction_id"]
        }
        key_columns = key_map.get(table_name, ["id"])  # fallback if needed

    try:
        # JDBC connection details (uses existing Spark session)
        temp_table = f"temp_{table_name}"
        jdbc_url = (
            f"jdbc:postgresql://{engine.url.host}:{engine.url.port}/{engine.url.database}"
        )
        db_properties = {
            "user": engine.url.username,
            "password": engine.url.password,
            "driver": "org.postgresql.Driver",
            "batchsize": "100000",
            "fetchsize": "100000",
            "isolationLevel": "READ_COMMITTED"
        }

        logging.info(
            f"📥 Writing new data to temp table: {schema_name}.{temp_table}")
        spark_df.write.jdbc(
            url=jdbc_url,
            table=f"{schema_name}.{temp_table}",
            mode="overwrite",  # overwrite temp table each run
            properties=db_properties
        )

        # Deduplicate & Insert ONLY NEW rows
        key_condition = " AND ".join(
            [f"tgt.{k} = src.{k}" for k in key_columns])

        with engine.begin() as conn:
            # Check that the target table exists — schema is owned by create_tables.sql
            table_exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = '{schema_name}' 
                      AND table_name = '{table_name}'
                )
            """)).scalar()

            if not table_exists:
                # Hard fail — the operator forgot to run create_tables.sql first
                logging.error(
                    f"❌ Target table {schema_name}.{table_name} does not exist."
                )
                logging.error(
                    f"   Schema is owned by sql/create_tables.sql. Run it first:"
                )
                logging.error(
                    f"   psql -U postgres -d <database> -f sql/create_tables.sql"
                )
                # Best-effort cleanup of the temp table we just created
                conn.execute(
                    text(f"DROP TABLE IF EXISTS {schema_name}.{temp_table}"))
                raise SystemExit(1)

            # ✅ ==================================== ✅
            # Checking for new rows to insert — this is the core incremental logic
            logging.info(
                f"🔍 Checking for new rows in {schema_name}.{table_name}")

            if table_name == "dim_transaction":
                # EXPLICIT CAST: Convert text → numeric for amount
                result = conn.execute(text(f"""
                    INSERT INTO {schema_name}.{table_name} 
                        (transaction_id, transaction_date, date_key, transaction_type, amount)
                    SELECT 
                        src.transaction_id,
                        src.transaction_date,
                        src.date_key,
                        src.transaction_type,
                        src.amount::NUMERIC
                    FROM {schema_name}.{temp_table} src
                    LEFT JOIN {schema_name}.{table_name} tgt 
                      ON {key_condition}
                    WHERE tgt.{key_columns[0]} IS NULL
                """))
            else:
                # Normal safe insert for all other tables
                result = conn.execute(text(f"""
                    INSERT INTO {schema_name}.{table_name}
                    SELECT src.* 
                    FROM {schema_name}.{temp_table} src
                    LEFT JOIN {schema_name}.{table_name} tgt 
                      ON {key_condition}
                    WHERE tgt.{key_columns[0]} IS NULL
                """))
            # ✅ ==================================== ✅

            inserted = result.rowcount
            if inserted == 0:
                logging.info(
                    f"ℹ️ No new rows to insert — {schema_name}.{table_name} is already up to date")
            else:
                logging.info(
                    f"✅ Appended {inserted} new rows to {schema_name}.{table_name}")

            # Clean up temporary table
            conn.execute(
                text(f"DROP TABLE IF EXISTS {schema_name}.{temp_table}"))

    except SystemExit:
        # Re-raise SystemExit cleanly so it propagates to the orchestrator
        raise
    except Exception as e:
        logging.error(f"❌ Failed to process {schema_name}.{table_name}: {e}")
        raise SystemExit(1)  # 🚨 STOP entire process on any error


@timed
def main():

    tables, spark = transform_main()

    # Create DB connection engine - STOP immediately if fails
    engine = get_db_engine()
    if not engine:
        logging.error(
            "❌ CRITICAL: Database engine not created — ABORTING PROCESS")
        raise SystemExit(1)

    # Make sure analytics schema exists. NOTE: This only creates the schema,
    # not the tables. The four target tables (dim_customer, dim_transaction,
    # dim_employee, fact_transactions) must already exist via create_tables.sql.
    ensure_schema_exists(engine, "analytics")

    section("Loading dimension & fact tables — INCREMENTAL MODE")
    # Date FIRST
    load_spark_to_postgres_incremental(
        spark_df=tables["dim_date"],
        table_name="dim_date",
        engine=engine
    )

    load_spark_to_postgres_incremental(
        spark_df=tables["dim_customer"],
        table_name="dim_customer",
        engine=engine
    )

    load_spark_to_postgres_incremental(
        spark_df=tables["dim_transaction"],
        table_name="dim_transaction",
        engine=engine
    )

    load_spark_to_postgres_incremental(
        spark_df=tables["dim_employee"],
        table_name="dim_employee",
        engine=engine
    )

    load_spark_to_postgres_incremental(
        spark_df=tables["fact_transactions"],
        table_name="fact_transactions",
        engine=engine
    )

    logging.info("🎉 ALL TABLES LOADED SUCCESSFULLY — ONLY NEW DATA APPENDED")
    return tables, spark


if __name__ == "__main__":
    spark = None
    try:
        tables, spark = main()

    except SystemExit:
        logging.error("❌ Process stopped due to critical error")

    except Exception as e:
        logging.error(f"❌ An unexpected error occurred: {e}")

    finally:

        if spark is not None:
            spark.stop()
            logging.info("✅ Spark session stopped successfully")
