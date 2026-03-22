"""
House Sale Data ETL Pipeline
============================
Implement the three functions below to complete the ETL pipeline.

Steps:
  1. EXTRACT  – load the CSV into a PySpark DataFrame
  2. TRANSFORM – split the data by neighborhood and save each as a separate CSV
  3. LOAD      – insert each neighborhood DataFrame into its own PostgreSQL table
"""
from __future__ import annotations

import csv  # noqa: F401
import os  # noqa: F401
from pathlib import Path

from dotenv import load_dotenv  # noqa: F401
from pyspark.sql import DataFrame, SparkSession  # noqa: F401
from pyspark.sql import functions as F  # noqa: F401

# ── Predefined constants (do not modify) ──────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

NEIGHBORHOODS = [
    "Downtown", "Green Valley", "Hillcrest", "Lakeside", "Maple Heights",
    "Oakwood", "Old Town", "Riverside", "Suburban Park", "University District",
]

OUTPUT_DIR   = ROOT / "output" / "by_neighborhood"
OUTPUT_FILES = {hood: OUTPUT_DIR / f"{hood.replace(' ', '_').lower()}.csv" for hood in NEIGHBORHOODS}

PG_TABLES = {hood: f"public.{hood.replace(' ', '_').lower()}" for hood in NEIGHBORHOODS}

PG_COLUMN_SCHEMA = (
    "house_id TEXT, neighborhood TEXT, price INTEGER, square_feet INTEGER, "
    "num_bedrooms INTEGER, num_bathrooms INTEGER, house_age INTEGER, "
    "garage_spaces INTEGER, lot_size_acres NUMERIC(6,2), has_pool BOOLEAN, "
    "recently_renovated BOOLEAN, energy_rating TEXT, location_score INTEGER, "
    "school_rating INTEGER, crime_rate INTEGER, "
    "distance_downtown_miles NUMERIC(6,2), sale_date DATE, days_on_market INTEGER"
)


def extract(spark: SparkSession, csv_path: str) -> DataFrame:
    """Load the CSV dataset into a PySpark DataFrame with correct data types."""
    # Check if the file exists before attempting to load
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"Input file not found at: {csv_path}")

    print(f"Extracting data from {csv_path}...")
    
    # We use inferSchema=True so Spark automatically detects types 
    # (Integer for price, Date for sale_date, Boolean for has_pool, etc.)
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_path)
    )


def transform(df: DataFrame) -> dict[str, DataFrame]:
    """Split the data by neighborhood and save each as a separate CSV file."""
    partitions = {}

    # Create the output directory if it doesn't exist yet
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for hood in NEIGHBORHOODS:
        # 1. Filter the data for this specific neighborhood
        hood_df = df.filter(F.col("neighborhood") == hood)
        
        # 2. Define paths
        # temp_path is where Spark writes its directory
        # final_csv_path is the actual .csv file you want
        temp_path = OUTPUT_DIR / f"{hood.replace(' ', '_').lower()}_temp"
        final_csv_path = OUTPUT_FILES[hood]
        
        # 3. Write to CSV 
        # Using coalesce(1) to ensure each neighborhood results in a single CSV file
        print(f"Transforming and saving data for: {hood}")
        (
            hood_df.coalesce(1)
            .write.csv(str(temp_path), header=True, mode="overwrite")
        )
        
        # 4. Clean up: Move the part file out and rename it, then delete the temp folder
        # We look for the file starting with 'part-' inside the temp directory
        try:
            part_file = next(temp_path.glob("part-*.csv"))
            part_file.replace(final_csv_path) # Move and rename
            
            # Remove the temporary Spark directory and metadata
            import shutil
            shutil.rmtree(temp_path)
        except StopIteration:
            print(f"Warning: No data found for neighborhood: {hood}")

        # 5. Store the DataFrame in our dictionary for the next step (Load)
        partitions[hood] = hood_df
        
    return partitions


def load(partitions: dict[str, DataFrame], jdbc_url: str, pg_props: dict) -> None:
    """Insert each neighborhood dataset into its own PostgreSQL table."""
    for hood, hood_df in partitions.items():
        # Get the specific table name mapping (e.g., 'public.green_valley')
        table_name = PG_TABLES[hood]
        
        print(f"Loading data into PostgreSQL table: {table_name}")
        
        # Write the data to PostgreSQL via the JDBC connector
        # Using 'append' because tables are pre-created, or 'overwrite' to refresh data
        (
            hood_df.write
            .jdbc(
                url=jdbc_url, 
                table=table_name, 
                mode="overwrite", 
                properties=pg_props
            )
        )
    print("\nETL Pipeline completed successfully!")


# ── Main (do not modify) ───────────────────────────────────────────────────────
def main() -> None:
    load_dotenv(ROOT / ".env")

    jdbc_url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST', 'localhost')}:"
        f"{os.getenv('PG_PORT', '5432')}/{os.environ['PG_DATABASE']}"
    )
    pg_props = {
        "user":     os.environ["PG_USER"],
        "password": os.getenv("PG_PASSWORD", ""),
        "driver":   "org.postgresql.Driver",
    }
    csv_path = str(ROOT / os.getenv("DATASET_DIR", "dataset") / os.getenv("DATASET_FILE", "historical_purchases.csv"))

    spark = (
        SparkSession.builder.appName("HouseSaleETL")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df         = extract(spark, csv_path)
    partitions = transform(df)
    load(partitions, jdbc_url, pg_props)

    spark.stop()


if __name__ == "__main__":
    main()
