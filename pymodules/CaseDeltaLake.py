import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=True)


class CaseDeltaLake:
    def __init__(self):
        self._blobacct = os.getenv("AZURE_DATALAKE_ACCOUNT_NAME")
        self._accesskey = os.getenv("AZURE_DATALAKE_ACCOUNT_KEY")
        self.bronze_db = os.getenv("LOCAL_DELTA_BRONZE_DB")
        self.silver_db = os.getenv("LOCAL_DELTA_SILVER_DB")
        #self._connect_string = f"AccountName={self._blobacct};AccountKey={self._accesskey};DefaultEndpointsProtocol=http;BlobEndpoint=http://127.0.0.1:10000/{self._blobacct};"

    def write_delta_bronze(self, df: pyspark.sql.DataFrame, df_name: str, sparkDAG: SparkSession) -> None:
        """
        Write a dataframe as a Delta table into the emulator container at subpath.
        Example subpath: 'delta/case_charges'
        """
        try:
            #account_url = f"https://{self._blobacct}.blob.core.windows.net"
            print(f"DataFrame name: {df_name}")

            delta_path = f"abfss://{self.bronze_db}@{self._blobacct}.dfs.core.windows.net/tables/{df_name}"

             # Write DataFrame to Delta (blocking)
           
            df.write.format("delta").mode("overwrite") \
                .option("overwriteSchema", "true") \
                .option("delta.autoOptimize.optimizeWrite", "true") \
                .option("delta.autoOptimize.autoCompact", "true") \
                .save(delta_path)

            # Refresh Spark catalog and create table
            '''sparkDAG.sql(f"DROP TABLE IF EXISTS bronze.{df_name}")
            sparkDAG.sql(f"CREATE TABLE bronze.{df_name} USING DELTA LOCATION '{delta_path}'")
            sparkDAG.catalog.clearCache()'''

            #bronze_df = sparkDAG.read.format("delta").load(delta_path)

            return None

        except Exception as e:
            print(f"Error writing Delta Bronze table: {e}")
            raise

    def read_delta_table(spark: SparkSession, table_name: str, db_name: str) -> pyspark.sql.DataFrame:
        """
        Read a Delta table from the specified database.
        """
        try:
            full_table_name = f"{db_name}.{table_name}"
            df = spark.read.format("delta").table(full_table_name)
            return df
        except Exception as e:
            print(f"Error reading Delta table {full_table_name}: {e}")
            raise

    def query_delta_table(self, bronze_table_name: str, spark: SparkSession, query: str) -> pyspark.sql.DataFrame:
        """
        Execute a Spark SQL query against Delta tables in blob storage and return the resulting DataFrame.
        """
        try:
            delta_table_path = f"abfss://{self.bronze_db}@{self._blobacct}.dfs.core.windows.net/tables/{bronze_table_name}"

            bronze_table_name = "bronze_table"

            spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")
            spark.sql(f"CREATE TABLE {bronze_table_name} USING DELTA LOCATION '{delta_table_path}'")

            spark.sql(f"DESCRIBE TABLE {bronze_table_name}").show()
            # Now query only the records you need using Spark SQL
            result_df = spark.sql("SELECT * FROM bronze_table WHERE <your_filter_condition> LIMIT 10")

            df = spark.sql(query)
            return df
        except Exception as e:
            print(f"Error executing query on Delta tables: {e}")
            raise

    def delta_table_schema(self, bronze_table_name: str, spark: SparkSession) -> pyspark.sql.DataFrame:
        """
        Execute a Spark SQL query against Delta tables in blob storage and return the resulting DataFrame.
        """
        try:
            delta_table_path = f"abfss://{self.bronze_db}@{self._blobacct}.dfs.core.windows.net/tables/{bronze_table_name}"

            spark.sql(f"DROP TABLE IF EXISTS {bronze_table_name}")
            spark.sql(f"CREATE TABLE {bronze_table_name} USING DELTA LOCATION '{delta_table_path}'")

            spark.sql(f"DESCRIBE TABLE {bronze_table_name}").show()
            # Now query only the records you need using Spark SQL
            result_df = spark.sql(f"SELECT * FROM {bronze_table_name}")

            return result_df
        
        except Exception as e:
            print(f"Error executing query on Delta tables: {e}")
            raise


        