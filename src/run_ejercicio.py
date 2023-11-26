from src.init_config import launcher
from pyspark.sql.types import *
from pyspark.dbutils import DBUtils


def get_dbutils(spark):
    return DBUtils(spark)


def run_ejercicio(project_data, **_):

    ### Usamos DBUTILS
    dbutils = get_dbutils(spark=project_data.spark)

    df = project_data.read_spark_csv_files(storage_account=project_data.config["STORAGE_ACCOUNT"],
                                           blob_storage=project_data.config["STAGING_BLOB_STORAGE"],
                                           file_name="circuits.csv",
                                           header=True,
                                           sep=",")

    df.show()
    
    project_data.write_spark_parquet_files(df,
                              storage_account=project_data.config["STORAGE_ACCOUNT"],
                              blob_storage=project_data.config["REFINED_BLOB_STORAGE"],
                              file_name="circuits.parquet")

    return None


if __name__ == "__main__":
    launcher(run_ejercicio, init_spark=True, use_databricks_spark=True,  enviroment="PRODUCCION")
