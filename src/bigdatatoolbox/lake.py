class Lake(object):
    """
    Clase encargada de gestionar las llamadas al datalake, escritura y lectura de datos con las diferentes interfaces.
    """

    def __init__(self, config, logger, spark=None):
        self.config = config
        self.logger = logger
        self.spark = spark

    def read_spark_parquet_files(self, storage_account: str, blob_storage: str, file_name: str):
        """
        Función que recibe Un Storage, un fichero en un blob de databricks.
        """
        df = self.spark.read.parquet(f"abfss://{blob_storage}@{storage_account}.dfs.core.windows.net/{file_name}")

        return df

    def read_spark_csv_files(self, storage_account: str, blob_storage: str, file_name: str, header: bool = False, sep: str = ";"):
        """
        Función que recibe Un Storage, un fichero en un blob de databricks.
        """
        df = self.spark.read.csv(f"abfss://{blob_storage}@{storage_account}.dfs.core.windows.net/{file_name}",
                                 header=header, sep=sep)

        return df
    
    def write_spark_parquet_files(self, df, storage_account: str, blob_storage: str, file_name: str):
        """
        Función que recibe un dataframe de spark y lo escribe en un Storage, en un blob de databricks.
        """
        
        df.write.parquet(f"abfss://{blob_storage}@{storage_account}.dfs.core.windows.net/{file_name}")

        return None