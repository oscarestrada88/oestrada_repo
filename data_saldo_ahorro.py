from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime

spark = SparkSession.builder.appName('pfm').getOrCreate()

# nombre: 
#     saldos/pasivo/cuenta-ahorro/saldos_pasivo_bbog_cuenta-ahorro_pruebas
# schema:
#     root
#      |-- balance: double (nullable = true)
#      |-- date: date (nullable = true)
#      |-- id_product: string (nullable = true)
# ex:
#     (100.5,datetime(2020,12,31),71234)



schema = StructType([
    StructField("balance", DoubleType(), True),
    StructField("date", DateType(), True),
    StructField("id_product", StringType(), True),
])


data = [
    (1000.0,datetime(2020,9,1),'8300000000000000'),
    (3000.0,datetime(2020,9,1),'8310000000000000'),
    (9000000.94,datetime(2020,7,1),'9150000000000000'),
    (1000000.0,datetime(2020,8,1),'9150000000000000'),
    (11211.11,datetime(2020,8,1),'9150000000000000'),
    (2000000.00,datetime(2020,8,1),'8270000000000000'),
]
output_path = 'C:\tmp\bolsillos\alcancia_pasivo_bbog_cuenta-ahorro_pruebas'


df = spark.createDataFrame(data, schema)
df.printSchema()                        
df.show()                               
                                        
df.write.parquet(output_path)           

#loaded_df = spark.read.format("parquet").option("header", "true").load(output_path)


# python3 -m venv venv
# which python3
# source venv/bin/activate ---(SOLO ESTE PARA CARGAR date) PARA ACTIVAR PYTHON CON LIBRERIA
# which python3
# python3 -m pip install -r requirements.txt
# python3 date_saldos.py