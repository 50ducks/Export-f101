#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession, DataFrame
from datetime import datetime

import logs

from config import properties, url, directory_path


# In[2]:


#Spark Session

appName = 'Jupyter'
master = 'local'
spark = SparkSession \
.builder \
.appName(appName) \
.master(master) \
.config("spark.sql.caseSensitive", "false") \
.config("spark.jars", "postgresql-42.7.2.jar") \
.getOrCreate()

logs.set_start_time(datetime.now())


# In[3]:


#export and save

df = spark.read.jdbc(url=url, table="dm.dm_f101_round_f", properties=properties)
new_directory_path = directory_path + "\\f101\\f101.csv"
print(new_directory_path)
try:
    df.toPandas().to_csv(new_directory_path, index=False)
    print(f"Файл успешно сохранен: {new_directory_path}")
except Exception as e:
    print(f"Неизвестная ошибка при записи в CSV: {e}")


# In[4]:


#exctract from new .csv and write into table

new_df = spark.read.format("CSV") \
            .options(header = True, inferSchema = True, delimiter = ',', encoding = "UTF-8") \
            .load(new_directory_path)
new_df.write.jdbc(url=url, table='dm.dm_f101_round_f_v2', mode="overwrite", properties=properties)

logs.set_end_time(datetime.now())


# In[5]:


#logs

log = spark.sql(f"""
    SELECT * FROM values 
    (
        CAST('{logs.get_start_time()}' AS timestamp),
        CAST('{logs.get_end_time()}' AS timestamp)
    ) AS (
        start_time, end_time
    )
""")

log.show()

log.write.jdbc(url=url, table="logs.export_logs", mode="append", properties=properties)


# In[ ]:




