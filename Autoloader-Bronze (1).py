# Databricks notebook source
from pyspark.sql.functions import col ,year
from delta.tables import * 

# COMMAND ----------

# MAGIC %run /Users/admin@monashuniversity503.onmicrosoft.com/TestsStocks/CredentialsADLS

# COMMAND ----------

#dbutils.secrets.listScopes()



# COMMAND ----------

# MAGIC %run /Users/admin@monashuniversity503.onmicrosoft.com/TestsStocks/CredentialsADLS

# COMMAND ----------

data_category = ['balancesheet','cashflow','incomestatement']

bronze_cont_path = 'abfss://bronzestocks@stockpricesdev.dfs.core.windows.net'


staging_loc = "abfss://staging@stockpricesdev.dfs.core.windows.net/balancesheet/"


def getcloudfilesopt(dcat , path ):

    schema_path = f"{path}/{dcat}/schema/"



    cloudFilesOption = {
    
    "cloudFiles.format": "csv",
    "cloudFiles.useNotifications": "true", # Use file notifications for efficient discovery
    "cloudFiles.includeExistingFiles": "true", # Process existing files in addition to new ones
    "cloudFiles.resourceGroup": "StocksDev",
    "cloudFiles.subscriptionId": dbutils.secrets.get(scope="Client-Secret", key="subid"),
    "cloudFiles.tenantId": dbutils.secrets.get(scope="Client-Secret", key="dirctenantid"),
    "cloudFiles.clientId": dbutils.secrets.get(scope="Client-Secret", key="appclientid"),
    "cloudFiles.clientSecret": dbutils.secrets.get(scope="Client-Secret",key="clientsecretappreg"),
    "cloudFiles.schemaEvolutionMode"  : "rescue",
    "cloudFiles.schemaLocation": schema_path  #delete folder in adls to reset

    }


return cloudFilesOption




def run_autoloader(dcat):

    cloudfileoptions = getcloudfilesopt(data_category[0] , bronze_cont_path )



    df_bs = spark.readStream.format("cloudFiles").options(**cloudfileoptions).option("Header" ,True).load(staging_loc)
    (df_bs.writeStream.format("delta")/
    .outputMode("append")/
    .queryName("Autoloader")/
    .option("mergeSchema", "true")/
    .trigger(once=True)/
    .option("checkpointLocation", bronze_cont_path + dcat + "/" + "checkpoint/")
    .start(bronze_cont_path + f"{dcat}/")
)






# COMMAND ----------

# upsert merge 
#def upsert_operation(microBatchOutputDF , BatchId):
    #delta_df = DeltaTable.forName(spark, "table name")
    #(delta_df.alias("t")
    #.merge(microBatchDF.alias("m"),
   # "m.commonColumn = t.commonColumn"),
    #.whenMatchedUpdateAll()
    #.whenNotMatchedInsertAll()
    #.execute()
  #)

# COMMAND ----------

df2 = spark.read.format("delta").load(bronze_loc + "balancesheet/")
display(df2)
