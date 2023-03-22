#%%
from decouple import config
import os
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

token = config('TOKEN')
cluster_id = config('CLUSTER_ID')
workspace_url = config('WORKSPACE_URL')
connection_string = (f"sc://{workspace_url}:443/;"f"token={token};""use_ssl=true;"
                     f"x-databricks-cluster-id={cluster_id}")

#%%
os.environ['SPARK_REMOTE'] = connection_string 
spark = SparkSession.builder.getOrCreate()
# test check connection to cluster
spark.sql("show catalogs").show()

# %%
from databricks.sdk import WorkspaceClient
spark = SparkSession.builder.remote('databricks://default').getOrCreate()

w = WorkspaceClient(host='https://e2-demo-field-eng.cloud.databricks.com/',
                    token=token)
from databricks.sdk.runtime import dbutils
#x = dbutils.fs.ls('/')
# If you do not have the File > Upload Data menu option, uncomment and run these lines to load the dataset.

# %%
