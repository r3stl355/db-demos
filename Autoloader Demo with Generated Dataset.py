# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Demo
# MAGIC 
# MAGIC Using multiple autoloades for the same location
# MAGIC 
# MAGIC Pre-requisites (most of these can be completed via Terraform): 
# MAGIC   - Databricks workspace
# MAGIC   - Azure storage account
# MAGIC   - Storage account queue (setting up 2 autoloaders here so need 2 of those)
# MAGIC   - Event Grid Notifications configured for some location in that storage account with notifications sent to each queue
# MAGIC   - SAS connection string to the blob and queue in the storage account (stored in Databricks secrets here but can also be specified as a value)
# MAGIC   - Service principle with access to the storage account (also write as data loaded and saved back here). (service principle's secred is stored in Databricks secrets here but can also be specified as a value)
# MAGIC   

# COMMAND ----------

import os
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ***Supply your own values for these***

# COMMAND ----------

TENANT_ID = <TENANT_ID>
SUBSCRIPTION_ID = <SUBSCRIPTION_ID>
RESOURCE_GROUP = <RESOURCE_GROUP>

CLIENT_ID = <CLIENT_ID>
STORAGE_ACCOUNT = <STORAGE_ACCOUNT>
CONTAINER = <CONTAINER>
QUEUE_1 = <QUEUE_1>
QUEUE_2 = <QUEUE_2>

SECRET_SCOPE = <SECRET_SCOPE>
CLIENT_SECRET = <CLIENT_SECRET>. # or dbutils.secrets.get(<SECRET_SCOPE>, <CLIENT_SECRET_NAME>)
SAS_CONNECTION_STRING = <SAS_CONNECTION_STRING>  # or dbutils.secrets.get(<SECRET_SCOPE>, <SAS_CONNECTION_STRING_NAME>)

DEMO_SCHEMA = 'ts timestamp, job_id uuid, job_name string, start_date date, coordinates array<float>, metric int'
DATASET_ROOT_DIR = "/tmp/autoloader_demo"

# COMMAND ----------

# This is relative to the root of storage container
FOLDER = "/"
BLOB_ABFSS_PATH = os.path.join(f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net", FOLDER if FOLDER != "/" else "")
BLOB_HTTPS_PATH = os.path.join(f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER}/", FOLDER if FOLDER != "/" else "")
print(f"Monitoring {BLOB_ABFSS_PATH} ({BLOB_HTTPS_PATH})")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Spark authentication
# MAGIC 
# MAGIC Need to be defined via Spark config, can also be set via cluster/job config
# MAGIC 
# MAGIC Note:
# MAGIC   - can also use an extended version, e.g. `"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net"` instead of just `"fs.azure.account.auth.type"`
# MAGIC   - DLT will require an additional prefix for the config keys (`hadoop.spark`), e.g. `"hadoop.spark.fs.azure.account.auth.type"`

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id", CLIENT_ID)
spark.conf.set(f"fs.azure.account.oauth2.client.secret", CLIENT_SECRET)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Data generator

# COMMAND ----------

import random
import time
import uuid
from datetime import datetime as dt
from datetime import timedelta
import json, tempfile, os, pathlib, sys
from collections import OrderedDict
import string
import requests

# These will be used by default if no values are supplied
DEFAULT_DATE_TIME = dt.utcnow()
DEFAULT_DATE_RANGE = [DEFAULT_DATE_TIME - timedelta(days=14), DEFAULT_DATE_TIME]
DEFAULT_TIMESTAMP_FORMAT_UNIX = True
DEFAULT_INT_RANGE = [0, 100]
DEFAULT_FLOAT_RANGE = [-1000.0, 1000.0]
DEFAULT_FLOAT_SCALE = 4
DEFAULT_STRING_LENGTH_RANGE = [3, 10]
DEFAULT_ARRAY_LENGTH_RANGE = [2, 2]
DEFAULT_FILE_ROW_COUNT_RANGE = [500, 600]
DEFAULT_FILE_COUNT = 1

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
DATE_FORMAT = "%Y-%m-%d"

class Generator:
    def __init__(self, schema, timestamp_format_unix=DEFAULT_TIMESTAMP_FORMAT_UNIX):
        parsed_schema = [list(filter(len, kv.strip().split(" "))) for kv in schema.lower().split(',')]
        self.columns = [kv[0] for kv in parsed_schema]
        self.types = [kv[1] for kv in parsed_schema]
        self.timestamp_format_unix = timestamp_format_unix

    def _generate_value(self, dtype):
        if dtype.startswith("timestamp"):
            ts = self.generate_timestamp()
            if self.timestamp_format_unix:
                data = int(ts.timestamp())
            else:
                data = self.generate_timestamp().strftime(TIMESTAMP_FORMAT)
        elif dtype.startswith("date"):
            data = self.generate_date().strftime(DATE_FORMAT)
        elif dtype == "int":
            data = self.generate_int()
        elif dtype == "float":
            data = self.generate_float()
        elif dtype == "string":
            data = self.generate_string()
        elif dtype.startswith("array"):
            array_dtype = dtype[len("array"):].strip("<> ")
            data = self.generate_array(dtype=array_dtype)
        else:
            # Make UUID the default value
            data = str(uuid.uuid4())
        return data

    def generate_record(self):
        res = OrderedDict()
        for i in range(len(self.columns)):
            dtype = self.types[i]
            res[self.columns[i]] = self._generate_value(dtype)
        return res

    def generate_timestamp(self):
        return dt.utcnow()

    def generate_date(self, range=DEFAULT_DATE_RANGE):
        return dt.fromtimestamp(random.randint(int(range[0].timestamp()), int(range[1].timestamp())))

    def generate_int(self, range=DEFAULT_INT_RANGE):
        return random.randint(range[0], range[1])

    def generate_float(self, range=DEFAULT_FLOAT_RANGE):
        rnd_val = random.random()
        return round(range[0] + rnd_val * (range[1] - range[0]), DEFAULT_FLOAT_SCALE)

    def generate_string(self, length_range=DEFAULT_STRING_LENGTH_RANGE):
        l = random.randint(*length_range)
        ascii = string.ascii_letters
        all = ascii + string.digits + "-_"
        return ascii[random.randint(0, len(ascii)-1)] + "".join([all[random.randint(0, len(all)-1)] for i in range(l-1)])

    def generate_array(self, dtype, length_range=DEFAULT_ARRAY_LENGTH_RANGE):
        res = []
        for i in range(random.randint(*length_range)):
            res.append(self._generate_value(dtype))
        return res

def generate_single_json_file(generator, file_path, row_range=DEFAULT_FILE_ROW_COUNT_RANGE, overwrites=None):
    rec_count = random.randint(*row_range)

    with open(file_path, 'w') as f:
        for _ in range(rec_count):
            record = generator.generate_record()
            if overwrites:
                for (k, v) in overwrites.items():
                    if k in generator.columns:
                        record[k] = v
            json.dump(record, f)
            f.write("\n")

    return rec_count


def generate_json_files_partitioned_by_date(generator, file_count=DEFAULT_FILE_COUNT):
    temp_dir = tempfile.mkdtemp()
    mount = pathlib.Path(DATASET_ROOT_DIR)
    res = []
    for i in range(file_count):
        file_ts = int(time.time()) + i
        tmp_file_path = os.path.join(temp_dir, f"generated-{file_ts}.json")
        file_date = generator.generate_date().strftime(DATE_FORMAT)
        rec_count = generate_single_json_file(generator, tmp_file_path, overwrites={"start_date": file_date})
        y, m, d = file_date.split('-')
        folder = mount / f'year={y}' / f'month={m}' / f'day={d}'
        folder.mkdir(parents=True, exist_ok=True)
        dst_file = folder / os.path.basename(tmp_file_path)
        os.rename(tmp_file_path, dst_file)
        res.append(str(dst_file))
        print(f'- written {rec_count} records to {dst_file}')
    return res

def upload_files(dataset_files, content_type="application/json"):
    # In the absense of `az` CLI and properly configured azure storage client, this can be done Spark or dbutils but they don't trigger an Event Grid Notification
#     df = spark.read.format("json").load(f"file://{out_dir}").repartition(1).write.format("json").mode("append").partitionBy("year", "month", "day").save(blob_dir_path)
#     dbutils.fs.cp(f"file://{file}", os.path.join(blob_dir_path, file[len(out_dir):]))

    # Ssing REST API works
    # 1. Authenticate
    res = requests.post(f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token", 
                        data=f"grant_type=client_credentials&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}&resource=https%3A%2F%2Fstorage.azure.com%2F")
    assert res.status_code == 200
    token = json.loads(res.content.decode())["access_token"]
    
    # 2. Upload
    headers = {"Authorization": f"Bearer {token}", "Content-type": content_type, "x-ms-version": "2018-03-28", "x-ms-blob-type": "BlockBlob"}
    target_template = BLOB_HTTPS_PATH.rstrip("/") + "/" + "{}"
    for file in dataset_files:
        target_path = target_template.format(file[len(DATASET_ROOT_DIR):].lstrip("/"))
        print(f"- uploading {file} to {target_path}")
        r = requests.put(target_path, data=open(file, 'rb'), headers=headers)
        if r.status_code not in (200, 201):
            print(f"\t - failed, {r}")
        else:
            print("\t - OK")

def generate_and_upload_demo_data(dataset_root_dir=DATASET_ROOT_DIR, schema=DEMO_SCHEMA, file_count=DEFAULT_FILE_COUNT):
    generator = Generator(schema)
    files = generate_json_files_partitioned_by_date(generator, file_count)
    upload_files(files)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### First autoloader instance

# COMMAND ----------

# WARNING: setting this True will clear the output and checkpoint locations and should only be used for testing. In addition, this will also clear the source/drop location if relevant flat is set in the code
START_CLEAN = False

# Generator is using an extened schema format, reduce it to common schema type
demo_schema = DEMO_SCHEMA.replace("uuid", "string") + ", year int, month int, day int"

# Auto Loader instance specific settings
autoloader_out_folder = "root" if folder == "/" else folder.replace("/", "_")
autoloader_out_root_path = f"/tmp/{autoloader_out_folder}"
autoloader_checkpoint_path = f"{autoloader_out_root_path}/_checkpoint"
autoloader_out_path = f"{autoloader_out_root_path}/output"

if START_CLEAN:
    dbutils.fs.rm(autoloader_out_path, True)
    dbutils.fs.rm(autoloader_checkpoint_path, True)
    
    # MAKE SURE YOU KNOW WHAT YOU ARE DOING BEFORE SETTING NEXT BRANCH TO EXECUTE
    if False:
        dbutils.fs.rm(BLOB_ABFSS_PATH, True)

# COMMAND ----------

def start_autoloader(schema, queue, abfss_path, checkpoint_path, output_path):
    
    # Create the Auto Loader stream and make any extra processing as needed (e.g. add a source file name)
    autoloaderDf = (spark.readStream.format('cloudFiles')
      .option('cloudFiles.format', 'json')
      .schema(demo_schema)
      .option("cloudFiles.connectionString", SAS_CONNECTION_STRING)
      .option("cloudFiles.resourceGroup", RESOURCE_GROUP)
      .option("cloudFiles.subscriptionId", SUBSCRIPTION_ID)
      .option("cloudFiles.tenantId", TENANT_ID)
      .option('cloudFiles.partitionColumns', 'year,month,day')
      .option('cloudFiles.includeExistingFiles', False)
      .option('cloudFiles.queueName', queue)
      .option('cloudFiles.useNotifications', True)  # This turns on notification mode (as opposed to directory listing mode)
      .load(abfss_path)
      .withColumn('source_file', F.input_file_name()))

    # Start the stream writing it out to the target destination using some checkpoint location
    autoloaderDf.writeStream.format('delta').option('checkpointLocation', checkpoint_path).start(output_path)

# COMMAND ----------

start_autoloader(demo_schema, QUEUE_1, BLOB_ABFSS_PATH, autoloader_checkpoint_path, autoloader_out_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Upload some data
# MAGIC Schema of the data must conform to what the reader expects, and it needs to be partitioned. The easiest way to generate (and upload) 
# MAGIC such data is by running the generator scripts defined earlier in this notebook

# COMMAND ----------

generate_and_upload_demo_data(file_count=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display written data
# MAGIC ***Note:*** it may take few seconds for the initial records to be written to the output location, re-run the next cell if you get an error.
# MAGIC 
# MAGIC Run the next cell and wait until the record count for the file generate above is shown in the output.
# MAGIC 
# MAGIC Notice that `max_ts` column value shows that only the newly uploaded data is being processed
# MAGIC  

# COMMAND ----------

display(spark.readStream.format('delta')
  .load(autoloader_out_path)
  .groupBy("source_file")
  .agg(F.max("ts").alias("max_ts"), F.count("job_name").alias("record_count"))
  .sort("max_ts"), 
  processingTime='5 seconds')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Add more data by running the same script watch the query results change after a short delay.

# COMMAND ----------

generate_and_upload_demo_data(file_count=5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Second autoloader instance

# COMMAND ----------

autoloader_out_folder_2 = "root_2" if folder == "/" else folder.replace("/", "_") + "_2"
autoloader_out_root_path_2 = f"/tmp/{autoloader_out_folder_2}"
autoloader_checkpoint_path_2 = f"{autoloader_out_root_path_2}/_checkpoint"
autoloader_out_path_2 = f"{autoloader_out_root_path_2}/output"

if START_CLEAN:
    dbutils.fs.rm(autoloader_out_path_2, True)
    dbutils.fs.rm(autoloader_checkpoint_path_2, True)
    
start_autoloader(demo_schema, QUEUE_2, BLOB_ABFSS_PATH, autoloader_checkpoint_path_2, autoloader_out_path_2)

# COMMAND ----------

display(spark.readStream.format('delta')
  .load(autoloader_out_path_2)
  .groupBy("source_file")
  .agg(F.max("ts").alias("max_ts"), F.count("job_name").alias("record_count"))
  .sort("max_ts"), 
  processingTime='5 seconds')

# COMMAND ----------

generate_and_upload_demo_data(file_count=3)
