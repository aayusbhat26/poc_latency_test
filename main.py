import os
import sys
import time
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from huggingface_hub import batch_bucket_files

# Import the modules from your pipelines folder
from pipelines.generator_and_upload import generate_dirty_hvac_data
import pipelines.staging as staging
import pipelines.silver as silver
import pipelines.gold as gold

def upload_all_to_hf(bucket_id):
    upload_list = []
    
    # 1. Walk the local data_lake and stage all Delta tables for upload
    for root, _, files in os.walk("./data_lake"):
        for file in files:
            local_file = os.path.join(root, file)
            # Create cleaner remote paths (e.g., "batch/...", "staging/...")
            remote_file = os.path.relpath(local_file, "./data_lake").replace("\\", "/")
            upload_list.append((local_file, remote_file))
            
    # 2. Add the text log file to the upload batch
    upload_list.append(("pipeline_execution.txt", "logs/pipeline_execution.txt"))
    
    # 3. Add the real-time data dump (or fallback to dummy)
    if os.path.exists("real_time_data.json"):
        upload_list.append(("real_time_data.json", "real_time/real_time_data.json"))
    else:
        upload_list.append(("real_time_init.json", "real_time/real_time_init.json"))
    
    print(f"Uploading {len(upload_list)} files to Hugging Face...")
    batch_bucket_files(bucket_id, add=upload_list)
    print("Upload complete!")

def main():
    total_start = time.time()
    log_data = [
        "=== MEDALLION PIPELINE EXECUTION LOG ===", 
        f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
        ""
    ]
    
    hf_token = os.environ.get("HF_TOKEN")
    if not hf_token:
        print("CRITICAL ERROR: HF_TOKEN not found. Ensure it is set in GitHub Secrets.")
        sys.exit(1)
        
    bucket_id = "aayushbhat26/poc_testing_latency"
    
    # ====================================================================
    # PHASE 1: Data Generation (Simulating incoming IoT Stream)
    # ====================================================================
    print("\n[PHASE 1] Generating Raw HVAC Telemetry Data...")
    gen_start = time.time()
    import pipelines.generator_and_upload as gen
    
    # Generate 100,000 records of dirty data
    raw_json_file = gen.generate_dirty_hvac_data(100000)
    
    # Simulate uploading the raw data to the Hugging Face Data Lake
    print("Uploading raw data to Data Lake (Hugging Face Bucket)...")
    if not os.path.exists("./data_lake/raw"):
        os.makedirs("./data_lake/raw")
    shutil.copy(raw_json_file, "./data_lake/raw/hvac_batch_latest.json")
    
    # Generate the dummy file for real-time folder creation fallback
    with open("real_time_init.json", "w") as f:
        f.write('{"status": "ready", "message": "Forces the real_time folder creation in the object store."}')
        
    # ====================================================================
    # PHASE 1.5: Dump Real-Time Stream to JSON (For Hugging Face storage)
    # ====================================================================
    print("\n[PHASE 1.5] Draining real-time stream for object storage backup...")
    try:
        import kafka_to_batch
        kafka_to_batch.fetch_all_messages()
    except Exception as e:
        print(f"Failed to drain Kafka stream: {e}")
        
    log_data.append(f"[Data Generation] Time: {round(time.time() - gen_start, 2)}s")
    
    # --- PHASE 2: SPARK BOOT ---
    print("[2/5] Booting PySpark Engine...")
    spark_start = time.time()
    builder = SparkSession.builder.appName("Master_Pipeline") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.showConsoleProgress", "false")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    log_data.append(f"[PySpark Boot] Time: {round(time.time() - spark_start, 2)}s")

    # --- PHASE 3: JSON TO BATCH DELTA ---
    print("[3/5] Converting Raw JSON to Batch Delta Table...")
    batch_start = time.time()
    df_raw = spark.read.json(raw_json_file)
    df_raw.write.format("delta").mode("overwrite").save("./data_lake/batch")
    log_data.append(f"[Batch Layer Write] Time: {round(time.time() - batch_start, 2)}s")
    
    # --- PHASE 4: MEDALLION COMPUTE ---
    print("[4/5] Running Medallion Transformations...")
    staging_time = staging.run(spark)
    log_data.append(f"[Staging Layer Compute] Time: {round(staging_time, 2)}s")
    
    silver_time = silver.run(spark)
    log_data.append(f"[Silver Layer Compute] Time: {round(silver_time, 2)}s")
    
    gold_time = gold.run(spark)
    log_data.append(f"[Gold Layer Compute] Time: {round(gold_time, 2)}s")
    
    # Finalize Timing Log
    log_data.append("")
    log_data.append(f"TOTAL PIPELINE TIME: {round(time.time() - total_start, 2)}s")
    
    with open("pipeline_execution.txt", "w") as f:
        f.write("\n".join(log_data))
    
    # --- PHASE 5: MASS UPLOAD ---
    print("[5/5] Executing Mass Upload to Storage Bucket...")
    upload_all_to_hf(bucket_id)

if __name__ == "__main__":
    main()