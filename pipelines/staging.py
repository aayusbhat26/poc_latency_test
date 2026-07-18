import time

def run(spark):
    start_time = time.time()
    
    # Read directly from the new Batch Delta table created in main.py
    df_raw = spark.read.format("delta").load("./data_lake/batch")
    
    # Transform 1: Drop records missing critical identifiers or base HVAC metrics
    df_staging = df_raw.dropna(subset=[
        "Device_ID",
        "Entering_Chilled_Water_Temperature_Sensor", 
        "Leaving_Chilled_Water_Temperature_Sensor"
    ])
    
    # Transform 2: Remove any exact duplicate records that might have been ingested
    df_staging = df_staging.dropDuplicates()
    
    # Save processed records directly as an immutable Delta Table
    df_staging.write.format("delta").mode("overwrite").save("./data_lake/staging")
    
    return time.time() - start_time