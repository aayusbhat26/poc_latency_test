import time
from pyspark.sql.functions import col, from_unixtime, round as spark_round, expr, hour, when

def run(spark):
    start_time = time.time()
    
    # Read from the Staging Delta layer
    df_stg = spark.read.format("delta").load("./data_lake/staging")
    
    # 1. Type Casting (Crucial for Gold layer aggregations to work)
    df_casted = df_stg \
        .withColumn("Entering_Chilled_Water_Temperature_Sensor", col("Entering_Chilled_Water_Temperature_Sensor").cast("double")) \
        .withColumn("Leaving_Chilled_Water_Temperature_Sensor", col("Leaving_Chilled_Water_Temperature_Sensor").cast("double")) \
        .withColumn("Leaving_Chilled_Water_Temperature_Setpoint", col("Leaving_Chilled_Water_Temperature_Setpoint").cast("double")) \
        .withColumn("System_Power_Consumption_kW", col("System_Power_Consumption_kW").cast("double")) \
        .withColumn("VFD_Frequency_Hz", col("VFD_Frequency_Hz").cast("double")) \
        .withColumn("Compressor_1_Current_Amps", col("Compressor_1_Current_Amps").cast("double")) \
        .withColumn("Compressor_2_Current_Amps", col("Compressor_2_Current_Amps").cast("double")) \
        .withColumn("CO2_Level_PPM", col("CO2_Level_PPM").cast("double")) \
        .withColumn("Filter_Pressure_Drop_Pa", col("Filter_Pressure_Drop_Pa").cast("double")) \
        .withColumn("Refrigerant_Suction_Temperature", col("Refrigerant_Suction_Temperature").cast("double")) \
        .withColumn("Refrigerant_Discharge_Temperature", col("Refrigerant_Discharge_Temperature").cast("double")) \
        .withColumn("Cooling_Tower_Water_Temperature", col("Cooling_Tower_Water_Temperature").cast("double")) \
        .withColumn("Return_Air_Temperature_Sensor", col("Return_Air_Temperature_Sensor").cast("double")) \
        .withColumn("Supply_Air_Temperature_Sensor", col("Supply_Air_Temperature_Sensor").cast("double")) \
        .withColumn("Energy_Efficiency_Ratio_EER", col("Energy_Efficiency_Ratio_EER").cast("double")) \
        .withColumn("Chilled_Water_Valve_Position_Pct", col("Chilled_Water_Valve_Position_Pct").cast("double")) \
        .withColumn("Condenser_Water_Valve_Position_Pct", col("Condenser_Water_Valve_Position_Pct").cast("double")) \
        .withColumn("Pump_Hours", col("Cumulated_Pump_2_Hours_Sensor").cast("double"))
        
    # 2. Add 16 New Business Columns (bringing total to 56 Columns)
    df_silver = df_casted \
        .withColumn("timestamp_formatted", from_unixtime(col("timestamp") / 1000).cast("timestamp")) \
        .withColumn("Date", expr("to_date(timestamp_formatted)")) \
        .withColumn("Hour_of_Day", hour("timestamp_formatted")) \
        .withColumn("Operational_Shift", 
            when((col("Hour_of_Day") >= 6) & (col("Hour_of_Day") < 14), "Morning")
            .when((col("Hour_of_Day") >= 14) & (col("Hour_of_Day") < 22), "Afternoon")
            .otherwise("Night")
        ) \
        .withColumn("Cooling_Capacity_Delta", spark_round(col("Entering_Chilled_Water_Temperature_Sensor") - col("Leaving_Chilled_Water_Temperature_Sensor"), 2)) \
        .withColumn("Setpoint_Deviation", spark_round(col("Leaving_Chilled_Water_Temperature_Sensor") - col("Leaving_Chilled_Water_Temperature_Setpoint"), 2)) \
        .withColumn("SLA_Breach_Flag", when(col("Setpoint_Deviation") > 2.0, 1).otherwise(0)) \
        .withColumn("Estimated_Hourly_Cost_USD", spark_round(col("System_Power_Consumption_kW") * 0.12, 2)) \
        .withColumn("System_Load_Percentage", spark_round((col("VFD_Frequency_Hz") / 60.0) * 100, 2)) \
        .withColumn("Total_Compressor_Amps", spark_round(col("Compressor_1_Current_Amps") + col("Compressor_2_Current_Amps"), 2)) \
        .withColumn("Air_Quality_Status", when(col("CO2_Level_PPM") > 600, "Poor").otherwise("Good")) \
        .withColumn("Maintenance_Recommendation", when(col("Filter_Pressure_Drop_Pa") > 120, "Replace Filter").otherwise("None")) \
        .withColumn("Asset_Health_Status", when(col("Active_Fault_Code") == "0", "Healthy").otherwise("Critical")) \
        .withColumn("Evaporator_Approach_Temp", spark_round(col("Leaving_Chilled_Water_Temperature_Sensor") - col("Refrigerant_Suction_Temperature"), 2)) \
        .withColumn("Condenser_Approach_Temp", spark_round(col("Refrigerant_Discharge_Temperature") - col("Cooling_Tower_Water_Temperature"), 2)) \
        .withColumn("Air_Cooling_Delta", spark_round(col("Return_Air_Temperature_Sensor") - col("Supply_Air_Temperature_Sensor"), 2))
        
    # Write optimized 56-column schema down to the local Silver path
    df_silver.write.format("delta").mode("overwrite").save("./data_lake/silver")
    
    return time.time() - start_time