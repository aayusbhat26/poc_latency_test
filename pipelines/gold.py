import time

def run(spark):
    start_time = time.time()
    
    # Read incoming enriched data straight from the Silver layer
    df_silv = spark.read.format("delta").load("./data_lake/silver")
    df_silv.createOrReplaceTempView("silver_hvac")
    
    # ====================================================================
    # SECTION 1: DIMENSION TABLES (Contextual Data)
    # ====================================================================
    
    # 1. dim_device
    spark.sql("""
        SELECT DISTINCT Device_ID, Cooler_Fluid_Type_Select_Status, Pump_Control_Parameter, Pump_Shutdhow_Daily_Setpoint 
        FROM silver_hvac
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/dim_device")
    
    # 2. dim_time
    spark.sql("""
        SELECT DISTINCT Date, Hour_of_Day, Operational_Shift 
        FROM silver_hvac
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/dim_time")
    
    # 3. dim_fault_codes
    spark.sql("""
        SELECT DISTINCT Active_Fault_Code, Asset_Health_Status 
        FROM silver_hvac
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/dim_fault_codes")
    
    # 4. dim_status_flags
    spark.sql("""
        SELECT DISTINCT Air_Quality_Status, Maintenance_Recommendation 
        FROM silver_hvac
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/dim_status_flags")

    # ====================================================================
    # SECTION 2: CORE FACT TABLES (Granular Measurements)
    # ====================================================================
    
    # 5. fact_hourly_telemetry
    spark.sql("""
        SELECT Device_ID, Date, Hour_of_Day, Operational_Shift,
            ROUND(AVG(Entering_Chilled_Water_Temperature_Sensor), 2) as Avg_Entering_Temp,
            ROUND(AVG(Leaving_Chilled_Water_Temperature_Sensor), 2) as Avg_Leaving_Temp,
            ROUND(AVG(System_Power_Consumption_kW), 2) as Avg_Power_kW,
            ROUND(SUM(Estimated_Hourly_Cost_USD), 2) as Total_Cost_USD
        FROM silver_hvac GROUP BY Device_ID, Date, Hour_of_Day, Operational_Shift
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/fact_hourly_telemetry")
    
    # 6. fact_fault_events
    spark.sql("""
        SELECT timestamp_formatted, Device_ID, Active_Fault_Code, Setpoint_Deviation, SLA_Breach_Flag
        FROM silver_hvac WHERE Active_Fault_Code != '0' OR SLA_Breach_Flag = 1
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/fact_fault_events")

    # ====================================================================
    # SECTION 3: BUSINESS DATA MARTS (Aggregated Dashboards)
    # ====================================================================
    
    # 7. mart_daily_costs
    spark.sql("""
        SELECT Date, Device_ID, ROUND(SUM(Estimated_Hourly_Cost_USD), 2) as Daily_Cost_USD, ROUND(SUM(System_Power_Consumption_kW), 2) as Daily_Power_kW
        FROM silver_hvac GROUP BY Date, Device_ID
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_daily_costs")
    
    # 8. mart_shift_performance
    spark.sql("""
        SELECT Date, Operational_Shift, ROUND(AVG(Cooling_Capacity_Delta), 2) as Avg_Cooling_Capacity, ROUND(AVG(System_Load_Percentage), 2) as Avg_System_Load
        FROM silver_hvac GROUP BY Date, Operational_Shift
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_shift_performance")
    
    # 9. mart_sla_breach_summary
    spark.sql("""
        SELECT Device_ID, Date, SUM(SLA_Breach_Flag) as Total_Breaches, ROUND(MAX(Setpoint_Deviation), 2) as Max_Deviation
        FROM silver_hvac GROUP BY Device_ID, Date
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_sla_breach_summary")
    
    # 10. mart_compressor_health
    spark.sql("""
        SELECT Device_ID, Date, ROUND(AVG(Total_Compressor_Amps), 2) as Avg_Amps, ROUND(MAX(Total_Compressor_Amps), 2) as Max_Amps, ROUND(AVG(VFD_Frequency_Hz), 2) as Avg_VFD_Hz
        FROM silver_hvac GROUP BY Device_ID, Date
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_compressor_health")
    
    # 11. mart_environmental_compliance
    spark.sql("""
        SELECT Date, Operational_Shift, ROUND(AVG(CO2_Level_PPM), 2) as Avg_CO2, SUM(CASE WHEN Air_Quality_Status = 'Poor' THEN 1 ELSE 0 END) as Poor_Air_Quality_Events
        FROM silver_hvac GROUP BY Date, Operational_Shift
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_environmental_compliance")
    
    # 12. mart_pump_maintenance
    spark.sql("""
        SELECT Device_ID, MAX(Pump_Hours) as Total_Run_Hours, SUM(CASE WHEN Pump_Relay_1_Status = '0' THEN 1 ELSE 0 END) as Pump_Failures
        FROM silver_hvac GROUP BY Device_ID
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_pump_maintenance")
    
    # 13. mart_cooling_efficiency (EER & Approach)
    spark.sql("""
        SELECT Device_ID, Date, ROUND(AVG(Energy_Efficiency_Ratio_EER), 2) as Avg_EER, ROUND(AVG(Evaporator_Approach_Temp), 2) as Avg_Evap_Approach, ROUND(AVG(Condenser_Approach_Temp), 2) as Avg_Cond_Approach
        FROM silver_hvac GROUP BY Device_ID, Date
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_cooling_efficiency")
    
    # 14. mart_valve_operations
    spark.sql("""
        SELECT Device_ID, ROUND(AVG(Chilled_Water_Valve_Position_Pct), 2) as Avg_Chilled_Valve_Pct, ROUND(AVG(Condenser_Water_Valve_Position_Pct), 2) as Avg_Condenser_Valve_Pct
        FROM silver_hvac GROUP BY Device_ID
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_valve_operations")
    
    # 15. mart_filter_diagnostics
    spark.sql("""
        SELECT Device_ID, Date, ROUND(MAX(Filter_Pressure_Drop_Pa), 2) as Max_Pressure_Drop, SUM(CASE WHEN Maintenance_Recommendation = 'Replace Filter' THEN 1 ELSE 0 END) as Filter_Alerts
        FROM silver_hvac GROUP BY Device_ID, Date
    """).write.format("delta").mode("overwrite").save("./data_lake/gold/mart_filter_diagnostics")

    return time.time() - start_time