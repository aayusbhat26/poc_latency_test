import json
import time
import random

def generate_dirty_hvac_data(num_records):
    file_name = "hvac_raw.json"
    
    with open(file_name, "w") as f:
        for i in range(num_records):
            current_timestamp = str(int(time.time() * 1000) + (i * 1000))
            
            record = {
                # --- Base Identifiers & Time (2 keys) ---
                "timestamp": current_timestamp,
                "Device_ID": f"CHILLER_{random.randint(1, 15):02d}",
                
                # --- Core HVAC Keys (10 keys) ---
                "Entering_Chilled_Water_Temperature_Sensor": str(round(random.uniform(60.0, 76.5), 1)),
                "Leaving_Chilled_Water_Temperature_Sensor": str(round(random.uniform(36.0, 44.0), 1)),
                "Cumulated_Pump_2_Hours_Sensor": str(round(29112.0 + (i * 0.5), 5)),
                "Cooler_Fluid_Type_Select_Status": str(random.choice([1, 2])),
                "Pump_Relay_1_Status": str(random.choice([0, 1])),
                "Pump_Relay_2_Status": str(random.choice([0, 1])),
                "Flow_Switch_Status": str(random.choice([0, 1])),
                "Leaving_Chilled_Water_Temperature_Setpoint": "40.00001525878906",
                "LWT_Setpoint": str(random.randint(0, 5)),
                "Pump_Control_Parameter": str(random.choice([1, 2, 3])),
                
                # --- Compressors & Condensers (8 keys) ---
                "Compressor_1_Status": str(random.choice([0, 1])),
                "Compressor_2_Status": str(random.choice([0, 1])),
                "Evaporator_Pressure_Sensor_kPa": str(round(random.uniform(300.0, 350.0), 2)),
                "Condenser_Pressure_Sensor_kPa": str(round(random.uniform(800.0, 1200.0), 2)),
                "Refrigerant_Discharge_Temperature": str(round(random.uniform(65.0, 85.0), 1)),
                "Refrigerant_Suction_Temperature": str(round(random.uniform(5.0, 15.0), 1)),
                "Oil_Pressure_Sensor_kPa": str(round(random.uniform(200.0, 250.0), 2)),
                "Oil_Temperature_Sensor": str(round(random.uniform(40.0, 60.0), 1)),
                
                # --- Environment & Airflow (6 keys) ---
                "Ambient_Air_Temperature_Sensor": str(round(random.uniform(70.0, 95.0), 1)),
                "Return_Air_Temperature_Sensor": str(round(random.uniform(68.0, 78.0), 1)),
                "Supply_Air_Temperature_Sensor": str(round(random.uniform(55.0, 65.0), 1)),
                "Relative_Humidity_Percentage": str(round(random.uniform(40.0, 60.0), 1)),
                "CO2_Level_PPM": str(random.randint(400, 800)),
                "Filter_Pressure_Drop_Pa": str(round(random.uniform(50.0, 150.0), 1)),
                
                # --- Electrical & Power (6 keys) ---
                "System_Power_Consumption_kW": str(round(random.uniform(50.0, 150.0), 2)),
                "Compressor_1_Current_Amps": str(round(random.uniform(40.0, 80.0), 1)),
                "Compressor_2_Current_Amps": str(round(random.uniform(40.0, 80.0), 1)),
                "VFD_Frequency_Hz": str(round(random.uniform(30.0, 60.0), 1)),
                "Energy_Efficiency_Ratio_EER": str(round(random.uniform(10.0, 15.0), 2)),
                "Pump_Shutdhow_Daily_Setpoint": str(random.randint(4, 8)),
                
                # --- Flow Rates & Valves (5 keys) ---
                "Evaporator_Water_Flow_Rate_GPM": str(round(random.uniform(200.0, 300.0), 1)),
                "Condenser_Water_Flow_Rate_GPM": str(round(random.uniform(250.0, 350.0), 1)),
                "Cooling_Tower_Water_Temperature": str(round(random.uniform(75.0, 85.0), 1)),
                "Chilled_Water_Valve_Position_Pct": str(round(random.uniform(0.0, 100.0), 1)),
                "Condenser_Water_Valve_Position_Pct": str(round(random.uniform(0.0, 100.0), 1)),
                
                # --- System States (3 keys) ---
                "Chiller_Run_Hours": str(round(15000.0 + (i * 0.1), 1)),
                "Active_Fault_Code": "0" if random.random() > 0.05 else str(random.randint(1, 99)),
                "Maintenance_Required_Flag": "0",
            }
            
            # Injecting Controlled Nulls for Staging layer to filter
            random_val = random.random()
            if random_val < 0.08:
                record["Entering_Chilled_Water_Temperature_Sensor"] = None
            elif random_val < 0.14:
                record["Leaving_Chilled_Water_Temperature_Sensor"] = None
                
            f.write(json.dumps(record) + "\n")
            
    return file_name