import os
import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hvac_telemetry")

# Kafka Producer Configuration for Aiven
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'security.protocol': 'SSL',
    'ssl.ca.location': os.getenv("KAFKA_SSL_CA_LOCATION", "ca.pem"),
    'ssl.certificate.location': os.getenv("KAFKA_SSL_CERT_LOCATION", "service.cert"),
    'ssl.key.location': os.getenv("KAFKA_SSL_KEY_LOCATION", "service.key"),
}

# Optional: Handle missing credentials gracefully for testing
if not KAFKA_BOOTSTRAP_SERVERS or KAFKA_BOOTSTRAP_SERVERS == "<YOUR_AIVEN_SERVICE_URI>":
    print("WARNING: Kafka credentials not fully configured in .env!")
    print("Producer will run in DRY-RUN mode (printing to console only).")
    producer = None
else:
    try:
        producer = Producer(conf)
        print("Successfully connected to Aiven Kafka.")
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        producer = None

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def generate_live_hvac_reading():
    device_id = f"CHILLER_{str(random.randint(1, 20)).zfill(2)}"
    
    # Simulate some live telemetry
    base_temp = 42.0
    entering_temp = random.uniform(65.0, 75.0)
    leaving_temp = entering_temp - random.uniform(10.0, 15.0)
    flow_rate = random.uniform(200.0, 500.0)
    power_kw = random.uniform(50.0, 150.0)
    
    # Randomly inject a fault occasionally
    fault_code = "0"
    if random.random() < 0.05:
        fault_code = random.choice(["F01", "F02", "F03", "F04"])
        
    record = {
        "timestamp": datetime.now().isoformat(),
        "Device_ID": device_id,
        "Entering_Chilled_Water_Temperature_Sensor": round(entering_temp, 2),
        "Leaving_Chilled_Water_Temperature_Sensor": round(leaving_temp, 2),
        "Condenser_Water_Flow_Rate_Sensor_GPM": round(flow_rate, 2),
        "System_Power_Consumption_kW": round(power_kw, 2),
        "Active_Fault_Code": fault_code,
        "Pump_Hours": random.randint(1000, 20000),
        "CO2_Level_PPM": random.randint(400, 800)
    }
    return record

def main():
    print(f"Starting Live Data Producer. Sending to topic: {KAFKA_TOPIC}")
    try:
        while True:
            record = generate_live_hvac_reading()
            
            if producer:
                # Send to Kafka
                producer.produce(
                    KAFKA_TOPIC,
                    key=record["Device_ID"],
                    value=json.dumps(record),
                    callback=delivery_report
                )
                producer.poll(0)
            else:
                # Dry run
                print(f"[DRY-RUN] Generated: {record}")
                
            # Wait 5 seconds
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        if producer:
            producer.flush()

if __name__ == "__main__":
    main()
