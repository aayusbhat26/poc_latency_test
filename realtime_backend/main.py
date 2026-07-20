import os
import json
import asyncio
import threading
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from confluent_kafka import Consumer
from dotenv import load_dotenv
import time

load_dotenv(os.path.join(os.path.dirname(__file__), "../.env"))

app = FastAPI(title="Lakehouse UI Real-Time Streaming Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hvac_telemetry")

# In-memory buffer to hold the latest messages
latest_messages = []
MAX_BUFFER_SIZE = 100
total_messages_received = 0

def kafka_consumer_thread():
    if not KAFKA_BOOTSTRAP_SERVERS or KAFKA_BOOTSTRAP_SERVERS == "<YOUR_AIVEN_SERVICE_URI>":
        print("WARNING: Kafka credentials not fully configured in .env!")
        print("Generating mock live data for the stream...")
        import random
        from datetime import datetime
        while True:
            mock_msg = {
                "timestamp": datetime.now().isoformat(),
                "Device_ID": f"CHILLER_{str(random.randint(1, 20)).zfill(2)}",
                "Entering_Chilled_Water_Temperature_Sensor": round(random.uniform(65.0, 75.0), 2),
                "Leaving_Chilled_Water_Temperature_Sensor": round(random.uniform(50.0, 60.0), 2),
                "System_Power_Consumption_kW": round(random.uniform(50.0, 150.0), 2),
                "Active_Fault_Code": random.choice(["0", "F01", "0", "0"])
            }
            latest_messages.append(mock_msg)
            if len(latest_messages) > MAX_BUFFER_SIZE:
                latest_messages.pop(0)
            time.sleep(30)
            
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'realtime_dashboard_group',
        'auto.offset.reset': 'latest',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(os.path.dirname(__file__), "../", os.getenv("KAFKA_SSL_CA_LOCATION", "ca.pem")),
        'ssl.certificate.location': os.path.join(os.path.dirname(__file__), "../", os.getenv("KAFKA_SSL_CERT_LOCATION", "service.cert")),
        'ssl.key.location': os.path.join(os.path.dirname(__file__), "../", os.getenv("KAFKA_SSL_KEY_LOCATION", "service.key")),
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
        print("Successfully connected to Aiven Kafka as Consumer.")
        
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
                
            try:
                record = json.loads(msg.value().decode('utf-8'))
                
                # Perform any lightweight "Silver" cleaning here if needed
                if record.get("Entering_Chilled_Water_Temperature_Sensor") is not None:
                    global total_messages_received
                    latest_messages.append(record)
                    total_messages_received += 1
                    if len(latest_messages) > MAX_BUFFER_SIZE:
                        latest_messages.pop(0)
            except Exception as e:
                print(f"Error parsing message: {e}")
                
    except Exception as e:
        print(f"Failed to start Kafka consumer: {e}")

# Start the background consumer thread when the app starts
@app.on_event("startup")
async def startup_event():
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()

@app.get("/api/stream")
async def message_stream(request: Request):
    async def event_generator():
        global total_messages_received
        last_index = total_messages_received
        
        # On connection, send the most recent message if it exists
        if latest_messages:
            yield {
                "event": "message",
                "id": str(last_index),
                "retry": 15000,
                "data": json.dumps(latest_messages[-1])
            }
            
        while True:
            # If client closes connection, stop sending
            if await request.is_disconnected():
                break

            # If there are new messages
            if total_messages_received > last_index:
                # Send the latest message
                new_msg = latest_messages[-1]
                yield {
                    "event": "message",
                    "id": str(total_messages_received),
                    "retry": 15000,
                    "data": json.dumps(new_msg)
                }
                last_index = total_messages_received
            
            await asyncio.sleep(1)

    return EventSourceResponse(event_generator())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8001, reload=True)
