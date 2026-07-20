import os
import json
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "hvac_telemetry")

def fetch_all_messages():
    """
    Connects to Aiven Kafka, consumes all unread messages from the beginning 
    of the retention window, and dumps them to raw_data.json.
    """
    if not KAFKA_BOOTSTRAP_SERVERS or KAFKA_BOOTSTRAP_SERVERS == "<YOUR_AIVEN_SERVICE_URI>":
        print("Kafka credentials not provided. Generating fake batch data instead.")
        # Fallback to generator
        import pipelines.generator_and_upload as gen
        gen.generate_dirty_hvac_data(1000)
        return

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'github_actions_batch_group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.getenv("KAFKA_SSL_CA_LOCATION", "ca.pem"),
        'ssl.certificate.location': os.getenv("KAFKA_SSL_CERT_LOCATION", "service.cert"),
        'ssl.key.location': os.getenv("KAFKA_SSL_KEY_LOCATION", "service.key"),
        'enable.auto.commit': True
    }
    
    try:
        consumer = Consumer(conf)
        consumer.subscribe([KAFKA_TOPIC])
        
        print("Connected to Aiven Kafka. Draining messages for batch processing...")
        
        all_records = []
        empty_polls = 0
        
        # Poll until no more messages are available (drain the topic)
        while empty_polls < 3: # Stop after 3 seconds of no messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                empty_polls += 1
                continue
                
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                empty_polls += 1
                continue
                
            # Reset empty polls
            empty_polls = 0
            
            try:
                record = json.loads(msg.value().decode('utf-8'))
                all_records.append(record)
            except Exception as e:
                print(f"JSON Parse Error: {e}")

        print(f"Drained {len(all_records)} real-time records from Kafka.")
        
        if len(all_records) == 0:
            print("No real-time data found. Generating a few fake records to keep pipeline from failing.")
            import pipelines.generator_and_upload as gen
            gen.generate_dirty_hvac_data(50)
            return

        # Write to real_time_data.json
        with open("real_time_data.json", "w") as f:
            for rec in all_records:
                f.write(json.dumps(rec) + "\n")
                
        print("Successfully wrote Kafka stream data to real_time_data.json.")
        
    except Exception as e:
        print(f"Error connecting to Kafka for batch: {e}")
        print("Falling back to random data generator...")
        import pipelines.generator_and_upload as gen
        gen.generate_dirty_hvac_data(1000)

if __name__ == "__main__":
    fetch_all_messages()
