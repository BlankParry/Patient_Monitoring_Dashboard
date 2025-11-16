# processing_consumer.py
import json
from kafka import KafkaConsumer, KafkaProducer
import sys

def create_kafka_consumer(broker_url, input_topic):
    """Creates and returns a KafkaConsumer instance."""
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[broker_url],
            auto_offset_reset='earliest',
            group_id='vitals-processor-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Kafka Consumer connected. Listening to '{input_topic}'.")
        return consumer
    except Exception as e:
        print(f"Error connecting consumer: {e}")
        sys.exit(1)

def create_kafka_producer(broker_url):
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer connected (for publishing enriched data).")
        return producer
    except Exception as e:
        print(f"Error connecting producer: {e}")
        sys.exit(1)

def calculate_derived_fields(data):
    """Calculates BMI, MAP, and Pulse Pressure from the raw data."""
    try:
        systolic = data['SystolicBloodPressure']
        diastolic = data['DiastolicBloodPressure']
        weight_kg = data['Weight_kg']
        height_m = data['Height_m']
        
        # 1. Calculate Mean Arterial Pressure (MAP)
        data['Derived_MAP'] = round(((1/3) * systolic) + ((2/3) * diastolic), 2)
        
        # 2. Calculate Pulse Pressure (PP)
        data['Derived_Pulse_Pressure'] = systolic - diastolic
        
        # 3. Calculate Body Mass Index (BMI)
        data['Derived_BMI'] = round(weight_kg / (height_m ** 2), 2)
            
    except Exception as e:
        print(f"Warning: Error in calculation: {e}")
    
    return data

def process_messages(consumer, producer, output_topic):
    """The main processing loop. Consumes, calculates, and produces."""
    print("Starting processing loop...")
    try:
        for message in consumer:
            enriched_data = calculate_derived_fields(message.value)
            producer.send(output_topic, value=enriched_data)
            
            print(f"Processed: Patient {enriched_data['PatientID'][:8]}... | "
                  f"Added BMI={enriched_data.get('Derived_BMI')}, "
                  f"MAP={enriched_data.get('Derived_MAP')}")
            
    except KeyboardInterrupt:
        print("\nStopping processing.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("Consumer and Producer closed.")

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    INPUT_TOPIC = "vitals_raw"
    OUTPUT_TOPIC = "vitals_enriched"
    
    kafka_consumer = create_kafka_consumer(KAFKA_BROKER, INPUT_TOPIC)
    kafka_producer = create_kafka_producer(KAFKA_BROKER)
    
    if kafka_consumer and kafka_producer:
        process_messages(kafka_consumer, kafka_producer, OUTPUT_TOPIC)

