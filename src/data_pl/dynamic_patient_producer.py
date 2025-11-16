#dynamic_patient_producer.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Initialize Faker to create the patient's identity
fake = Faker()

class Patient:
    """
    Manages the state and vital signs for a single simulated patient.
    """
    def __init__(self, patient_id):
        # --- 1. Static Demographics (set once) ---
        self.id = patient_id
        self.age = fake.random_int(min=18, max=90)
        self.gender = random.choice(['Male', 'Female'])
        self.weight_kg = round(random.uniform(50.0, 120.0), 2)
        self.height_m = round(random.uniform(1.5, 2.0), 2)
        
        # --- 2. Dynamic State (changes over time) ---
        self.condition = "Stable"
        # Base vitals for a 'Stable' patient
        self.base_hr = 75
        self.base_resp_rate = 16
        self.base_temp = 36.8
        self.base_o2_sat = 98.0
        self.base_systolic_bp = 120
        self.base_diastolic_bp = 80
        
        print(f"Simulating Patient {self.id[:8]}... "
              f"Age: {self.age}, Gender: {self.gender}")
        print("Initial condition: Stable")

    def simulate_event(self):
        """
        Randomly triggers a change in the patient's condition.
        This function introduces the "unpredictable" element.
        """
        # 5% chance each second to trigger an event
        if random.random() < 0.05:
            if self.condition == "Stable":
                self.condition = "Critical"
                print("\n*** EVENT: Patient condition changing to CRITICAL ***\n")
                self.base_hr = 130       # Tachycardia
                self.base_resp_rate = 28 # Tachypnea
                self.base_o2_sat = 91.0    # Hypoxia
            else:
                self.condition = "Stable"
                print("\n*** EVENT: Patient condition changing to STABLE ***\n")
                self.base_hr = 75
                self.base_resp_rate = 16
                self.base_o2_sat = 98.0

    def get_vitals(self):
        """
        Generates a new set of vitals based on the patient's
        current condition, adding a small "jitter" to look realistic.
        """
        jitter = lambda base, amount: base + random.randint(-amount, amount)
        
        vitals = {
            "HeartRate": jitter(self.base_hr, 3),
            "RespiratoryRate": jitter(self.base_resp_rate, 1),
            "BodyTemperature": round(self.base_temp + random.uniform(-0.2, 0.2), 2),
            "OxygenSaturation": round(self.base_o2_sat + random.uniform(-0.5, 0.5), 2),
            "SystolicBloodPressure": jitter(self.base_systolic_bp, 5),
            "DiastolicBloodPressure": jitter(self.base_diastolic_bp, 3),
        }
        
        # Ensure vitals stay in a realistic range
        vitals["OxygenSaturation"] = min(vitals["OxygenSaturation"], 100.0)
        return vitals

    def get_data_payload(self):
        """Combines static demographics and dynamic vitals."""
        self.simulate_event()
        current_vitals = self.get_vitals()
        
        payload = {
            "PatientID": self.id,
            "Timestamp": datetime.now().isoformat(),
            "Condition": self.condition, # Our simulation flag
            **current_vitals,
            "Age": self.age,
            "Gender": self.gender,
            "Weight_kg": self.weight_kg,
            "Height_m": self.height_m
        }
        return payload

def create_kafka_producer(broker_url="localhost:9092"):
    """Creates and returns a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer connected successfully.")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

def stream_data(producer, topic_name):
    """Continuously generates and sends data for one patient."""
    if producer is None: return

    patient = Patient(patient_id=fake.uuid4())
    
    try:
        while True:
            data = patient.get_data_payload()
            producer.send(topic_name, value=data)
            print(f"Sent: {data['Condition']} | "
                  f"HR: {data['HeartRate']} | "
                  f"O2: {data['OxygenSaturation']}")
            time.sleep(5) # Simulates one reading per second
            
    except KeyboardInterrupt:
        print("\nStopping data generation.")
    finally:
        producer.flush()
        producer.close()
        print("Kafka Producer closed.")

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    KAFKA_TOPIC = "vitals_raw"
    
    kafka_producer = create_kafka_producer(KAFKA_BROKER)
    
    if kafka_producer:
        stream_data(kafka_producer, KAFKA_TOPIC)

