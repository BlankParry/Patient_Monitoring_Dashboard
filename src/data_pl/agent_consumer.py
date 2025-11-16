# agent_consumer.py (v2 - Robust)
import json
import os
import google.generativeai as genai
from kafka import KafkaConsumer, KafkaProducer
import sys
import time

def configure_llm():
    """Configures the Gemini API."""
    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print("Error: GOOGLE_API_KEY environment variable not set.")
            sys.exit(1)
            
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-flash')
        print("Google Gemini model configured successfully.")
        return model
    except Exception as e:
        print(f"Error configuring LLM: {e}")
        sys.exit(1)

def create_kafka_consumer(broker_url, input_topic):
    """Creates a consumer for the explained alerts topic."""
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[broker_url],
            auto_offset_reset='earliest',
            group_id='agent-recommender-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"Agent Consumer connected. Listening to '{input_topic}'.")
        return consumer
    except Exception as e:
        print(f"Error connecting consumer: {e}. Is Kafka running?")
        sys.exit(1)

def create_kafka_producer(broker_url):
    """Creates a producer for the final dashboard recommendations."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Agent Producer connected.")
        return producer
    except Exception as e:
        print(f"Error connecting producer: {e}")
        sys.exit(1)

def generate_recommendation(model, alert_data):
    """
    Uses the LLM to generate a human-readable recommendation.
    """
    try:
        shap_explanation = alert_data.get("shap_explanation", {})
        # Filter out any errors
        top_drivers = [k for k in shap_explanation.keys() if k != "error"]
        
        vitals = alert_data.get("patient_data", {})
        
        # --- THIS IS THE FIX ---
        # If the list is empty or has errors, fall back gracefully.
        if not top_drivers:
            drivers_text = "Primary factors not available (SHAP error)."
        else:
            # Safely get the top 3, even if there are only 1 or 2
            driver1 = top_drivers[0] if len(top_drivers) > 0 else "(none)"
            driver2 = top_drivers[1] if len(top_drivers) > 1 else "(none)"
            driver3 = top_drivers[2] if len(top_drivers) > 2 else "(none)"
            drivers_text = f"""
            The model's top 3 reasons for this alert are:
            1. {driver1}
            2. {driver2}
            3. {driver3}
            """
        # --- END FIX ---

        prompt = f"""
        You are a clinical assistant AI.
        A patient alert was triggered for 'High Risk'.
        
        Key Patient Vitals:
        - Heart Rate: {vitals.get('HeartRate')} bpm
        - Oxygen Saturation: {vitals.get('OxygenSaturation')} %
        - Systolic BP: {vitals.get('SystolicBloodPressure')} mmHg
        
        {drivers_text}

        Based on this, generate a single, concise recommendation 
        (less than 20 words) for the clinical staff.
        Start with "Recommendation:".
        """
        
        response = model.generate_content(prompt)
        return response.text.strip()
        
    except Exception as e:
        print(f"Error generating recommendation: {e}")
        return "Recommendation: Check patient vitals immediately."

def process_messages(consumer, producer, output_topic, llm_model):
    """Consumes, generates recommendations, and produces."""
    print("Starting Agent processing loop...")
    try:
        for message in consumer:
            alert_data = message.value
            patient_id = alert_data.get("patient_data", {}).get("PatientID", "N/A")
            
            print(f"Agent processing alert for Patient {patient_id[:8]}...")

            recommendation = generate_recommendation(llm_model, alert_data)
            
            dashboard_message = {
                "patient_data": alert_data["patient_data"],
                "alert_type": alert_data["alert_type"],
                "shap_explanation": alert_data["shap_explanation"],
                "agent_recommendation": recommendation
            }
            
            producer.send(output_topic, value=dashboard_message)
            print(f"Sent to dashboard: {recommendation}")

    except KeyboardInterrupt:
        print("\nStopping Agent processing.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("Agent Consumer and Producer closed.")

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    INPUT_TOPIC = "alerts_explained"
    OUTPUT_TOPIC = "dashboard_recommendations"
    
    llm_model = configure_llm()
    consumer = create_kafka_consumer(KAFKA_BROKER, INPUT_TOPIC)
    producer = create_kafka_producer(KAFKA_BROKER)
    
    if all([llm_model, consumer, producer]):
        process_messages(consumer, producer, OUTPUT_TOPIC, llm_model)