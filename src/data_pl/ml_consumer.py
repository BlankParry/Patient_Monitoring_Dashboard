# ml_consumer.py (v_random_forest_final_fix_4)
import json
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import shap
from kafka import KafkaConsumer, KafkaProducer
import sys
import numpy as np

def load_artifacts():
    """Loads the RandomForest .pkl model and encoders."""
    try:
        model = joblib.load(r"..\model_encs\risk_model.pkl")
        gender_encoder = joblib.load(r"..\model_encs\gender_encoder.pkl")
        risk_encoder = joblib.load(r"..\model_encs\risk_encoder.pkl")
        print("Model and encoders loaded successfully.")
        
        explainer = shap.TreeExplainer(model)
        print("SHAP TreeExplainer created successfully.")
        
        return model, gender_encoder, risk_encoder, explainer
        
    except FileNotFoundError:
        print("Error: Model files not found. Run 'train_model.py' first.")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading artifacts: {e}")
        sys.exit(1)

def create_kafka_consumer(broker_url, input_topic):
    """Creates a consumer for the enriched vitals topic."""
    try:
        consumer = KafkaConsumer(
            input_topic,
            bootstrap_servers=[broker_url],
            auto_offset_reset='earliest',
            group_id='ml-risk-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print(f"ML Consumer connected. Listening to '{input_topic}'.")
        return consumer
    except Exception as e:
        print(f"Error connecting consumer: {e}")
        sys.exit(1)

def create_kafka_producer(broker_url):
    """Creates a producer for the explained alerts topic."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker_url],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("ML Producer connected.")
        return producer
    except Exception as e:
        print(f"Error connecting producer: {e}")
        sys.exit(1)

def get_shap_explanation(explainer, data_row):
    """Generates a SHAP explanation for a single data row."""
    try:
        # shap_values is a single 3D array: (1 sample, 14 features, 2 classes)
        shap_values = explainer.shap_values(data_row)
        
        # --- THIS IS THE FIX ---
        # We slice the 3D array to get:
        # [0, :, 1] -> Sample 0, All Features (:), Class 1
        # This gives us a 1D array of shape (14,)
        shap_values_for_row = shap_values[0, :, 1]
        # --- END FIX ---

        feature_names = data_row.columns
        
        # 'value' will now be a single float, and round() will work.
        explanation = {
            feature: round(value, 3)
            for feature, value in zip(feature_names, shap_values_for_row)
        }
        
        # Sort by absolute impact
        sorted_explanation = sorted(
            explanation.items(),
            key=lambda item: abs(item[1]),
            reverse=True
        )
        return dict(sorted_explanation)
    except Exception as e:
        print(f"Error generating SHAP values: {e}")
        return {"error": str(e)}

def process_messages(consumer, producer, output_topic, model, g_encoder, r_encoder, explainer):
    """Consumes, predicts, explains, and produces."""
    print("Starting ML processing loop...")
    
    training_cols = [
        'Heart Rate', 'Respiratory Rate', 'Body Temperature',
        'Oxygen Saturation', 'Systolic Blood Pressure', 'Diastolic Blood Pressure',
        'Age', 'Gender', 'Weight (kg)', 'Height (m)', 'Derived_HRV',
        'Derived_Pulse_Pressure', 'Derived_BMI', 'Derived_MAP'
    ]
    
    try:
        for message in consumer:
            data = message.value
            
            df_row = pd.DataFrame([data])
            df_row = df_row.rename(columns={
                "HeartRate": "Heart Rate", "RespiratoryRate": "Respiratory Rate",
                "BodyTemperature": "Body Temperature", "OxygenSaturation": "Oxygen Saturation",
                "SystolicBloodPressure": "Systolic Blood Pressure",
                "DiastolicBloodPressure": "Diastolic Blood Pressure",
                "Weight_kg": "Weight (kg)", "Height_m": "Height (m)"
            })
            for col in training_cols:
                if col not in df_row.columns:
                    df_row[col] = 0
            df_row['Gender'] = g_encoder.transform(df_row['Gender'])
            df_row = df_row.reindex(columns=training_cols, fill_value=0)

            prediction_prob = model.predict_proba(df_row)[0][1] # Prob of class 1
            
            if prediction_prob > 0.5:
                prediction_label = "High Risk"
                print(f"Processed Patient {data['PatientID'][:8]}: RISK={prediction_label} (Prob: {prediction_prob:.2f})")
                
                explanation = get_shap_explanation(explainer, df_row)
                
                if "error" in explanation:
                    print(f"SHAP Error: {explanation['error']}")
                
                alert_message = {
                    "alert_type": "High Risk Patient",
                    "patient_data": data,
                    "prediction": prediction_label,
                    "probability": round(float(prediction_prob), 4),
                    "shap_explanation": explanation
                }
                
                producer.send(output_topic, value=alert_message)
            else:
                prediction_label = "Low Risk"
                print(f"Processed Patient {data['PatientID'][:8]}: RISK={prediction_label} (Prob: {prediction_prob:.2f})")

    except KeyboardInterrupt:
        print("\nStopping ML processing.")
    finally:
        consumer.close()
        producer.flush()
        producer.close()
        print("ML Consumer and Producer closed.")

if __name__ == "__main__":
    KAFKA_BROKER = "localhost:9092"
    INPUT_TOPIC = "vitals_enriched"
    OUTPUT_TOPIC = "alerts_explained"
    
    artifacts = load_artifacts()
    
    if artifacts:
        model, g_enc, r_enc, explainer = artifacts
        consumer = create_kafka_consumer(KAFKA_BROKER, INPUT_TOPIC)
        producer = create_kafka_producer(KAFKA_BROKER)
        
        if all([consumer, producer]):
            process_messages(consumer, producer, OUTPUT_TOPIC, model, g_enc, r_enc, explainer)