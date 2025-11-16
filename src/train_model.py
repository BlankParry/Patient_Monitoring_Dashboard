# train_model.py (v_random_forest)
import pandas as pd
# Import RandomForest
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score, classification_report
import joblib
import os

def train_model(csv_file_path):
    """
    Loads data, trains a RandomForest model, and saves
    the model and label encoders.
    """
    print(f"Loading dataset: {csv_file_path}")
    df = pd.read_csv(csv_file_path)

    # 1. Define Features (X) and Target (y)
    features = [
        'Heart Rate', 'Respiratory Rate', 'Body Temperature',
        'Oxygen Saturation', 'Systolic Blood Pressure', 'Diastolic Blood Pressure',
        'Age', 'Gender', 'Weight (kg)', 'Height (m)', 'Derived_HRV',
        'Derived_Pulse_Pressure', 'Derived_BMI', 'Derived_MAP'
    ]
    target = 'Risk Category'
    
    # 2. Encode Categorical Data
    print("Encoding 'Gender' column...")
    gender_encoder = LabelEncoder()
    # Modify the main DataFrame first
    df['Gender'] = gender_encoder.fit_transform(df['Gender'])
    
    # 3. Create X (now 100% numeric) and y
    X = df[features]
    y_raw = df[target]

    # 4. Map Target Variable
    # Map "Low Risk" to 0 and "High Risk" to 1
    y_binary = y_raw.map({'Low Risk': 0, 'High Risk': 1})
        
    # We still need the original risk_encoder for the labels
    risk_encoder = LabelEncoder()
    risk_encoder.fit(y_raw)

    # 5. Split Data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y_binary, test_size=0.2, random_state=42, stratify=y_binary
    )
    
    print("Training RandomForest model...")
    # 6. Train Model
    # --- THIS IS THE NEW MODEL ---
    model = RandomForestClassifier(
        n_estimators=100, # A good default
        random_state=42,
        n_jobs=-1 # Use all available CPU cores
    )
    # --- END NEW MODEL ---
    
    # This will now succeed
    model.fit(X_train, y_train)

    # 7. Evaluate Model
    preds = model.predict(X_test)
    accuracy = accuracy_score(y_test, preds)
    print(f"\nModel trained. Accuracy: {accuracy * 100:.2f}%")
    print("Classification Report:")
    print(classification_report(y_test, preds, target_names=['Low Risk (0)', 'High Risk (1)']))

    # 8. Save Artifacts
    # --- THIS IS THE NEW SAVE METHOD ---
    # Save as a .pkl file using joblib
    joblib.dump(model, "risk_model.pkl")
    # --- END NEW SAVE METHOD ---
    
    joblib.dump(gender_encoder, "gender_encoder.pkl")
    joblib.dump(risk_encoder, "risk_encoder.pkl")
    
    print("Saved 'risk_model.pkl', 'gender_encoder.pkl', and 'risk_encoder.pkl'")

if __name__ == "__main__":
    CSV_FILE = "human_vital_signs_dataset_2024.csv"
    
    # Clean up old model files
    if os.path.exists("risk_model.json"):
        os.remove("risk_model.json")
        print("Removed old .json model file.")
    if os.path.exists("risk_model.pkl"):
        os.remove("risk_model.pkl")
        print("Removed old .pkl model file.")
    
    train_model(CSV_FILE)

