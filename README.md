Ensure you have Docker desktop installed.

Launch Docker desktop before opening the project.

Run the following command in your terminal "docker-compose up -d" following which you should see kafka and zookeeper starting.

Run the following programs in different terminals to enable your data pipeline.

dynamic_patient_producer.py > processing_consumer.py > ml_consumer.py

Set your google api key before running agent_consumer.py by using the following command "set GOOGLE_API_KEY=YOUR_API_KEY_HERE" in the same terminal.
