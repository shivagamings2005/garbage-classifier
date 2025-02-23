import yolov5
import torch
import random
from pathlib import Path
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime
import os

# MySQL Configuration (TiDB Serverless)
db_config = {
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'user': '5ztcqT1EBcgYB5u.root',
    'password': '92t7gF7zq3Oz9eKb',
    'database': 'satwa',
    'port': 4000,
    'ssl_ca': '/etc/ssl/certs/ca-certificates.crt',  # Works on Spaces
    'ssl_verify_cert': True,
    'ssl_verify_identity': True
}

# Connect to MySQL
try:
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()
    print("Connected to MySQL database successfully.")
except mysql.connector.Error as err:
    print(f"Failed to connect to MySQL: {err}")
    exit()

# Create table
create_table_query = """
CREATE TABLE IF NOT EXISTS garbage_classification (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME,
    payload_value INT,
    predicted_class VARCHAR(255)
)
"""
try:
    cursor.execute(create_table_query)
    db.commit()
    print("Table checked/created.")
except mysql.connector.Error as err:
    print(f"Error creating table: {err}")
    exit()

# Force CPU (Spaces free tier is CPU-only)
device = 'cpu'
print(f"Using device: {device}")

# Load model
model = yolov5.load('keremberke/yolov5m-garbage')
model.to(device)

# Model parameters
model.conf = 0.25
model.iou = 0.45
model.agnostic = False
model.multi_label = False
model.max_det = 1000

class_names = model.names

# Base directory in Spaces
base_dir = "/data/garbage_classification"
base_path = Path(base_dir)

if not base_path.exists() or not base_path.is_dir():
    print(f"Error: Directory '{base_dir}' not found.")
    exit()

subfolders = sorted([f for f in base_path.rglob('*') if f.is_dir()])
if not subfolders:
    print(f"Error: No subfolders found in '{base_dir}'.")
    exit()

print(f"Found {len(subfolders)} subfolders.")

def get_folder_index(value):
    if 1 <= value <= 11: return 0
    elif 12 <= value <= 22: return 1
    elif 23 <= value <= 33: return 2
    elif 34 <= value <= 44: return 3
    elif 45 <= value <= 55: return 4
    elif 56 <= value <= 66: return 5
    elif 67 <= value <= 77: return 6
    elif 78 <= value <= 88: return 7
    elif 89 <= value <= 99: return 8
    else: return -1

def process_random_image(subfolder, payload_value):
    image_extensions = ('.jpg', '.jpeg', '.png', '.bmp')
    images = [f for f in subfolder.iterdir() if f.is_file() and f.suffix.lower() in image_extensions]
    
    if not images:
        print(f"No images in {subfolder}")
        return None
    
    random_image = random.choice(images)
    print(f"Selected image: {random_image}")

    results = model(str(random_image), size=640)
    predictions = results.pred[0]
    
    predicted_class = "None"
    if len(predictions) > 0:
        scores = predictions[:, 4]
        top_idx = scores.argmax()
        top_score = scores[top_idx]
        top_cat = int(predictions[top_idx, 5])
        predicted_class = class_names[top_cat] if top_cat < len(class_names) else "Unknown"
        print(f"Class: {predicted_class}, Confidence: {top_score:.2f}")
    else:
        print("No objects detected.")
    
    timestamp = datetime.now()
    insert_query = "INSERT INTO garbage_classification (timestamp, payload_value, predicted_class) VALUES (%s, %s, %s)"
    try:
        cursor.execute(insert_query, (timestamp, payload_value, predicted_class))
        db.commit()
        print("Data saved to database.")
    except mysql.connector.Error as err:
        print(f"Error saving to database: {err}")
    
    return predicted_class

def on_message(client, userdata, msg):
    try:
        value = int(msg.payload.decode())
        print(f"Received MQTT value: {value}")
        
        folder_idx = get_folder_index(value)
        
        if folder_idx != -1 and folder_idx < len(subfolders):
            selected_folder = subfolders[folder_idx]
            print(f"Mapped to folder: {selected_folder}")
            process_random_image(selected_folder, value)
        else:
            timestamp = datetime.now()
            insert_query = "INSERT INTO garbage_classification (timestamp, payload_value, predicted_class) VALUES (%s, %s, %s)"
            cursor.execute(insert_query, (timestamp, value, "Out of Range"))
            db.commit()
            print("Value out of range. Saved to database.")
    except ValueError:
        print("Invalid payload - must be an integer")

# MQTT Setup
broker = "test.mosquitto.org"
topic = "garbage/index"

client = mqtt.Client()
client.on_message = on_message

client.connect(broker, 1883, 60)
client.subscribe(topic)

print(f"Subscribed to {topic}, waiting for messages...")
client.loop_forever()  # Runs indefinitely
