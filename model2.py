import yolov5
import random
from pathlib import Path
import paho.mqtt.client as mqtt
import mysql.connector
from datetime import datetime
import os

# MySQL Configuration with TiDB Serverless
db_config = {
    'host': os.getenv('MYSQL_HOST', 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com'),
    'user': os.getenv('MYSQL_USER', '5ztcqT1EBcgYB5u.root'),
    'password': os.getenv('MYSQL_PASSWORD', '92t7gF7zq3Oz9eKb'),
    'database': 'satwa',
    'port': 4000,
    'ssl_ca': '/etc/ssl/certs/ca-certificates.crt',  # Common CA path on Render
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

# Create table if it doesn’t exist
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
    print("Table 'garbage_classification' checked/created.")
except mysql.connector.Error as err:
    print(f"Error creating table: {err}")
    exit()

# Force CPU usage (Render free tier has no GPU)
device = 'cpu'
print(f"Using device: {device}")

# Load model
model = yolov5.load('keremberke/yolov5m-garbage')
model.to(device)

# Set model parameters
model.conf = 0.25
model.iou = 0.45
model.agnostic = False
model.multi_label = False
model.max_det = 1000

# Get class names
class_names = model.names

# Set base directory (for Render)
base_dir = "/app/data/garbage_classification"
base_path = Path(base_dir)

# Verify directory
if not base_path.exists() or not base_path.is_dir():
    print(f"Error: Directory '{base_dir}' not found or is not a directory.")
    exit()

# Get sorted list of subfolders
subfolders = sorted([f for f in base_path.rglob('*') if f.is_dir()])
if not subfolders:
    print(f"Error: No subfolders found in '{base_dir}'.")
    exit()

print(f"Found {len(subfolders)} subfolders.")

# Define range mapping
def get_folder_index(value):
    if 1 <= value <= 11:
        return 0
    elif 12 <= value <= 22:
        return 1
    elif 23 <= value <= 33:
        return 2
    elif 34 <= value <= 44:
        return 3
    elif 45 <= value <= 55:
        return 4
    elif 56 <= value <= 66:
        return 5
    elif 67 <= value <= 77:
        return 6
    elif 78 <= value <= 88:
        return 7
    elif 89 <= value <= 99:
        return 8
    else:
        return -1

# Process image and store in database
def process_random_image(subfolder, payload_value):
    image_extensions = ('.jpg', '.jpeg', '.png', '.bmp')
    images = [f for f in subfolder.iterdir() if f.is_file() and f.suffix.lower() in image_extensions]
    
    if not images:
        print(f"No images found in folder: {subfolder}")
        return None
    
    random_image = random.choice(images)
    print(f"\nSelected image: {random_image}")

    results = model(str(random_image), size=640)
    predictions = results.pred[0]
    
    predicted_class = "None"
    if len(predictions) > 0:
        scores = predictions[:, 4]
        top_idx = scores.argmax()
        top_score = scores[top_idx]
        top_cat = int(predictions[top_idx, 5])
        predicted_class = class_names[top_cat] if top_cat < len(class_names) else "Unknown"
        print(f"Top Classification:")
        print(f"Class Name: {predicted_class}")
        print(f"Confidence: {top_score:.2f}")
    else:
        print("No objects detected in the image.")
    
    # Store in database
    timestamp = datetime.now()
    insert_query = "INSERT INTO garbage_classification (timestamp, payload_value, predicted_class) VALUES (%s, %s, %s)"
    try:
        cursor.execute(insert_query, (timestamp, payload_value, predicted_class))
        db.commit()
        print("Data saved to database.")
    except mysql.connector.Error as err:
        print(f"Error saving to database: {err}")
    
    return predicted_class

# MQTT callback
def on_message(client, userdata, msg):
    try:
        value = int(msg.payload.decode())
        print(f"\nReceived MQTT value: {value}")
        
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
            print("Value out of range or no corresponding folder. Saved to database.")
    except ValueError:
        print("Invalid payload received - must be an integer")

# MQTT Setup
broker = "test.mosquitto.org"
topic = "garbage/index"

client = mqtt.Client()
client.on_message = on_message

client.connect(broker, 1883, 60)
client.subscribe(topic)

print(f"Subscribed to {topic}, waiting for messages...")
try:
    client.loop_forever()
except KeyboardInterrupt:
    print("\nShutting down...")
    cursor.close()
    db.close()
    client.disconnect()