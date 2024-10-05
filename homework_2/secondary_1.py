from flask import Flask, request, jsonify
import time
import logging
import json
import random

app = Flask(__name__)

# Logging setup
logging.basicConfig(filename='secondary_1.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replicated messages - list of dicts
replicated_messages = []
message_ids = set()  # Track message IDs for deduplication

delay_time= [30,60,90,120]

@app.route('/replicate', methods=['POST'])
def replicate_message():
    data = request.json
    message_id = data.get('id')  # The message ID from the master
    message = data.get('message')
    timestamp = data.get('timestamp')
    
    if message and timestamp and message_id:
        # Deduplication: Skip if message with this ID already exists
        if message_id in message_ids:
            logging.info(f"Duplicate message ignored: {message_id}")
            return jsonify({'status': 'Duplicate message ignored'}), 200

        # Simulate delay for eventual consistency
        time.sleep(random.choice(delay_time)) 

        # Append the replicated message
        replicated_message_entry = {
            'id': message_id,
            'message': message,
            'timestamp': timestamp
        }
        replicated_messages.append(replicated_message_entry)
        message_ids.add(message_id)  # Track this ID to prevent duplication

        # Log the replicated message
        logging.info(f"Message replicated: {replicated_message_entry}")
        return jsonify({'status': 'Message replicated'}), 200
    
    logging.warning('Invalid data provided for replication')
    return jsonify({'status': 'Invalid data provided'}), 400

@app.route('/messages', methods=['GET'])
def get_messages():
    logging.info("Replicated messages requested")

    # Sort replicated messages by 'id'
    sorted_messages = sorted(replicated_messages, key=lambda msg: msg['id'])

    # Log the sorted replicated messages
    pretty_json = json.dumps({"messages": sorted_messages}, indent=4)
    logging.info(pretty_json)
    
    return jsonify({'messages': sorted_messages}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)  # Secondary1
