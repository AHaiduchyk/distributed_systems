from flask import Flask, request, jsonify
import time
import logging
import json
import random
from datetime import datetime

app = Flask(__name__)

# Logging setup
logging.basicConfig(filename='secondary_2.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replicated messages - list of dicts
replicated_messages = []
message_ids = set()  # Track message IDs for deduplication

# Simulate delay for eventual consistency
# delay_time = [30, 60, 90, 120]  # in seconds

# Chance of a random internal server error or missed POST request (for testing retry)
# missed_request_chance = 0.2  # 20% chance of a missed request

def pretty_log(msg, log_type='info', **kwargs):
    """Pretty log helper for structured logs."""
    log_entry = {
        'event': msg,
        'timestamp': datetime.now().isoformat(),
        'details': kwargs
    }
    if log_type == 'info':
        logging.info(json.dumps(log_entry, indent=4))
    elif log_type == 'warning':
        logging.warning(json.dumps(log_entry, indent=4))
    elif log_type == 'error':
        logging.error(json.dumps(log_entry, indent=4))
        
@app.route('/replicate', methods=['POST'])
def replicate_message():
    # Simulate network failure or unavailability (missed POST request)
    # if random.random() < missed_request_chance:
    #     logging.error("Simulated network failure: POST request not received")
    #     return jsonify({'status': 'POST request failed (simulated)'}), 500

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
        # time.sleep(random.choice(delay_time))

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

    # Track received IDs
    received_ids = set(msg['id'] for msg in sorted_messages)

    # Check if any message is missing its predecessor
    filtered_messages = [
        msg for msg in sorted_messages
        if all(prev_id in received_ids for prev_id in range(1, msg['id']))
    ]

    # Log the filtered replicated messages
    logging.info(f"Filtered messages: {filtered_messages}")

    return jsonify({'messages': filtered_messages}), 200

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    """Heartbeat endpoint to indicate the secondary is healthy."""
    pretty_log("Heartbeat received from master", status="Healthy")
    return jsonify({'status': 'Healthy'}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)  # Secondary2
