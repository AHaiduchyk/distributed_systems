from flask import Flask, request, jsonify
import requests
from datetime import datetime
import logging
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)

# Logging configuration
logging.basicConfig(filename='master.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Master messages - list of dicts
messages = []
messages_lock = threading.Lock()  # Create a lock for synchronizing access to the messages list


# List of secondaries
secondaries = [
    'http://secondary_1:5001',
    'http://secondary_2:5002',
]

# Function to replicate the message to a secondary
def replicate_message(secondary, message_entry):
    try:
        response = requests.post(f'{secondary}/replicate', json=message_entry)
        return response.status_code == 200  # Return True if ACK received
    except Exception as e:
        logging.error(f"Secondary unreachable: {e}")
        return False  # Return False if there's an error

# Function to replicate the message to secondaries and wait for acknowledgments
def replicate_message_to_secondaries(message_entry, w, ack_event):
    ack_count = 1  # The master always gets the message

    # Use ThreadPoolExecutor to send requests to all secondaries in parallel
    with ThreadPoolExecutor(max_workers=len(secondaries)) as executor:
        # Submit a task for each secondary
        futures = {executor.submit(replicate_message, secondary, message_entry): secondary for secondary in secondaries}

        # Process the responses as they complete
        for future in as_completed(futures):
            if future.result():  # If ACK received
                ack_count += 1
                logging.info(f"ACK received from {futures[future]}")
            if ack_count >= w:  # Check if we have received enough ACKs
                ack_event.set()  # Enough ACKs received, unblock the client
                break

    # In case we didn't reach the required ACKs, unblock anyway after processing
    ack_event.set()

@app.route('/messages', methods=['POST'])
def add_message():
    data = request.json
    message = data.get('message')
    w = data.get('w', 1)  # Get write concern parameter from the request
    ack_event = threading.Event()  # Create an event to wait for ACKs

    if message:
        timestamp = datetime.now().isoformat()

        # Lock access to the shared 'messages' list
        with messages_lock:
            message_entry = {
                'message': message,
                'timestamp': timestamp,
                'id': len(messages) + 1  # Ensure message ID is unique and consistent
            }
            messages.append(message_entry)

        # Start the replication process in the background
        replication_thread = threading.Thread(target=replicate_message_to_secondaries, args=(message_entry, w, ack_event))
        replication_thread.start()

        # If w=1, return success immediately without waiting for ACKs from secondaries
        if w == 1:
            logging.info(f"Message received and processed without waiting for secondaries. Message: {message_entry}")
            return jsonify({'status': 'Message replicated', 'message': message_entry}), 200

        # Otherwise, wait for the required ACKs from secondaries
        ack_event.wait()  # Wait until the required ACKs are received
        return jsonify({'status': 'Message replicated', 'message': message_entry}), 200

    logging.warning('No message provided')
    return jsonify({'status': 'No message provided'}), 400


@app.route('/messages', methods=['GET'])
def get_messages():
    logging.info("Replicated messages requested")

    # Sort replicated messages by 'id'
    sorted_messages = sorted(messages, key=lambda msg: msg['id'])

    # Log the sorted replicated messages
    pretty_json = json.dumps({"messages": sorted_messages}, indent=4)
    logging.info(pretty_json)
    
    return jsonify({'messages': sorted_messages}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
