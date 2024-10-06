import threading
import requests
import time
import logging
from flask import Flask, request, jsonify
from datetime import datetime
import json
from multiprocessing import Value  # Correct import

app = Flask(__name__)
logging.basicConfig(filename='secondary_1.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

secondaries = ["http://secondary1:5001", "http://secondary2:5002"]

# Master messages - list of dicts
messages = []
messages_lock = threading.Lock()  # Lock for thread safety


def replicate_to_secondary(secondary_url, message, ack_event, ack_count, retries=7):
    """
    Replicate the message to a secondary. If successful, increment the ack_count and set the ack_event if required.
    """
    for attempt in range(retries):
        try:
            response = requests.post(f'{secondary_url}/replicate', json=message, timeout=5)
            if response.status_code == 200:
                # Increment the ack_count and check if we've hit the target number of acks (w)
                with ack_count.get_lock():
                    ack_count.value += 1
                    if ack_count.value >= ack_count.required:
                        ack_event.set()  # Signal the event if we have enough acks
                return True
        except requests.exceptions.RequestException as e:
            logging.error(f"Replication to {secondary_url} failed: {str(e)}. Retrying...")
            time.sleep(3 ** attempt)  # Exponential backoff
    return False


@app.route('/replicate', methods=['POST'])
def replicate_message():
    data = request.json
    message = data.get('message')
    w = data.get('w', 1)  # Get write concern parameter from the request

    if message:
        timestamp = datetime.now().isoformat()
        message_entry = {
            'message': message,
            'timestamp': timestamp,
            'id': len(messages) + 1  # Assuming ID is based on the length of messages
        }

        # Ensure thread-safe access to the messages list
        with messages_lock:
            messages.append(message_entry)

    # Log the message to master
    logging.info(f"Master received message: {message}")

    successful_acks = 1  # Acknowledge master

    # Event to signal when we've received enough acknowledgments
    ack_event = threading.Event()

    # A shared counter for acknowledgments
    ack_count = Value('i', successful_acks)  # Start with the master's acknowledgment
    ack_count.required = w  # Store the required number of acks

    threads = []

    # Launch threads for replication (this happens regardless of w)
    for secondary in secondaries:
        t = threading.Thread(
            target=replicate_to_secondary,
            args=(secondary, message_entry, ack_event, ack_count)
        )
        t.start()
        threads.append(t)

    # If w=1, return immediately without waiting for any secondary acknowledgment
    if w == 1:
        logging.info(f"Returning immediately with w=1. Replication will continue in background.")
        return jsonify({'status': 'Message replicated', 'message': message}), 200

    # Wait for the acknowledgment event to be set (once we hit the required number of acks)
    ack_event.wait()  # This will return as soon as w acknowledgments are received

    logging.info(f"ack_count.value: {ack_count.value}, w: {w}")
    # Once we have enough acknowledgments, return success, but replication will still continue in the background
    if ack_count.value >= w:
        return jsonify({'status': 'Message replicated', 'message': message}), 200
    else:
        return jsonify({'status': 'Replication failed'}), 500



@app.route('/messages', methods=['GET'])
def get_messages():
    logging.info("Replicated messages requested")

    # Sort replicated messages by 'id'
    with messages_lock:
        sorted_messages = sorted(messages, key=lambda msg: msg['id'])

    # Log the sorted replicated messages
    pretty_json = json.dumps({"messages": sorted_messages}, indent=4)
    logging.info(pretty_json)

    return jsonify({'messages': sorted_messages}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
