import threading
import requests
import time
import logging
from flask import Flask, request, jsonify
from datetime import datetime
import json
from multiprocessing import Value

app = Flask(__name__)

# Configure pretty logging
logging.basicConfig(filename='master.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

secondaries = {
    "http://secondary1:5001": "Healthy",
    "http://secondary2:5002": "Healthy"
}
heartbeat_interval = 10  # Heartbeat interval in seconds
heartbeat_timeout = 3  # Timeout for heartbeat requests
quorum_size = 2  # Required number of healthy secondaries for quorum
master_read_only = False  # Flag to track read-only mode

# Master messages - list of dicts
messages = []
messages_lock = threading.Lock()  # Lock for thread safety


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


def check_quorum():
    """Check if the number of healthy secondaries meets the quorum size."""
    global master_read_only
    healthy_count = sum(1 for status in secondaries.values() if status == "Healthy")
    
    if healthy_count < quorum_size:
        master_read_only = True
        pretty_log("Quorum not met. Master switching to read-only mode.", log_type='warning', quorum_size=quorum_size, healthy_count=healthy_count)
    else:
        master_read_only = False
        pretty_log("Quorum met. Master in write mode.", quorum_size=quorum_size, healthy_count=healthy_count)


def heartbeat_check():
    """Periodically checks the health of secondaries and updates the quorum status."""
    while True:
        for secondary_url in secondaries.keys():
            try:
                response = requests.get(f"{secondary_url}/heartbeat", timeout=heartbeat_timeout)
                if response.status_code == 200:
                    secondaries[secondary_url] = "Healthy"
                    pretty_log(f"Heartbeat check successful for {secondary_url}", status="Healthy")
                else:
                    secondaries[secondary_url] = "Suspected"
                    pretty_log(f"Heartbeat check for {secondary_url}", status="Suspected", response_code=response.status_code)
            except requests.exceptions.RequestException:
                secondaries[secondary_url] = "Unhealthy"
                pretty_log(f"Heartbeat check failed for {secondary_url}", status="Unhealthy", error="RequestException")
        
        # After heartbeat checks, update quorum status
        check_quorum()
        time.sleep(heartbeat_interval)


def replicate_to_secondary(secondary_url, message, ack_event, ack_count, retries=7):
    """Replicate the message to a secondary. If successful, increment the ack_count and set the ack_event if required."""
    for attempt in range(retries):
        try:
            response = requests.post(f'{secondary_url}/replicate', json=message, timeout=5)
            if response.status_code == 200:
                with ack_count.get_lock():
                    ack_count.value += 1
                    if ack_count.value >= ack_count.required:
                        ack_event.set()  # Signal the event if we have enough acks
                pretty_log(f"Replication successful for {secondary_url}", status="Success", message_id=message['id'])
                return True
        except requests.exceptions.RequestException as e:
            pretty_log(f"Replication failed for {secondary_url}", log_type='error', error=str(e), attempt=attempt + 1)
            time.sleep(3 ** attempt)  # Exponential backoff
    return False


@app.route('/replicate', methods=['POST'])
def replicate_message():
    global master_read_only
    
    if master_read_only:
        pretty_log("Master in read-only mode. Rejecting append request.", log_type='warning')
        return jsonify({'error': 'Quorum not met. Master is in read-only mode and cannot accept new messages.'}), 503

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

        pretty_log(f"Master received message", message=message, message_id=message_entry['id'])

    successful_acks = 1  # Acknowledge master

    # Event to signal when we've received enough acknowledgments
    ack_event = threading.Event()

    # A shared counter for acknowledgments
    ack_count = Value('i', successful_acks)  # Start with the master's acknowledgment
    ack_count.required = w  # Store the required number of acks

    threads = []

    # Launch threads for replication
    for secondary in secondaries.keys():
        if secondaries[secondary] == "Healthy":  # Only replicate to healthy secondaries
            t = threading.Thread(
                target=replicate_to_secondary,
                args=(secondary, message_entry, ack_event, ack_count)
            )
            t.start()
            threads.append(t)

    if w == 1:
        pretty_log(f"Returning immediately with w=1. Replication continues in background.", write_concern=w)
        return jsonify({'status': 'Message replicated', 'message': message}), 200

    ack_event.wait()  # This will return as soon as w acknowledgments are received

    pretty_log(f"ack_count.value: {ack_count.value}, required w: {w}")
    if ack_count.value >= w:
        return jsonify({'status': 'Message replicated', 'message': message}), 200
    else:
        return jsonify({'status': 'Replication failed'}), 500


@app.route('/health', methods=['GET'])
def get_health_status():
    """API to check the health status of secondaries."""
    pretty_log("Health status requested")
    return jsonify(secondaries), 200


@app.route('/quorum', methods=['GET'])
def get_quorum_status():
    """API to check if the master is in read-only mode."""
    status = 'Read-Only' if master_read_only else 'Write'
    pretty_log("Quorum status requested", quorum_met=not master_read_only, status=status)
    return jsonify({'quorum_met': not master_read_only, 'status': status}), 200


@app.route('/messages', methods=['GET'])
def get_messages():
    """API to get all replicated messages."""
    pretty_log("Replicated messages requested")
    with messages_lock:
        sorted_messages = sorted(messages, key=lambda msg: msg['id'])
    pretty_json = json.dumps({"messages": sorted_messages}, indent=4)
    pretty_log("Messages retrieved", messages=sorted_messages)
    return jsonify({'messages': sorted_messages}), 200


if __name__ == '__main__':
    heartbeat_thread = threading.Thread(target=heartbeat_check, daemon=True)
    heartbeat_thread.start()  # Start the heartbeat check thread
    app.run(host='0.0.0.0', port=5000)
