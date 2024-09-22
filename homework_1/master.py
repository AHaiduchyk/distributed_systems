from flask import Flask, request, jsonify
import requests
from datetime import datetime
import logging
import json

app = Flask(__name__)


logging.basicConfig(filename='master.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Master messages - list of dicts
messages = []

# List of secondaries
secondaries = [
    'http://secondary_1:5001',
    'http://secondary_2:5002',
]

@app.route('/messages', methods=['POST'])
def add_message():
    data = request.json
    message = data.get('message')
    
    if message:
        timestamp = datetime.now().isoformat()
        message_entry = {
            'message': message,
            'timestamp': timestamp
        }
        messages.append(message_entry)
        logging.info(f"Message added: {message_entry}")

        # Replicate message to secondaries
        for secondary in secondaries:
            try:
                response = requests.post(f'{secondary}/replicate', json=message_entry)
                if response.status_code != 200:
                    logging.error(f"Replication failed to {secondary}")
                    return jsonify({'status': 'Replication failed'}), 500
            except Exception as e:
                logging.error(f"Secondary unreachable: {e}")
                return jsonify({'status': 'Secondary unreachable', 'error': str(e)}), 500

        # Log the successfully replicated message
        logging.info(f"Message replicated: {message_entry}")
        return jsonify({'status': 'Message replicated', 'message': message_entry}), 200

    logging.warning('No message provided')
    return jsonify({'status': 'No message provided'}), 400

@app.route('/messages', methods=['GET'])
def get_messages():
    logging.info("Messages requested")
    
    # Format messages as pretty JSON
    pretty_json = json.dumps({"messages": messages}, indent=4)
    logging.info(pretty_json)  # Log formatted JSON

    return jsonify({'messages': messages}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
