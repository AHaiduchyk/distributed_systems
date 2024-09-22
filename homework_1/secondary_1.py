from flask import Flask, request, jsonify
import time
import logging
import json

app = Flask(__name__)

# Налаштування логування
logging.basicConfig(filename='secondary_1.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Replicated messages - list of dicts
replicated_messages = []

@app.route('/replicate', methods=['POST'])
def replicate_message():
    data = request.json
    message = data.get('message')
    timestamp = data.get('timestamp')
    
    if message and timestamp:
        time.sleep(2)  # Імітуємо затримку
        replicated_message_entry = {
            'message': message,
            'timestamp': timestamp
        }
        replicated_messages.append(replicated_message_entry)
        
        # Log the replicated message
        logging.info(f"Message replicated: {replicated_message_entry}")
        return jsonify({'status': 'Message replicated'}), 200
    
    logging.warning('No message provided for replication')
    return jsonify({'status': 'No message provided'}), 400

@app.route('/messages', methods=['GET'])
def get_messages():
    logging.info("Replicated messages requested")
    
    # Log the current replicated messages for better insight
    pretty_json = json.dumps({"messages": replicated_messages}, indent=4)
    logging.info(pretty_json)
    
    return jsonify({'messages': replicated_messages}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)  # Secondary1
