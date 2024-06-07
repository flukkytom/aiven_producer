from flask import Flask
import threading
import producer

app = Flask(__name__)

def run_producer():
    producer.produce_messages()

if __name__ == '__main__':
    # Start the producer in a separate thread
    threading.Thread(target=run_producer).start()
    # Run the Flask app
    app.run(host='0.0.0.0', port=5001)
