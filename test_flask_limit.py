from flask import Flask, request
import requests
import threading
import time

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

@app.route('/test', methods=['POST'])
def test():
    data = request.form.get('big_field', '')
    return f"Received {len(data)} bytes"

def run_server():
    app.run(port=5005)

threading.Thread(target=run_server, daemon=True).start()
time.sleep(2)

big_data = "A" * (1024 * 1024) # 1 MB
try:
    r = requests.post("http://127.0.0.1:5005/test", data={'big_field': big_data})
    print(r.status_code)
    print(r.text)
except Exception as e:
    print(e)
