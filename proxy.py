#!/usr/bin/env python3
"""
Stratum HTTP Proxy - Python version
Bridges Cloudflare Workers (HTTP) to mining pool (Stratum TCP)
"""

import socket
import json
import threading
import time
import logging
from flask import Flask, request, jsonify
from flask_cors import CORS

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# Pool config
POOL_HOST = 'na.luckpool.net'
POOL_PORT = 3956
NONCE_BATCH_SIZE = 56

# Active connections
connections = {}
connections_lock = threading.Lock()

# Nonce state per job
nonce_state = {
    'job_id': None,
    'current': 0,
    'max': 0xFFFFFFFF
}
nonce_lock = threading.Lock()


class StratumConnection:
    def __init__(self, worker_id):
        self.worker_id = worker_id
        self.sock = None
        self.subscribed = False
        self.authorized = False
        self.extranonce1 = None
        self.extranonce2_size = None
        self.difficulty = None
        self.job = None
        self.responses = {}
        self.lock = threading.Lock()
        self._recv_thread = None
        self._running = False

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(30)
        self.sock.connect((POOL_HOST, POOL_PORT))
        self._running = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        log.info(f'[{self.worker_id}] Connected to {POOL_HOST}:{POOL_PORT}')

    def _recv_loop(self):
        buf = ''
        while self._running:
            try:
                data = self.sock.recv(4096).decode('utf-8')
                if not data:
                    break
                buf += data
                while '\n' in buf:
                    line, buf = buf.split('\n', 1)
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        msg = json.loads(line)
                        log.info(f'[{self.worker_id}] << {line[:120]}')
                        self._handle_message(msg)
                    except json.JSONDecodeError:
                        pass
            except socket.timeout:
                continue
            except Exception as e:
                log.error(f'[{self.worker_id}] Recv error: {e}')
                break
        self._running = False

    def _handle_message(self, msg):
        method = msg.get('method')
        if method == 'mining.notify':
            new_job_id = msg['params'][0]
            with nonce_lock:
                if nonce_state['job_id'] != new_job_id:
                    nonce_state['job_id'] = new_job_id
                    nonce_state['current'] = 0
                    log.info(f'[nonce] New job {new_job_id}, reset nonce counter')
            self.job = msg['params']

        elif method == 'mining.set_difficulty':
            self.difficulty = msg['params'][0]
            log.info(f'[{self.worker_id}] Difficulty set to {self.difficulty}')

        msg_id = msg.get('id')
        if msg_id is not None:
            with self.lock:
                self.responses[msg_id] = msg

    def send(self, method, params, msg_id=None):
        if msg_id is None:
            msg_id = int(time.time() * 1000) % 100000
        msg = json.dumps({'id': msg_id, 'method': method, 'params': params}) + '\n'
        self.sock.sendall(msg.encode('utf-8'))
        log.info(f'[{self.worker_id}] >> {msg.strip()}')
        return msg_id

    def wait_response(self, msg_id, timeout=10):
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                if msg_id in self.responses:
                    return self.responses.pop(msg_id)
            time.sleep(0.1)
        raise TimeoutError(f'No response for id={msg_id}')

    def close(self):
        self._running = False
        if self.sock:
            self.sock.close()


def get_or_create_connection(worker_id):
    with connections_lock:
        conn = connections.get(worker_id)
        if conn and conn._running:
            return conn
        conn = StratumConnection(worker_id)
        conn.connect()
        connections[worker_id] = conn
        return conn


@app.route('/health')
def health():
    with connections_lock:
        active = sum(1 for c in connections.values() if c._running)
    return jsonify({
        'status': 'ok',
        'connections': active,
        'pool': f'{POOL_HOST}:{POOL_PORT}',
        'nonce_batch_size': NONCE_BATCH_SIZE,
        'current_job': nonce_state['job_id'],
        'current_nonce': nonce_state['current']
    })


@app.route('/subscribe', methods=['POST'])
def subscribe():
    data = request.json
    worker_id = data.get('worker_id')
    if not worker_id:
        return jsonify({'error': 'worker_id required'}), 400

    try:
        conn = get_or_create_connection(worker_id)
        msg_id = conn.send('mining.subscribe', ['rifaiminer/1.0.0'])
        resp = conn.wait_response(msg_id)

        if resp.get('result'):
            conn.subscribed = True
            conn.extranonce1 = resp['result'][1]
            conn.extranonce2_size = resp['result'][2]

        return jsonify({
            'success': True,
            'extranonce1': conn.extranonce1,
            'extranonce2_size': conn.extranonce2_size
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/authorize', methods=['POST'])
def authorize():
    data = request.json
    worker_id = data.get('worker_id')
    wallet = data.get('wallet_address')
    password = data.get('password', 'x')

    if not worker_id or not wallet:
        return jsonify({'error': 'worker_id and wallet_address required'}), 400

    try:
        conn = get_or_create_connection(worker_id)
        msg_id = conn.send('mining.authorize', [wallet, password])
        resp = conn.wait_response(msg_id)
        conn.authorized = resp.get('result') is True
        return jsonify({'success': conn.authorized})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/get_work', methods=['POST'])
def get_work():
    data = request.json
    worker_id = data.get('worker_id')

    try:
        conn = get_or_create_connection(worker_id)
        if not conn.authorized:
            return jsonify({'error': 'Not authorized'}), 400

        # Wait for job
        for _ in range(50):
            if conn.job:
                break
            time.sleep(0.1)

        if not conn.job:
            return jsonify({'error': 'No job available'}), 500

        # Assign nonce batch
        with nonce_lock:
            start = nonce_state['current']
            end = min(start + NONCE_BATCH_SIZE - 1, nonce_state['max'])
            nonce_state['current'] = end + 1
            if nonce_state['current'] > nonce_state['max']:
                nonce_state['current'] = 0

        log.info(f'[nonce] Assigned {hex(start)}-{hex(end)} to {worker_id}')

        return jsonify({
            'success': True,
            'job': conn.job,
            'difficulty': conn.difficulty,
            'extranonce1': conn.extranonce1,
            'extranonce2_size': conn.extranonce2_size,
            'nonce_start': start,
            'nonce_end': end,
            'batch_size': NONCE_BATCH_SIZE
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/submit', methods=['POST'])
def submit():
    data = request.json
    worker_id = data.get('worker_id')
    job_id = data.get('job_id')
    extranonce2 = data.get('extranonce2', '00000000')
    ntime = data.get('ntime')
    nonce = data.get('nonce')

    try:
        conn = get_or_create_connection(worker_id)
        msg_id = conn.send('mining.submit', [
            f'{conn.extranonce1}',
            job_id,
            extranonce2,
            ntime,
            nonce
        ])
        resp = conn.wait_response(msg_id)
        success = resp.get('result') is True
        log.info(f'[{worker_id}] Share {"ACCEPTED" if success else "REJECTED"}')
        return jsonify({'success': success, 'error': resp.get('error')})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/disconnect', methods=['POST'])
def disconnect():
    data = request.json
    worker_id = data.get('worker_id')
    with connections_lock:
        conn = connections.pop(worker_id, None)
    if conn:
        conn.close()
    return jsonify({'success': True})


if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 3000))
    log.info(f'Starting Stratum HTTP Proxy on port {port}')
    log.info(f'Pool: {POOL_HOST}:{POOL_PORT}')
    app.run(host='0.0.0.0', port=port, threaded=True)
