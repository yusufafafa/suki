#!/usr/bin/env python3
"""
Phone Bridge - VerusCoin Mining
Connects to Luckpool via TCP Stratum
Offloads hashing to Cloudflare Worker
"""

import socket
import json
import time
import struct
import hashlib
import logging
import threading
import requests
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
log = logging.getLogger(__name__)

# Config
POOL_HOST = 'na.luckpool.net'
POOL_PORT = 3956
WALLET    = 'RGobnkLhYLPPxTeFuprGEw8WcdcHNULiSq'
WORKER    = 'cord1'
PASSWORD  = 'x'

# Cloudflare Worker URL
WORKER_URL = os.environ.get('WORKER_URL', 'https://cord1-rifaiminer.adijayasukabumi.workers.dev')

BATCH_SIZE = 56


def hex_to_bytes(h):
    return bytes.fromhex(h)


def bytes_to_hex(b):
    return b.hex()


def build_header(job, extranonce1, extranonce2, nonce=0):
    """Build 80-byte block header from stratum job"""
    # job = [job_id, prevhash, coinb1, coinb2, merkle_branch, version, nbits, ntime, clean]
    version    = job[5]
    prevhash   = job[1]
    coinb1     = job[2]
    coinb2     = job[3]
    merkle_branch = job[4]
    nbits      = job[6]
    ntime      = job[7]

    # Build coinbase
    coinbase = coinb1 + extranonce1 + extranonce2 + coinb2
    coinbase_hash = hashlib.sha256(hashlib.sha256(bytes.fromhex(coinbase)).digest()).digest()

    # Build merkle root
    merkle_root = coinbase_hash
    for branch in merkle_branch:
        merkle_root = hashlib.sha256(
            hashlib.sha256(merkle_root + bytes.fromhex(branch)).digest()
        ).digest()

    # Build 80-byte header
    header = (
        bytes.fromhex(version)[::-1] +   # version LE
        bytes.fromhex(prevhash)[::-1] +   # prevhash LE
        merkle_root[::-1] +               # merkle root LE
        bytes.fromhex(ntime)[::-1] +      # ntime LE
        bytes.fromhex(nbits)[::-1] +      # nbits LE
        struct.pack('<I', nonce)           # nonce LE
    )
    return header


def compute_target(difficulty):
    """Convert difficulty to 32-byte target hex"""
    if not difficulty:
        difficulty = 1
    max_target = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
    target = max_target // int(difficulty)
    return target.to_bytes(32, 'little').hex()


class StratumClient:
    def __init__(self):
        self.sock = None
        self.buf = ''
        self.msg_id = 0
        self.extranonce1 = None
        self.extranonce2_size = 4
        self.difficulty = 1
        self.current_job = None
        self.nonce_counter = 0
        self.nonce_lock = threading.Lock()
        self.running = False

    def connect(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((POOL_HOST, POOL_PORT))
        self.sock.settimeout(60)
        self.running = True
        log.info(f'Connected to {POOL_HOST}:{POOL_PORT}')

    def send(self, method, params):
        self.msg_id += 1
        msg = json.dumps({'id': self.msg_id, 'method': method, 'params': params}) + '\n'
        self.sock.sendall(msg.encode())
        log.info(f'>> {msg.strip()}')
        return self.msg_id

    def recv_line(self):
        while '\n' not in self.buf:
            data = self.sock.recv(4096).decode('utf-8')
            if not data:
                raise ConnectionError('Connection closed')
            self.buf += data
        line, self.buf = self.buf.split('\n', 1)
        return line.strip()

    def recv_response(self):
        while True:
            line = self.recv_line()
            if not line:
                continue
            try:
                msg = json.loads(line)
                log.info(f'<< {line[:150]}')
                return msg
            except json.JSONDecodeError:
                continue

    def subscribe(self):
        self.send('mining.subscribe', ['phone_bridge/1.0.0'])
        resp = self.recv_response()
        result = resp.get('result')
        if result:
            # Luckpool returns [null, extranonce1] or [subscriptions, extranonce1, extranonce2_size]
            if isinstance(result, list) and len(result) >= 2:
                self.extranonce1 = result[1] if result[1] else result[0]
                self.extranonce2_size = result[2] if len(result) > 2 else 4
            log.info(f'Subscribed. extranonce1={self.extranonce1} extranonce2_size={self.extranonce2_size}')

    def authorize(self):
        self.send('mining.authorize', [f'{WALLET}.{WORKER}', PASSWORD])
        resp = self.recv_response()
        if resp.get('result'):
            log.info(f'Authorized as {WALLET}.{WORKER}')
        else:
            log.error(f'Authorization failed: {resp}')

    def get_next_nonce_batch(self, job_id):
        """Get next nonce batch, reset on new job"""
        with self.nonce_lock:
            start = self.nonce_counter
            end = start + BATCH_SIZE - 1
            self.nonce_counter = end + 1
            if self.nonce_counter > 0xFFFFFFFF:
                self.nonce_counter = 0
        return start, end

    def offload_to_worker(self, header_hex, target, nonce_start):
        """Send hash request to Cloudflare Worker"""
        try:
            resp = requests.post(
                f'{WORKER_URL}/hash',
                json={
                    'header_hex': header_hex,
                    'target': target,
                    'nonce_start': nonce_start,
                    'batch_size': BATCH_SIZE
                },
                timeout=30
            )
            return resp.json()
        except Exception as e:
            log.error(f'Worker request failed: {e}')
            return None

    def submit_share(self, job_id, extranonce2, ntime, nonce_hex):
        """Submit winning share to pool"""
        self.send('mining.submit', [
            f'{WALLET}.{WORKER}',
            job_id,
            extranonce2,
            ntime,
            nonce_hex
        ])
        resp = self.recv_response()
        if resp.get('result'):
            log.info(f'[!!!] SHARE ACCEPTED! nonce={nonce_hex}')
        else:
            log.warning(f'Share rejected: {resp}')

    def handle_notify(self, params):
        """Handle new job from pool"""
        job_id = params[0]
        clean = params[8] if len(params) > 8 else False

        if clean or self.current_job is None or self.current_job[0] != job_id:
            log.info(f'[!] NEW JOB: {job_id} (clean={clean})')
            self.current_job = params
            with self.nonce_lock:
                self.nonce_counter = 0  # Reset nonce on new job

    def mine_job(self):
        """Mine current job - offload to Worker"""
        if not self.current_job:
            return

        job = self.current_job
        job_id = job[0]
        ntime = job[7]
        extranonce2 = '00' * self.extranonce2_size

        # Build header
        try:
            header = build_header(job, self.extranonce1, extranonce2)
            header_hex = header.hex()
        except Exception as e:
            log.error(f'Header build failed: {e}')
            return

        # Get target
        target = compute_target(self.difficulty)

        # Get nonce batch
        nonce_start, nonce_end = self.get_next_nonce_batch(job_id)

        log.info(f'Offloading to Worker: job={job_id} nonce={hex(nonce_start)}-{hex(nonce_end)}')

        # Send to Cloudflare Worker
        result = self.offload_to_worker(header_hex, target, nonce_start)

        if result and result.get('found'):
            nonce_hex = result['nonce_hex']
            log.info(f'[!!!] SHARE FOUND! nonce={nonce_hex} hash={result.get("hash", "")[:16]}...')
            self.submit_share(job_id, extranonce2, ntime, nonce_hex)

    def run(self):
        """Main mining loop"""
        self.connect()
        self.subscribe()
        self.authorize()

        log.info('Starting mining loop...')
        log.info(f'Worker URL: {WORKER_URL}')

        while self.running:
            try:
                # Check for pool messages (non-blocking)
                self.sock.settimeout(0.1)
                try:
                    line = self.recv_line()
                    if line:
                        msg = json.loads(line)
                        method = msg.get('method')

                        if method == 'mining.notify':
                            self.handle_notify(msg['params'])

                        elif method == 'mining.set_difficulty':
                            self.difficulty = msg['params'][0]
                            log.info(f'Difficulty: {self.difficulty}')

                except socket.timeout:
                    pass

                # Mine current job
                self.sock.settimeout(60)
                self.mine_job()

            except KeyboardInterrupt:
                log.info('Stopping...')
                self.running = False
            except Exception as e:
                log.error(f'Error: {e}')
                time.sleep(5)
                log.info('Reconnecting...')
                self.connect()
                self.subscribe()
                self.authorize()


if __name__ == '__main__':
    log.info('=== Phone Bridge - VerusCoin Miner ===')
    log.info(f'Wallet: {WALLET}')
    log.info(f'Pool: {POOL_HOST}:{POOL_PORT}')
    log.info(f'Worker: {WORKER_URL}')

    client = StratumClient()
    client.run()
