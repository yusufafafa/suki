#!/usr/bin/env python3
"""
Phone Bridge - VerusCoin Mining
Based on ccminer-verus Oink70/Verus2.2 equi-stratum.cpp

Flow:
1. Connect to pool via TCP Stratum
2. Receive job (140-byte header + 1344-byte solution from pool)
3. Offload hash check to Cloudflare Worker
4. Submit valid shares back to pool
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

WORKER_URL = os.environ.get('WORKER_URL', 'https://cord1-rifaiminer.adijayasukabumi.workers.dev')

BATCH_SIZE = 56


def swab32(x):
    """Swap bytes of a 32-bit integer (like ccminer swab32)"""
    return struct.unpack('>I', struct.pack('<I', x & 0xFFFFFFFF))[0]


def le32dec(b, offset=0):
    """Read little-endian uint32 from bytes"""
    return struct.unpack_from('<I', b, offset)[0]


def be32dec(b, offset=0):
    """Read big-endian uint32 from bytes"""
    return struct.unpack_from('>I', b, offset)[0]


def build_header_140(job, extranonce1, extranonce2):
    """
    Build 140-byte VerusCoin block header from stratum job.
    
    Based on ccminer equi_stratum_notify + stratum_gen_work:
    
    Luckpool job format (from equi_stratum_notify):
    params[0] = job_id
    params[1] = version   (8 hex chars = 4 bytes)
    params[2] = prevhash  (64 hex chars = 32 bytes)
    params[3] = coinb1    (merkle, 64 hex chars = 32 bytes)
    params[4] = coinb2    (reserved, 64 hex chars = 32 bytes)
    params[5] = stime     (8 hex chars = 4 bytes, ntime)
    params[6] = nbits     (8 hex chars = 4 bytes)
    params[7] = clean     (bool)
    params[8] = solution  (2688 hex chars = 1344 bytes)
    
    Header layout (140 bytes):
    [0:4]    version (LE)
    [4:36]   prevhash (32 bytes, as-is from pool)
    [36:68]  merkle/coinb1 (32 bytes, as-is)
    [68:100] coinb2/reserved (32 bytes, as-is)
    [100:104] ntime (LE)
    [104:108] nbits (LE)
    [108:140] nonce (32 bytes, extranonce1 + extranonce2 + zeros)
    """
    version  = job[1] if len(job) > 1 and isinstance(job[1], str) else '00000004'
    prevhash = job[2] if len(job) > 2 and isinstance(job[2], str) else '00' * 32
    coinb1   = job[3] if len(job) > 3 and isinstance(job[3], str) else '00' * 32
    coinb2   = job[4] if len(job) > 4 and isinstance(job[4], str) else '00' * 32
    stime    = job[5] if len(job) > 5 and isinstance(job[5], str) else '%08x' % int(time.time())
    nbits    = job[6] if len(job) > 6 and isinstance(job[6], str) else '1e0fffff'

    header = bytearray(140)

    # version (4 bytes LE) - hex2bin then store as-is (pool sends LE already)
    v = bytes.fromhex(version.zfill(8))
    header[0:4] = v

    # prevhash (32 bytes) - hex2bin as-is
    header[4:36] = bytes.fromhex(prevhash.zfill(64))

    # coinb1/merkle (32 bytes) - hex2bin as-is
    header[36:68] = bytes.fromhex(coinb1.zfill(64))

    # coinb2/reserved (32 bytes) - hex2bin as-is
    header[68:100] = bytes.fromhex(coinb2.zfill(64))

    # ntime (4 bytes LE)
    nt = bytes.fromhex(stime.zfill(8))
    header[100:104] = nt

    # nbits (4 bytes LE)
    nb = bytes.fromhex(nbits.zfill(8))
    header[104:108] = nb

    # nonce (32 bytes): extranonce1 (pool prefix) + extranonce2 + zeros
    nonce = bytearray(32)
    en1 = bytes.fromhex(extranonce1) if extranonce1 else b''
    en2 = bytes.fromhex(extranonce2) if extranonce2 else b''
    nonce[:len(en1)] = en1
    nonce[len(en1):len(en1)+len(en2)] = en2
    header[108:140] = nonce

    return bytes(header)


def compute_target_from_diff(difficulty):
    """
    Convert difficulty to 32-byte target.
    Based on diff_to_target_equi from equi-stratum.cpp:
    
    for (k = 6; k > 0 && diff > 1.0; k--)
        diff /= 4294967296.0;
    m = (uint64_t)(4294901760.0 / diff);
    target[k+1] = m >> 8
    target[k+2] = m >> 40
    then fill leading bytes with 0xff
    """
    if not difficulty or difficulty <= 0:
        difficulty = 1.0

    diff = float(difficulty)
    k = 6
    while k > 0 and diff > 1.0:
        diff /= 4294967296.0
        k -= 1

    m = int(4294901760.0 / diff)

    target = bytearray(32)
    if m == 0 and k == 6:
        # fill with 0xff
        for i in range(32):
            target[i] = 0xff
    else:
        # target[k+1] = m >> 8 (lower 32 bits of m>>8)
        struct.pack_into('<I', target, (k + 1) * 4, (m >> 8) & 0xFFFFFFFF)
        # target[k+2] = m >> 40 (lower 32 bits of m>>40)
        if k + 2 < 8:
            struct.pack_into('<I', target, (k + 2) * 4, (m >> 40) & 0xFFFFFFFF)
        # fill leading bytes with 0xff
        i = 0
        while i < 28 and target[i] == 0:
            target[i] = 0xff
            i += 1

    return bytes(target).hex()


class StratumClient:
    def __init__(self):
        self.sock = None
        self.buf = ''
        self.msg_id = 0
        self.extranonce1 = ''
        self.extranonce1_size = 0
        self.extranonce2_size = 4
        self.difficulty = 1.0
        self.target_hex = None
        self.current_job = None
        self.nonce_counter = 0
        self.nonce_lock = threading.Lock()
        self.running = False
        self.stats = {
            'jobs': 0,
            'submitted': 0,
            'accepted': 0,
            'rejected': 0,
            'hashes': 0,
            'start_time': time.time()
        }

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
                log.info(f'<< {line[:200]}')
                return msg
            except json.JSONDecodeError:
                continue

    def subscribe(self):
        self.send('mining.subscribe', ['phone_bridge/1.0.0'])
        resp = self.recv_response()
        result = resp.get('result')
        if result and isinstance(result, list) and len(result) >= 2:
            self.extranonce1 = result[1] if result[1] else ''
            self.extranonce1_size = len(bytes.fromhex(self.extranonce1)) if self.extranonce1 else 0
            self.extranonce2_size = result[2] if len(result) > 2 and result[2] else 4
            log.info(f'Subscribed. extranonce1={self.extranonce1} extranonce2_size={self.extranonce2_size}')

    def authorize(self):
        self.send('mining.authorize', [f'{WALLET}.{WORKER}', PASSWORD])
        resp = self.recv_response()
        if resp.get('result'):
            log.info(f'Authorized as {WALLET}.{WORKER}')
        else:
            log.error(f'Authorization failed: {resp}')

    def handle_notify(self, params):
        """Handle mining.notify - store job"""
        job_id = params[0]
        clean = params[7] if len(params) > 7 and isinstance(params[7], bool) else False

        if clean or self.current_job is None or self.current_job[0] != job_id:
            self.stats['jobs'] += 1
            print(f'\nNEW JOB RECEIVED: Job ID {job_id}')
            self.current_job = params
            with self.nonce_lock:
                self.nonce_counter = 0

    def handle_set_target(self, params):
        """Handle mining.set_target - store target directly from pool"""
        target_hex = params[0] if params else None
        if target_hex:
            self.target_hex = target_hex
            log.info(f'Target set: {target_hex[:16]}...')

    def get_nonce_batch(self):
        """Get next nonce batch"""
        with self.nonce_lock:
            start = self.nonce_counter
            end = start + BATCH_SIZE - 1
            self.nonce_counter = end + 1
            if self.nonce_counter > 0xFFFFFFFF:
                self.nonce_counter = 0
        return start, end

    def offload_to_worker(self, header_hex, solution_hex, target_hex, nonce_start):
        """Send hash request to Cloudflare Worker"""
        try:
            # Full data = header (140 bytes) + sol_size (3 bytes fd4005) + solution (1344 bytes)
            sol_size_header = 'fd4005'
            full_data_hex = header_hex + sol_size_header + solution_hex

            resp = requests.post(
                f'{WORKER_URL}/hash',
                json={
                    'header_hex': full_data_hex,
                    'target': target_hex,
                    'nonce_start': nonce_start,
                    'batch_size': BATCH_SIZE,
                    'nonce_offset': 108 * 2  # nonce at byte 108 in header
                },
                timeout=30
            )
            if resp.status_code != 200:
                print(f'[ERROR] Worker HTTP {resp.status_code}: {resp.text[:100]}')
                return None
            return resp.json()
        except requests.exceptions.JSONDecodeError:
            print(f'[ERROR] Worker JSON parse failed')
            return None
        except Exception as e:
            print(f'[ERROR] Worker request failed: {e}')
            return None

    def submit_share(self, job_id, ntime_hex, nonce_hex, sol_hex):
        """
        Submit share to pool.
        
        From equi_stratum_submit:
        params = [user, job_id, timehex, noncestr, solhex]
        - timehex = swab32(ntime) = ntime bytes reversed
        - noncestr = nonce bytes AFTER extranonce1 prefix
        - solhex = 1347 bytes (3 bytes size + 1344 bytes solution)
        """
        self.stats['submitted'] += 1

        # timehex = swab32(ntime) - reverse the 4 bytes
        try:
            ntime_int = int(ntime_hex, 16)
            timehex = '%08x' % swab32(ntime_int)
        except Exception:
            timehex = ntime_hex

        # noncestr = nonce without extranonce1 prefix
        # nonce is 32 bytes, extranonce1 is first N bytes
        # noncestr = remaining bytes after extranonce1
        nonce_bytes = bytes.fromhex(nonce_hex.zfill(64))  # 32 bytes
        noncestr = nonce_bytes[self.extranonce1_size:].hex()

        # job_id: ccminer uses job_id + 8 chars offset, but we use full job_id
        # Some pools want short job_id, try both
        short_job_id = job_id[8:] if len(job_id) > 8 else job_id

        self.send('mining.submit', [
            f'{WALLET}.{WORKER}',
            short_job_id,
            timehex,
            noncestr,
            sol_hex
        ])
        resp = self.recv_response()
        if resp.get('result'):
            self.stats['accepted'] += 1
            print(f'[!!!] SHARE ACCEPTED! nonce={nonce_hex}')
        else:
            self.stats['rejected'] += 1
            err = resp.get('error', 'unknown')
            print(f'Share rejected: {err}')
            # Try with full job_id if short failed
            if 'job' in str(err).lower() or 'stale' in str(err).lower():
                log.info('Retrying with full job_id...')
                self.send('mining.submit', [
                    f'{WALLET}.{WORKER}',
                    job_id,
                    timehex,
                    noncestr,
                    sol_hex
                ])
                resp2 = self.recv_response()
                if resp2.get('result'):
                    self.stats['accepted'] += 1
                    self.stats['rejected'] -= 1
                    print(f'[!!!] SHARE ACCEPTED (retry)! nonce={nonce_hex}')

    def mine_job(self):
        """Mine current job"""
        if not self.current_job:
            return

        job = self.current_job
        job_id  = job[0]
        ntime   = job[5] if len(job) > 5 and isinstance(job[5], str) else '%08x' % int(time.time())
        solution = job[8] if len(job) > 8 and isinstance(job[8], str) else None

        if not solution or len(solution) != 2688:
            # No valid solution from pool, skip
            return

        # Build 140-byte header
        extranonce2 = '00' * self.extranonce2_size
        try:
            header = build_header_140(job, self.extranonce1, extranonce2)
            header_hex = header.hex()
        except Exception as e:
            log.error(f'Header build failed: {e}')
            return

        # Get target
        if self.target_hex:
            target = self.target_hex
        else:
            target = compute_target_from_diff(self.difficulty)

        # Get nonce batch
        nonce_start, _ = self.get_nonce_batch()

        print(f'Sending nonce range to Cloudflare Worker...')

        # Offload to Worker
        result = self.offload_to_worker(header_hex, solution, target, nonce_start)
        self.stats['hashes'] += BATCH_SIZE

        elapsed = time.time() - self.stats['start_time']
        hashrate = self.stats['hashes'] / elapsed if elapsed > 0 else 0

        if result and result.get('found'):
            nonce_hex = result['nonce_hex']
            print(f'[!!!] SHARE FOUND! nonce={nonce_hex}')
            # sol_hex = size_header + solution
            sol_hex = 'fd4005' + solution
            self.submit_share(job_id, ntime, nonce_hex, sol_hex)
        else:
            s = self.stats
            print(f'Checked {BATCH_SIZE} hashes. No share found. [{hashrate:.2f} H/s | jobs={s["jobs"]} | submitted={s["submitted"]} | accepted={s["accepted"]} | rejected={s["rejected"]}]')

    def run(self):
        self.connect()
        self.subscribe()
        self.authorize()

        log.info('Starting mining loop...')
        log.info(f'Worker URL: {WORKER_URL}')

        while self.running:
            try:
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
                        elif method == 'mining.set_target':
                            self.handle_set_target(msg['params'])
                except socket.timeout:
                    pass

                self.sock.settimeout(60)
                self.mine_job()

            except KeyboardInterrupt:
                log.info('Stopping...')
                self.running = False
            except Exception as e:
                log.error(f'Error: {e}')
                time.sleep(5)
                log.info('Reconnecting...')
                try:
                    self.connect()
                    self.subscribe()
                    self.authorize()
                except Exception as e2:
                    log.error(f'Reconnect failed: {e2}')


if __name__ == '__main__':
    log.info('=== Phone Bridge - VerusCoin Miner ===')
    log.info(f'Wallet: {WALLET}')
    log.info(f'Pool: {POOL_HOST}:{POOL_PORT}')
    log.info(f'Worker: {WORKER_URL}')

    client = StratumClient()
    client.run()
