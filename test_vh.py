#!/usr/bin/env python3
"""
Test VerusHash V2 - compare our JS implementation with reference
"""
import requests
import sys

WORKER_URL = 'https://cord1-rifaiminer.adijayasukabumi.workers.dev'

# Test vector from verushash-node test.js
test_input = b'Test1234' * 12  # 96 bytes
test_hex = test_input.hex()

print(f'Input: "Test1234" x 12 ({len(test_input)} bytes)')
print(f'Hex: {test_hex[:40]}...')
print()

# Get our hash from CF Worker
resp = requests.post(
    f'{WORKER_URL}/hash_raw',
    json={'data_hex': test_hex},
    timeout=30
)

result = resp.json()
our_hash = result.get('hash', 'ERROR')

print(f'Our VerusHash V2: {our_hash}')

# Reverse hex (verushash-node returns reversed)
def reverse_hex(h):
    b = bytes.fromhex(h)
    return b[::-1].hex()

print(f'Reversed:         {reverse_hex(our_hash)}')
print()
print('Expected from verushash-node:')
print('VerusHash2: should match one of the above')
