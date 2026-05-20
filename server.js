const express = require('express');
const cors = require('cors');
const net = require('net');

const app = express();
const PORT = process.env.PORT || 3000;

// Pool configuration
const POOL_HOST = process.env.POOL_HOST || 'na.luckpool.net';
const POOL_PORT = process.env.POOL_PORT || 3956;

// Middleware
app.use(cors());
app.use(express.json());

// Active connections storage
const connections = new Map();

// Create Stratum connection
function createStratumConnection(workerId) {
  return new Promise((resolve, reject) => {
    const client = new net.Socket();
    const connection = {
      socket: client,
      subscribed: false,
      authorized: false,
      extranonce1: null,
      extranonce2_size: null,
      difficulty: null,
      job: null,
      messageQueue: []
    };

    client.connect(POOL_PORT, POOL_HOST, () => {
      console.log(`[${workerId}] Connected to ${POOL_HOST}:${POOL_PORT}`);
      connections.set(workerId, connection);
      resolve(connection);
    });

    client.on('data', (data) => {
      const messages = data.toString().split('\n').filter(m => m.trim());
      
      messages.forEach(msg => {
        try {
          const json = JSON.parse(msg);
          console.log(`[${workerId}] Received:`, json);
          
          // Handle mining.notify
          if (json.method === 'mining.notify') {
            connection.job = json.params;
          }
          
          // Handle mining.set_difficulty
          if (json.method === 'mining.set_difficulty') {
            connection.difficulty = json.params[0];
          }
          
          // Handle responses
          if (json.id !== undefined) {
            connection.messageQueue.push(json);
          }
        } catch (e) {
          console.error(`[${workerId}] Parse error:`, e.message);
        }
      });
    });

    client.on('error', (err) => {
      console.error(`[${workerId}] Socket error:`, err.message);
      connections.delete(workerId);
      reject(err);
    });

    client.on('close', () => {
      console.log(`[${workerId}] Connection closed`);
      connections.delete(workerId);
    });
  });
}

// Send JSON-RPC message
function sendMessage(connection, method, params, id = Date.now()) {
  const message = JSON.stringify({ id, method, params }) + '\n';
  connection.socket.write(message);
  console.log('Sent:', message.trim());
  return id;
}

// Wait for response
function waitForResponse(connection, id, timeout = 5000) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    
    const checkResponse = setInterval(() => {
      const response = connection.messageQueue.find(msg => msg.id === id);
      
      if (response) {
        connection.messageQueue = connection.messageQueue.filter(msg => msg.id !== id);
        clearInterval(checkResponse);
        resolve(response);
      }
      
      if (Date.now() - startTime > timeout) {
        clearInterval(checkResponse);
        reject(new Error('Response timeout'));
      }
    }, 100);
  });
}

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    connections: connections.size,
    pool: `${POOL_HOST}:${POOL_PORT}`
  });
});

// Subscribe to mining
app.post('/subscribe', async (req, res) => {
  try {
    const { worker_id } = req.body;
    
    if (!worker_id) {
      return res.status(400).json({ error: 'worker_id required' });
    }

    // Create or get connection
    let connection = connections.get(worker_id);
    if (!connection) {
      connection = await createStratumConnection(worker_id);
    }

    // Subscribe
    const subId = sendMessage(connection, 'mining.subscribe', ['miner/1.0.0']);
    const subResponse = await waitForResponse(connection, subId);

    if (subResponse.result) {
      connection.subscribed = true;
      connection.extranonce1 = subResponse.result[1];
      connection.extranonce2_size = subResponse.result[2];
    }

    res.json({
      success: true,
      extranonce1: connection.extranonce1,
      extranonce2_size: connection.extranonce2_size
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Authorize worker
app.post('/authorize', async (req, res) => {
  try {
    const { worker_id, wallet_address, password = 'x' } = req.body;
    
    if (!worker_id || !wallet_address) {
      return res.status(400).json({ error: 'worker_id and wallet_address required' });
    }

    const connection = connections.get(worker_id);
    if (!connection) {
      return res.status(400).json({ error: 'Not subscribed. Call /subscribe first' });
    }

    // Authorize
    const authId = sendMessage(connection, 'mining.authorize', [wallet_address, password]);
    const authResponse = await waitForResponse(connection, authId);

    connection.authorized = authResponse.result === true;

    res.json({
      success: connection.authorized,
      message: connection.authorized ? 'Authorized' : 'Authorization failed'
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Get work
app.post('/get_work', async (req, res) => {
  try {
    const { worker_id } = req.body;
    
    const connection = connections.get(worker_id);
    if (!connection) {
      return res.status(400).json({ error: 'Not connected' });
    }

    if (!connection.authorized) {
      return res.status(400).json({ error: 'Not authorized' });
    }

    // Wait for job
    let attempts = 0;
    while (!connection.job && attempts < 50) {
      await new Promise(resolve => setTimeout(resolve, 100));
      attempts++;
    }

    if (!connection.job) {
      return res.status(500).json({ error: 'No job available' });
    }

    res.json({
      success: true,
      job: connection.job,
      difficulty: connection.difficulty,
      extranonce1: connection.extranonce1,
      extranonce2_size: connection.extranonce2_size
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Submit share
app.post('/submit', async (req, res) => {
  try {
    const { worker_id, job_id, extranonce2, ntime, nonce } = req.body;
    
    const connection = connections.get(worker_id);
    if (!connection) {
      return res.status(400).json({ error: 'Not connected' });
    }

    // Submit
    const submitId = sendMessage(connection, 'mining.submit', [
      connection.extranonce1, // worker name (using extranonce1 as identifier)
      job_id,
      extranonce2,
      ntime,
      nonce
    ]);

    const submitResponse = await waitForResponse(connection, submitId);

    res.json({
      success: submitResponse.result === true,
      error: submitResponse.error || null
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Disconnect
app.post('/disconnect', (req, res) => {
  const { worker_id } = req.body;
  
  const connection = connections.get(worker_id);
  if (connection) {
    connection.socket.destroy();
    connections.delete(worker_id);
  }

  res.json({ success: true });
});

// Start server
app.listen(PORT, () => {
  console.log(`Stratum HTTP Proxy running on port ${PORT}`);
  console.log(`Pool: ${POOL_HOST}:${POOL_PORT}`);
});
