# Stratum HTTP Proxy

Converts Stratum (TCP) mining protocol to HTTP REST API for Cloudflare Workers.

## Architecture

```
Cloudflare Worker (HTTP) → Proxy (Railway/Render) → Mining Pool (Stratum TCP)
```

## Deploy to Railway

### 1. Install Railway CLI

```bash
npm install -g @railway/cli
```

### 2. Login

```bash
railway login
```

### 3. Deploy

```bash
cd stratum-proxy
railway init
railway up
```

### 4. Set Environment Variables

```bash
railway variables set POOL_HOST=na.luckpool.net
railway variables set POOL_PORT=3956
```

### 5. Get URL

```bash
railway domain
```

You'll get URL like: `https://stratum-proxy-production.up.railway.app`

---

## API Endpoints

### Health Check

```bash
GET /health
```

Response:
```json
{
  "status": "ok",
  "connections": 3,
  "pool": "na.luckpool.net:3956"
}
```

### Subscribe

```bash
POST /subscribe
Content-Type: application/json

{
  "worker_id": "worker1"
}
```

Response:
```json
{
  "success": true,
  "extranonce1": "00000001",
  "extranonce2_size": 4
}
```

### Authorize

```bash
POST /authorize
Content-Type: application/json

{
  "worker_id": "worker1",
  "wallet_address": "RYourVerusAddress...",
  "password": "x"
}
```

Response:
```json
{
  "success": true,
  "message": "Authorized"
}
```

### Get Work

```bash
POST /get_work
Content-Type: application/json

{
  "worker_id": "worker1"
}
```

Response:
```json
{
  "success": true,
  "job": [...],
  "difficulty": 16384,
  "extranonce1": "00000001",
  "extranonce2_size": 4
}
```

### Submit Share

```bash
POST /submit
Content-Type: application/json

{
  "worker_id": "worker1",
  "job_id": "abc123",
  "extranonce2": "00000000",
  "ntime": "507c0917",
  "nonce": "94d90000"
}
```

Response:
```json
{
  "success": true,
  "error": null
}
```

### Disconnect

```bash
POST /disconnect
Content-Type: application/json

{
  "worker_id": "worker1"
}
```

---

## Usage from Cloudflare Worker

```typescript
const PROXY_URL = 'https://stratum-proxy-production.up.railway.app';
const WORKER_ID = 'cord1-rifaiminer';
const WALLET = 'RYourVerusAddress...';

// 1. Subscribe
await fetch(`${PROXY_URL}/subscribe`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ worker_id: WORKER_ID })
});

// 2. Authorize
await fetch(`${PROXY_URL}/authorize`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    worker_id: WORKER_ID,
    wallet_address: WALLET,
    password: 'x'
  })
});

// 3. Get work
const workRes = await fetch(`${PROXY_URL}/get_work`, {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({ worker_id: WORKER_ID })
});
const work = await workRes.json();

// 4. Mine (hash the work)
const solution = await mineBlock(work.job);

// 5. Submit
if (solution) {
  await fetch(`${PROXY_URL}/submit`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      worker_id: WORKER_ID,
      job_id: work.job[0],
      extranonce2: solution.extranonce2,
      ntime: solution.ntime,
      nonce: solution.nonce
    })
  });
}
```

---

## Local Testing

```bash
cd stratum-proxy
npm install
npm start
```

Test:
```bash
curl http://localhost:3000/health
```

---

## Railway Free Tier

- **500 hours/month** execution time
- **100GB bandwidth**
- **512MB RAM**
- **1GB storage**

Perfect for proxy service!

---

## Troubleshooting

### "Connection refused"

Check pool host/port:
```bash
railway logs
```

### "Response timeout"

Pool might be slow. Increase timeout in code.

### "Too many connections"

Railway free tier limits connections. Deploy multiple proxies or upgrade.

---

## Next Steps

1. Deploy proxy to Railway
2. Get proxy URL
3. Update Worker code to use proxy URL
4. Deploy Worker to Cloudflare
5. Start mining!
