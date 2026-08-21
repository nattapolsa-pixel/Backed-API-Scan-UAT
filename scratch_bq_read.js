const fs = require('fs');
const crypto = require('crypto');
const https = require('https');

const key = JSON.parse(fs.readFileSync('bq-key.json', 'utf8'));
const b64url = v => Buffer.from(typeof v === 'string' ? v : JSON.stringify(v)).toString('base64url');
const now = Math.floor(Date.now() / 1000);
const head = b64url({ alg: 'RS256', typ: 'JWT' });
const body = b64url({ iss: key.client_email, scope: 'https://www.googleapis.com/auth/bigquery', aud: key.token_uri, iat: now, exp: now + 3600 });
const jwt = `${head}.${body}.${crypto.createSign('RSA-SHA256').update(`${head}.${body}`).end().sign(key.private_key).toString('base64url')}`;
const request = (url, method, data, headers = {}) => new Promise((resolve, reject) => {
  const req = https.request(url, { method, headers: { ...(data ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) } : {}), ...headers } }, res => {
    let text = ''; res.on('data', c => text += c); res.on('end', () => res.statusCode < 300 ? resolve(JSON.parse(text)) : reject(new Error(`${res.statusCode}: ${text}`)));
  }); req.on('error', reject); if (data) req.write(data); req.end();
});

(async () => {
  const token = await request(key.token_uri, 'POST', `grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion=${jwt}`, { 'Content-Type': 'application/x-www-form-urlencoded' });
  const query = process.env.BQ_SQL;
  const result = await request('https://bigquery.googleapis.com/bigquery/v2/projects/pro-analytics-db/queries', 'POST', JSON.stringify({ query, useLegacySql: false }), { Authorization: `Bearer ${token.access_token}` });
  console.log(JSON.stringify(result.rows || [], null, 2));
})().catch(error => { console.error(error.message); process.exit(1); });
