// Satu "room" publik untuk chat, music room, dan statistik user.
export class ChatRoom {
  constructor(state, env) {
    this.state = state;
    this.ready = state.blockConcurrencyWhile(async () => {
      this.startedAt = (await state.storage.get('startedAt')) ?? null;
      if (!this.startedAt) {
        this.startedAt = Date.now();
        await state.storage.put('startedAt', this.startedAt);
      }
      this.total = (await state.storage.get('total')) ?? 0;
      this.history = (await state.storage.get('history')) ?? [];
      this.music = (await state.storage.get('music')) ?? null;
    });
  }

  stats(exclude = null) {
    const sockets = this.state.getWebSockets().filter(ws => ws !== exclude);
    const online = sockets.length;
    return {
      total: this.total,
      online,
      offline: Math.max(0, this.total - online),
      startedAt: this.startedAt,
    };
  }

  broadcast(obj, exclude = null) {
    const msg = JSON.stringify(obj);
    for (const ws of this.state.getWebSockets()) {
      if (ws === exclude) continue;
      try { ws.send(msg); } catch {}
    }
  }

  async fetch(request) {
    await this.ready;
    const url = new URL(request.url);

    if (url.pathname.endsWith('/stats')) {
      return Response.json(this.stats());
    }

    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ name: 'Tamu', vid: null, last: 0, lastMusic: 0 });

    server.send(JSON.stringify({
      type: 'init',
      history: this.history,
      music: this.music,
      stats: this.stats(),
    }));
    this.broadcast({ type: 'stats', stats: this.stats() });

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, raw) {
    await this.ready;
    let data;
    try { data = JSON.parse(raw); } catch { return; }

    const att = ws.deserializeAttachment() || { name: 'Tamu', vid: null, last: 0, lastMusic: 0 };
    const now = Date.now();

    if (data.type === 'join') {
      att.name = clean(data.name, 20) || 'Tamu';
      const vid = clean(data.vid, 64);
      if (vid && !att.vid) {
        att.vid = vid;
        const key = 'v:' + vid;
        if (!(await this.state.storage.get(key))) {
          await this.state.storage.put(key, 1);
          this.total++;
          await this.state.storage.put('total', this.total);
        }
      }
      ws.serializeAttachment(att);
      this.broadcast({ type: 'stats', stats: this.stats() });
      return;
    }

    if (data.type === 'chat') {
      const text = clean(data.text, 300);
      if (!text) return;
      if (now - att.last < 800) {
        ws.send(JSON.stringify({ type: 'error', text: 'Pelan-pelan ya, tunggu sebentar.' }));
        return;
      }
      att.last = now;
      ws.serializeAttachment(att);

      const msg = { type: 'chat', name: att.name, text, t: now };
      this.history.push(msg);
      if (this.history.length > 50) this.history.shift();
      await this.state.storage.put('history', this.history);
      this.broadcast(msg);
      return;
    }

    if (data.type === 'music') {
      if (now - att.lastMusic < 5000) {
        ws.send(JSON.stringify({ type: 'error', text: 'Tunggu 5 detik sebelum ganti lagu.' }));
        return;
      }
      const url = clean(data.url, 500);
      if (!/^https:\/\/\S+$/.test(url)) {
        ws.send(JSON.stringify({ type: 'error', text: 'Link harus diawali https://' }));
        return;
      }
      att.lastMusic = now;
      ws.serializeAttachment(att);

      this.music = {
        url,
        title: clean(data.title, 60) || 'Lagu tanpa judul',
        by: att.name,
        startedAt: now,
      };
      await this.state.storage.put('music', this.music);
      this.broadcast({ type: 'music', music: this.music });
    }
  }

  async webSocketClose(ws) {
    this.broadcast({ type: 'stats', stats: this.stats(ws) }, ws);
  }

  async webSocketError(ws) {
    this.broadcast({ type: 'stats', stats: this.stats(ws) }, ws);
  }
}

function clean(v, max) {
  return typeof v === 'string'
    ? v.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, max)
    : '';
}

const json = (obj, status = 200) =>
  new Response(JSON.stringify(obj), { status, headers: { 'Content-Type': 'application/json' } });

// Hostname privat/lokal ditolak supaya proxy tidak bisa dipakai menembak jaringan internal.
function isPrivateHost(host) {
  const h = host.toLowerCase();
  if (h === 'localhost' || h.endsWith('.local') || h.endsWith('.internal')) return true;
  const m = h.match(/^(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  if (m) {
    const [a, b] = [+m[1], +m[2]];
    return a === 10 || a === 127 || a === 0 ||
      (a === 169 && b === 254) || (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) || a >= 224;
  }
  return h.includes(':'); // IPv6 literal ditolak
}

// POST /api/panel/servers  { panelUrl, apiKey }
// Key hanya diteruskan ke panel, tidak disimpan.
async function handlePanelServers(request) {
  if (request.method !== 'POST') return json({ error: 'Gunakan POST' }, 405);
  let body;
  try { body = await request.json(); } catch { return json({ error: 'Body harus JSON' }, 400); }

  const panelUrl = String(body.panelUrl || '').trim().replace(/\/+$/, '');
  const apiKey = String(body.apiKey || '').trim();

  let parsed;
  try { parsed = new URL(panelUrl); } catch { return json({ error: 'URL panel tidak valid' }, 400); }
  if (parsed.protocol !== 'https:' || parsed.pathname !== '/' || parsed.search || parsed.hash) {
    return json({ error: 'URL panel harus https:// tanpa path tambahan' }, 400);
  }
  if (isPrivateHost(parsed.hostname)) return json({ error: 'Host ini tidak diizinkan' }, 400);
  if (!/^ptla_[A-Za-z0-9]{8,}$/.test(apiKey)) {
    return json({ error: 'Harus Application API key (diawali ptla_)' }, 400);
  }

  let res;
  try {
    res = await fetch(`${parsed.origin}/api/application/servers?per_page=100`, {
      headers: { Authorization: `Bearer ${apiKey}`, Accept: 'application/json' },
      redirect: 'manual',
    });
  } catch {
    return json({ error: 'Tidak bisa terhubung ke panel' }, 502);
  }
  if (res.status === 401 || res.status === 403) return json({ error: 'API key ditolak panel' }, res.status);
  if (!res.ok) return json({ error: `Panel membalas status ${res.status}` }, 502);

  const data = await res.json().catch(() => null);
  const servers = (data?.data || []).map(s => ({
    id: s.attributes.id,
    name: s.attributes.name,
    identifier: s.attributes.identifier,
    suspended: s.attributes.suspended,
    node: s.attributes.node,
  }));
  return json({ servers });
}

// POST /api/subdomain  header X-Owner-Key  body { label, ip }
// Khusus owner: membuat A record di zone Cloudflare milik sendiri.
async function handleSubdomain(request, env) {
  if (request.method !== 'POST') return json({ error: 'Gunakan POST' }, 405);
  const ownerKey = request.headers.get('X-Owner-Key') || '';
  if (!env.OWNER_KEY || ownerKey !== env.OWNER_KEY) return json({ error: 'Tidak diizinkan' }, 403);

  let body;
  try { body = await request.json(); } catch { return json({ error: 'Body harus JSON' }, 400); }
  const label = String(body.label || '').trim().toLowerCase();
  const ip = String(body.ip || '').trim();

  if (!/^[a-z0-9](?:[a-z0-9-]{1,28}[a-z0-9])$/.test(label)) {
    return json({ error: 'Label 3-30 karakter: huruf kecil, angka, dan tanda -' }, 400);
  }
  const ipv4 = ip.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (!ipv4 || ipv4.slice(1).some(n => +n > 255)) return json({ error: 'IP tidak valid' }, 400);

  const name = `${label}.${env.ROOT_DOMAIN}`;
  const res = await fetch(`https://api.cloudflare.com/client/v4/zones/${env.CF_ZONE_ID}/dns_records`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${env.CF_API_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'A', name, content: ip, ttl: 1, proxied: false }),
  });
  const data = await res.json().catch(() => ({ success: false }));
  if (!data.success) {
    return json({ error: 'Cloudflare menolak permintaan', details: (data.errors || []).map(e => e.message) }, 502);
  }
  return json({ ok: true, name });
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname === '/api/ws' || url.pathname === '/api/stats') {
      const stub = env.ROOM.get(env.ROOM.idFromName('public'));
      return stub.fetch(request);
    }
    if (url.pathname === '/api/panel/servers') return handlePanelServers(request);
    if (url.pathname === '/api/subdomain') return handleSubdomain(request, env);

    return env.ASSETS.fetch(request);
  },
};
