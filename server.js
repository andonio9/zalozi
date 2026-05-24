require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const axios = require('axios');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const FormData = require('form-data');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Upload storage
const upload = multer({ 
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 }
});

// Database
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// Init DB
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS clients (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      color TEXT DEFAULT '#3b82f6',
      created_at TIMESTAMP DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS slips (
      id SERIAL PRIMARY KEY,
      client_id INTEGER REFERENCES clients(id),
      date DATE DEFAULT CURRENT_DATE,
      stake NUMERIC(10,2),
      total_odds NUMERIC(10,2),
      potential_win NUMERIC(10,2),
      result TEXT DEFAULT 'PENDING',
      our_amount NUMERIC(10,2) DEFAULT 0,
      is_profit BOOLEAN DEFAULT false,
      matches JSONB,
      image_path TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
    
    INSERT INTO clients (name, color) 
    VALUES 
      ('Румен', '#3b82f6'),
      ('Славчо', '#f59e0b'),
      ('Близнаците', '#f97316'),
      ('Доги', '#ef4444')
    ON CONFLICT DO NOTHING;
  `);
  console.log('✅ Database ready');
}

// WebSocket broadcast
function broadcast(data) {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

// ══════════════════════════════════════
// SPORTS APIs
// ══════════════════════════════════════

async function fetchNHL(date) {
  try {
    const r = await axios.get(`https://api-web.nhle.com/v1/score/${date}`, { timeout: 5000 });
    return r.data.games || [];
  } catch(e) { return []; }
}

async function fetchMLB(date) {
  try {
    const r = await axios.get(`https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${date}&hydrate=linescore`, { timeout: 5000 });
    return r.data.dates?.[0]?.games || [];
  } catch(e) { return []; }
}

async function fetchESPN(sport, league, date) {
  try {
    const d = date.replace(/-/g, '');
    const r = await axios.get(`https://site.api.espn.com/apis/site/v2/sports/${sport}/${league}/scoreboard?dates=${d}`, { timeout: 5000 });
    return r.data.events || [];
  } catch(e) { return []; }
}

async function fetchApiFootball(league, season, date) {
  try {
    const r = await axios.get('https://v3.football.api-sports.io/fixtures', {
      headers: { 'x-apisports-key': process.env.SPORTS_API_KEY },
      params: { league, season, date },
      timeout: 5000
    });
    return r.data.response || [];
  } catch(e) { return []; }
}

function norm(s) { return (s || '').toLowerCase().replace(/[^a-z]/g, ''); }

function resolveNHL(games, a1, a2, pick) {
  const u1 = (a1 || '').toUpperCase();
  const u2 = (a2 || '').toUpperCase();
  for (const g of games) {
    const home = g.homeTeam?.abbrev?.toUpperCase() || '';
    const away = g.awayTeam?.abbrev?.toUpperCase() || '';
    if (!((home === u1 || away === u1) && (home === u2 || away === u2))) continue;
    const st = g.gameState || '';
    if (st === 'FUT' || st === 'PRE') return { result: 'PENDING', score: '-' };
    const hs = Number(g.homeTeam?.score || 0);
    const as = Number(g.awayTeam?.score || 0);
    const t1Away = away === u1;
    const sc = (t1Away ? as : hs) + ':' + (t1Away ? hs : as);
    if (st === 'LIVE' || st === 'CRIT') return { result: 'LIVE', score: sc };
    const t1Won = t1Away ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveMLB(games, t1, t2, pick) {
  const n1 = norm(t1).slice(-5);
  const n2 = norm(t2).slice(-5);
  for (const g of games) {
    const aN = norm(g.teams?.away?.team?.name || '');
    const hN = norm(g.teams?.home?.team?.name || '');
    const m1 = aN.includes(n1) || n1.includes(aN.slice(-4));
    const m2 = hN.includes(n2) || n2.includes(hN.slice(-4));
    const m3 = aN.includes(n2) || n2.includes(aN.slice(-4));
    const m4 = hN.includes(n1) || n1.includes(hN.slice(-4));
    if (!((m1 && m2) || (m3 && m4))) continue;
    const st = g.status?.abstractGameState || '';
    if (st === 'Preview') return { result: 'PENDING', score: '-' };
    let as = Number(g.teams?.away?.score || 0);
    let hs = Number(g.teams?.home?.score || 0);
    const ls = g.linescore;
    if (!as && ls?.teams?.away) as = Number(ls.teams.away.runs || 0);
    if (!hs && ls?.teams?.home) hs = Number(ls.teams.home.runs || 0);
    const t1Away = m1;
    const sc = (t1Away ? as : hs) + ':' + (t1Away ? hs : as);
    if (st === 'Live') return { result: 'LIVE', score: sc };
    const t1Won = t1Away ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveESPN(events, t1, t2, pick) {
  const n1 = norm(t1).slice(-4);
  const n2 = norm(t2).slice(-4);
  for (const ev of events) {
    const comp = ev.competitions?.[0];
    if (!comp) continue;
    const cs = comp.competitors || [];
    const names = cs.map(c => norm(c.team?.displayName || ''));
    if (!names.some(n => n.includes(n1) || n1.includes(n.slice(-4)))) continue;
    if (!names.some(n => n.includes(n2) || n2.includes(n.slice(-4)))) continue;
    const st = comp.status?.type;
    if (!st?.completed && st?.state !== 'in') return { result: 'PENDING', score: '-' };
    if (st?.state === 'in') {
      const sc = cs.map(c => c.score || '0').join(':');
      return { result: 'LIVE', score: sc };
    }
    const winner = cs.find(c => c.winner);
    if (!winner) return { result: 'PENDING', score: '?' };
    const wn = norm(winner.team?.displayName || '');
    const s1 = cs.find(c => { const nm = norm(c.team?.displayName || ''); return nm.includes(n1) || n1.includes(nm.slice(-4)); })?.score || '?';
    const s2 = cs.find(c => { const nm = norm(c.team?.displayName || ''); return nm.includes(n2) || n2.includes(nm.slice(-4)); })?.score || '?';
    const t1Won = wn.includes(n1) || n1.includes(wn.slice(-4));
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: `${s1}:${s2}` };
  }
  return null;
}

async function resolveMatches(matches) {
  const dates = [];
  for (let i = -5; i <= 1; i++) {
    const d = new Date(Date.now() + i * 86400000);
    dates.push(d.toISOString().split('T')[0]);
  }

  const cache = {};
  const sports = {};
  matches.forEach(m => sports[m.sport] = true);

  for (const dt of dates) {
    if (sports['NHL'] && !cache['NHL_' + dt]) cache['NHL_' + dt] = await fetchNHL(dt);
    if (sports['MLB'] && !cache['MLB_' + dt]) cache['MLB_' + dt] = await fetchMLB(dt);
    if (sports['NBA'] && !cache['NBA_' + dt]) cache['NBA_' + dt] = await fetchESPN('basketball', 'nba', dt);
    if (sports['NFL'] && !cache['NFL_' + dt]) cache['NFL_' + dt] = await fetchESPN('football', 'nfl', dt);
    if (sports['SOCCER']) {
      const leagues = [39, 140, 78, 135, 61, 2, 3]; // EPL, LaLiga, Bundesliga, SerieA, Ligue1, UCL, UEFA
      for (const lg of leagues) {
        const key = `SOCCER_${lg}_${dt}`;
        if (!cache[key]) cache[key] = await fetchApiFootball(lg, 2025, dt);
      }
    }
  }

  return matches.map(m => {
    for (const dt of dates) {
      let r = null;
      if (m.sport === 'NHL') r = resolveNHL(cache['NHL_' + dt] || [], m.team1abbr, m.team2abbr, m.pick);
      else if (m.sport === 'MLB') r = resolveMLB(cache['MLB_' + dt] || [], m.team1, m.team2, m.pick);
      else if (m.sport === 'NBA') r = resolveESPN(cache['NBA_' + dt] || [], m.team1, m.team2, m.pick);
      else if (m.sport === 'NFL') r = resolveESPN(cache['NFL_' + dt] || [], m.team1, m.team2, m.pick);
      else if (m.sport === 'SOCCER') {
        const leagues = [39, 140, 78, 135, 61, 2, 3];
        for (const lg of leagues) {
          const fixtures = cache[`SOCCER_${lg}_${dt}`] || [];
          for (const fx of fixtures) {
            const ht = norm(fx.teams?.home?.name || '');
            const at = norm(fx.teams?.away?.name || '');
            const n1 = norm(m.team1).slice(-4);
            const n2 = norm(m.team2).slice(-4);
            if ((ht.includes(n1) || n1.includes(ht.slice(-4))) && (at.includes(n2) || n2.includes(at.slice(-4))) ||
                (ht.includes(n2) || n2.includes(ht.slice(-4))) && (at.includes(n1) || n1.includes(at.slice(-4)))) {
              const st = fx.fixture?.status?.short;
              if (st === 'NS' || st === 'TBD') { r = { result: 'PENDING', score: '-' }; break; }
              if (['1H','HT','2H','ET','BT','P'].includes(st)) {
                r = { result: 'LIVE', score: `${fx.goals?.home || 0}:${fx.goals?.away || 0}` }; break;
              }
              if (st === 'FT' || st === 'AET' || st === 'PEN') {
                const hg = fx.goals?.home || 0;
                const ag = fx.goals?.away || 0;
                const homeIsT1 = ht.includes(n1) || n1.includes(ht.slice(-4));
                const t1Won = homeIsT1 ? hg > ag : ag > hg;
                r = { result: (m.pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: homeIsT1 ? `${hg}:${ag}` : `${ag}:${hg}` };
                break;
              }
            }
          }
          if (r) break;
        }
      }
      if (r) return { ...m, ...r };
    }
    return { ...m, result: 'PENDING', score: 'N/F' };
  });
}

// ══════════════════════════════════════
// GPT-4o VISION
// ══════════════════════════════════════

async function readSlip(imageBuffer) {
  const base64 = imageBuffer.toString('base64');
  const prompt = `Read this bet365 betting slip image carefully. Return ONLY valid JSON:
{"matches":[{"display":"Bulgarian text shown","team1":"English team name","team1abbr":"CAR","team2":"English team name","team2abbr":"MTL","pick":"1","odds":1.50,"sport":"NHL","league":null}],"stake":150,"totalOdds":3.81,"potentialWin":593.48}
sport must be: NHL, MLB, NBA, NFL, or SOCCER
For soccer, add league: 39=EPL, 140=LaLiga, 78=Bundesliga, 135=SerieA, 61=Ligue1, 2=UCL
Return ONLY JSON, nothing else.`;

  const response = await axios.post('https://api.openai.com/v1/chat/completions', {
    model: 'gpt-4o',
    max_tokens: 1000,
    messages: [{
      role: 'user',
      content: [
        { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${base64}`, detail: 'high' } },
        { type: 'text', text: prompt }
      ]
    }]
  }, {
    headers: { 'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' }
  });

  const text = response.data.choices[0].message.content;
  const clean = text.replace(/```[\w]*\n?/g, '').replace(/```/g, '').trim();
  return JSON.parse(clean);
}

// ══════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════

// Get all clients with balances
app.get('/api/clients', async (req, res) => {
  try {
    const clients = await pool.query('SELECT * FROM clients ORDER BY id');
    const slips = await pool.query('SELECT client_id, result, our_amount, is_profit FROM slips');
    
    const result = clients.rows.map(c => {
      const cSlips = slips.rows.filter(s => s.client_id === c.id);
      const balance = cSlips.reduce((sum, s) => sum + (s.is_profit ? Number(s.our_amount) : -Number(s.our_amount)), 0);
      const wins = cSlips.filter(s => s.result === 'LOSS').length;
      const losses = cSlips.filter(s => s.result === 'WIN').length;
      return { ...c, balance: Math.round(balance * 100) / 100, wins, losses, total_slips: cSlips.length };
    });
    
    res.json(result);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get slips for client
app.get('/api/clients/:id/slips', async (req, res) => {
  try {
    const slips = await pool.query(
      'SELECT * FROM slips WHERE client_id = $1 ORDER BY created_at DESC',
      [req.params.id]
    );
    res.json(slips.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Upload and analyze slip
app.post('/api/analyze', upload.single('image'), async (req, res) => {
  try {
    const { client_id } = req.body;
    if (!req.file) return res.status(400).json({ error: 'Няма снимка' });
    if (!client_id) return res.status(400).json({ error: 'Няма клиент' });

    // Read slip with GPT-4o
    const slip = await readSlip(req.file.buffer);
    
    // Check results
    const resolvedMatches = await resolveMatches(slip.matches || []);
    
    // Calculate result
    const anyLoss = resolvedMatches.some(m => m.result === 'LOSS');
    const allWin = resolvedMatches.every(m => m.result === 'WIN');
    const isProfit = anyLoss;
    const isPending = !anyLoss && !allWin;
    const ourAmount = isPending ? 0 : isProfit ? slip.stake : slip.potentialWin;
    const result = isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN';

    // Save image
    const imgDir = path.join(__dirname, 'public', 'uploads');
    if (!fs.existsSync(imgDir)) fs.mkdirSync(imgDir, { recursive: true });
    const imgName = `slip_${Date.now()}.jpg`;
    const imgPath = path.join(imgDir, imgName);
    fs.writeFileSync(imgPath, req.file.buffer);

    // Save to DB
    const saved = await pool.query(
      `INSERT INTO slips (client_id, stake, total_odds, potential_win, result, our_amount, is_profit, matches, image_path)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING *`,
      [client_id, slip.stake, slip.totalOdds, slip.potentialWin, result, ourAmount, isProfit, JSON.stringify(resolvedMatches), `/uploads/${imgName}`]
    );

    // Broadcast to all connected dashboards
    broadcast({ type: 'new_slip', slip: saved.rows[0] });

    res.json({
      success: true,
      slip: saved.rows[0],
      matches: resolvedMatches,
      isProfit,
      isPending,
      ourAmount,
      result
    });
  } catch(e) {
    console.error(e);
    res.status(500).json({ error: e.message });
  }
});

// Get live matches
app.get('/api/live', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [nhl, mlb, nba] = await Promise.all([
      fetchNHL(today),
      fetchMLB(today),
      fetchESPN('basketball', 'nba', today)
    ]);
    
    const live = [
      ...nhl.filter(g => g.gameState === 'LIVE' || g.gameState === 'CRIT').map(g => ({
        sport: 'НХЛ',
        home: g.homeTeam?.abbrev,
        away: g.awayTeam?.abbrev,
        homeScore: g.homeTeam?.score || 0,
        awayScore: g.awayTeam?.score || 0,
        status: 'LIVE'
      })),
      ...mlb.filter(g => g.status?.abstractGameState === 'Live').map(g => ({
        sport: 'МЛБ',
        home: g.teams?.home?.team?.abbreviation,
        away: g.teams?.away?.team?.abbreviation,
        homeScore: g.teams?.home?.score || 0,
        awayScore: g.teams?.away?.score || 0,
        status: 'LIVE'
      })),
      ...nba.filter(e => e.competitions?.[0]?.status?.type?.state === 'in').map(e => {
        const comp = e.competitions[0];
        const cs = comp.competitors;
        return {
          sport: 'НБА',
          home: cs.find(c => c.homeAway === 'home')?.team?.abbreviation,
          away: cs.find(c => c.homeAway === 'away')?.team?.abbreviation,
          homeScore: cs.find(c => c.homeAway === 'home')?.score || 0,
          awayScore: cs.find(c => c.homeAway === 'away')?.score || 0,
          status: 'LIVE'
        };
      })
    ];
    
    res.json(live);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Update slip result manually
app.patch('/api/slips/:id', async (req, res) => {
  try {
    const { result, our_amount, is_profit } = req.body;
    const updated = await pool.query(
      'UPDATE slips SET result=$1, our_amount=$2, is_profit=$3 WHERE id=$4 RETURNING *',
      [result, our_amount, is_profit, req.params.id]
    );
    broadcast({ type: 'slip_updated', slip: updated.rows[0] });
    res.json(updated.rows[0]);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Stats
app.get('/api/stats', async (req, res) => {
  try {
    const stats = await pool.query(`
      SELECT 
        c.name,
        c.color,
        COUNT(s.id) as total_slips,
        SUM(CASE WHEN s.is_profit THEN s.our_amount ELSE -s.our_amount END) as balance,
        COUNT(CASE WHEN s.result = 'LOSS' THEN 1 END) as wins,
        COUNT(CASE WHEN s.result = 'WIN' THEN 1 END) as losses
      FROM clients c
      LEFT JOIN slips s ON c.id = s.client_id
      GROUP BY c.id, c.name, c.color
      ORDER BY c.id
    `);
    res.json(stats.rows);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Serve frontend
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Auto-refresh live scores every 30 seconds
setInterval(async () => {
  try {
    const pending = await pool.query("SELECT * FROM slips WHERE result = 'PENDING' ORDER BY created_at DESC LIMIT 20");
    for (const slip of pending.rows) {
      const matches = slip.matches || [];
      const resolved = await resolveMatches(matches);
      const anyLoss = resolved.some(m => m.result === 'LOSS');
      const allWin = resolved.every(m => m.result === 'WIN');
      if (anyLoss || allWin) {
        const isProfit = anyLoss;
        const ourAmount = isProfit ? slip.stake : slip.potential_win;
        const result = isProfit ? 'LOSS' : 'WIN';
        await pool.query(
          'UPDATE slips SET result=$1, our_amount=$2, is_profit=$3, matches=$4 WHERE id=$5',
          [result, ourAmount, isProfit, JSON.stringify(resolved), slip.id]
        );
        broadcast({ type: 'slip_resolved', slip_id: slip.id, result, ourAmount, isProfit });
      }
    }
  } catch(e) { console.error('Auto-refresh error:', e.message); }
}, 30000);

// Start
const PORT = process.env.PORT || 3000;
initDB().then(() => {
  server.listen(PORT, () => console.log(`✅ Сървърът работи на порт ${PORT}`));
});
