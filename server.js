require('dotenv').config();
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const axios = require('axios');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
const WebSocket = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());

// Log ALL requests
app.use(function(req, res, next) {
  console.log('[' + new Date().toISOString() + '] ' + req.method + ' ' + req.path + ' body:' + JSON.stringify(req.body).slice(0,100));
  next();
});
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false }
});

// ══════════════════════════════════════
// DATABASE INIT
// ══════════════════════════════════════
async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS clients (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      color TEXT DEFAULT '#3b82f6',
      notes TEXT DEFAULT '',
      active BOOLEAN DEFAULT true,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS slips (
      id SERIAL PRIMARY KEY,
      client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
      slip_date DATE DEFAULT CURRENT_DATE,
      stake NUMERIC(10,2) NOT NULL DEFAULT 0,
      total_odds NUMERIC(10,2) DEFAULT 1,
      potential_win NUMERIC(10,2) DEFAULT 0,
      result TEXT DEFAULT 'PENDING',
      our_amount NUMERIC(10,2) DEFAULT 0,
      is_profit BOOLEAN DEFAULT false,
      matches JSONB DEFAULT '[]',
      image_path TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
    CREATE TABLE IF NOT EXISTS settlements (
      id SERIAL PRIMARY KEY,
      client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,
      settled_at TIMESTAMP DEFAULT NOW(),
      balance_at_settlement NUMERIC(10,2) DEFAULT 0,
      notes TEXT DEFAULT ''
    );
    INSERT INTO clients (name, color) VALUES
      ('Румен', '#3b82f6'),
      ('Славчо', '#f59e0b'),
      ('Близнаците', '#f97316'),
      ('Доги', '#ef4444')
    ON CONFLICT DO NOTHING;
  `);
  console.log('✅ DB ready');
}

// ══════════════════════════════════════
// WEBSOCKET
// ══════════════════════════════════════
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ══════════════════════════════════════
// SPORTS APIs
// ══════════════════════════════════════
function norm(s) { return (s || '').toLowerCase().replace(/[^a-z]/g, ''); }

async function safeGet(url, opts = {}) {
  try { const r = await axios.get(url, { timeout: 6000, ...opts }); return r.data; }
  catch(e) { return null; }
}

async function fetchNHL(date) {
  const d = await safeGet(`https://api-web.nhle.com/v1/score/${date}`);
  return d?.games || [];
}

async function fetchMLB(date) {
  const d = await safeGet(`https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=${date}&hydrate=linescore`);
  return d?.dates?.[0]?.games || [];
}

async function fetchESPN(sport, league, date) {
  const d = await safeGet(`https://site.api.espn.com/apis/site/v2/sports/${sport}/${league}/scoreboard?dates=${date.replace(/-/g,'')}`);
  return d?.events || [];
}

async function fetchApiFootball(date) {
  if (!process.env.SPORTS_API_KEY) return [];
  const d = await safeGet('https://v3.football.api-sports.io/fixtures', {
    headers: { 'x-apisports-key': process.env.SPORTS_API_KEY },
    params: { date }
  });
  return d?.response || [];
}

function resolveNHL(games, a1, a2, pick) {
  const u1 = (a1||'').toUpperCase(), u2 = (a2||'').toUpperCase();
  for (const g of games) {
    const home = g.homeTeam?.abbrev?.toUpperCase()||'';
    const away = g.awayTeam?.abbrev?.toUpperCase()||'';
    if (!((home===u1||away===u1)&&(home===u2||away===u2))) continue;
    const st = g.gameState||'';
    if (st==='FUT'||st==='PRE') return { result:'PENDING', score:'-' };
    const hs = Number(g.homeTeam?.score||0), as = Number(g.awayTeam?.score||0);
    const t1Away = away===u1, sc = (t1Away?as:hs)+':'+(t1Away?hs:as);
    if (st==='LIVE'||st==='CRIT') return { result:'LIVE', score:sc };
    const t1Won = t1Away ? as>hs : hs>as;
    return { result:(pick==='1'?t1Won:!t1Won)?'WIN':'LOSS', score:sc };
  }
  return null;
}

function resolveMLB(games, t1, t2, pick) {
  const n1=norm(t1).slice(-5), n2=norm(t2).slice(-5);
  for (const g of games) {
    const aN=norm(g.teams?.away?.team?.name||''), hN=norm(g.teams?.home?.team?.name||'');
    const m1=aN.includes(n1)||n1.includes(aN.slice(-4)), m2=hN.includes(n2)||n2.includes(hN.slice(-4));
    const m3=aN.includes(n2)||n2.includes(aN.slice(-4)), m4=hN.includes(n1)||n1.includes(hN.slice(-4));
    if (!((m1&&m2)||(m3&&m4))) continue;
    const st = g.status?.abstractGameState||'';
    if (st==='Preview') return { result:'PENDING', score:'-' };
    let as=Number(g.teams?.away?.score||0), hs=Number(g.teams?.home?.score||0);
    const ls=g.linescore;
    if (!as&&ls?.teams?.away) as=Number(ls.teams.away.runs||0);
    if (!hs&&ls?.teams?.home) hs=Number(ls.teams.home.runs||0);
    const t1Away=m1, sc=(t1Away?as:hs)+':'+(t1Away?hs:as);
    if (st==='Live') return { result:'LIVE', score:sc };
    return { result:(pick==='1'?(t1Away?as>hs:hs>as):(t1Away?hs>as:as>hs))?'WIN':'LOSS', score:sc };
  }
  return null;
}

function resolveESPN(events, t1, t2, pick) {
  const n1=norm(t1).slice(-4), n2=norm(t2).slice(-4);
  for (const ev of events) {
    const comp=ev.competitions?.[0]; if (!comp) continue;
    const cs=comp.competitors||[];
    const names=cs.map(c=>norm(c.team?.displayName||''));
    if (!names.some(n=>n.includes(n1)||n1.includes(n.slice(-4)))) continue;
    if (!names.some(n=>n.includes(n2)||n2.includes(n.slice(-4)))) continue;
    const st=comp.status?.type;
    if (!st?.completed&&st?.state!=='in') return { result:'PENDING', score:'-' };
    if (st?.state==='in') { const sc=cs.map(c=>c.score||'0').join(':'); return { result:'LIVE', score:sc }; }
    const winner=cs.find(c=>c.winner);
    if (!winner) return { result:'PENDING', score:'?' };
    const wn=norm(winner.team?.displayName||'');
    const s1=cs.find(c=>{const nm=norm(c.team?.displayName||'');return nm.includes(n1)||n1.includes(nm.slice(-4));})?.score||'?';
    const s2=cs.find(c=>{const nm=norm(c.team?.displayName||'');return nm.includes(n2)||n2.includes(nm.slice(-4));})?.score||'?';
    const t1Won=wn.includes(n1)||n1.includes(wn.slice(-4));
    return { result:(pick==='1'?t1Won:!t1Won)?'WIN':'LOSS', score:`${s1}:${s2}` };
  }
  return null;
}

function resolveFootball(fixtures, t1, t2, pick) {
  const n1=norm(t1).slice(-4), n2=norm(t2).slice(-4);
  for (const fx of fixtures) {
    const ht=norm(fx.teams?.home?.name||''), at=norm(fx.teams?.away?.name||'');
    const match=(ht.includes(n1)||n1.includes(ht.slice(-4)))&&(at.includes(n2)||n2.includes(at.slice(-4)));
    const matchRev=(ht.includes(n2)||n2.includes(ht.slice(-4)))&&(at.includes(n1)||n1.includes(at.slice(-4)));
    if (!match&&!matchRev) continue;
    const st=fx.fixture?.status?.short;
    if (st==='NS'||st==='TBD') return { result:'PENDING', score:'-' };
    if (['1H','HT','2H','ET','BT','P'].includes(st)) return { result:'LIVE', score:`${fx.goals?.home||0}:${fx.goals?.away||0}` };
    if (st==='FT'||st==='AET'||st==='PEN') {
      const hg=fx.goals?.home||0, ag=fx.goals?.away||0;
      const homeIsT1=match;
      const t1Won=homeIsT1?hg>ag:ag>hg;
      return { result:(pick==='1'?t1Won:!t1Won)?'WIN':'LOSS', score:homeIsT1?`${hg}:${ag}`:`${ag}:${hg}` };
    }
  }
  return null;
}

async function resolveMatches(matches, slipDate) {
  // Build date range around the slip date
  const baseDate = slipDate ? new Date(slipDate) : new Date();
  const dates = [];
  for (let i=-2; i<=2; i++) {
    const d = new Date(baseDate); d.setDate(d.getDate()+i);
    dates.push(d.toISOString().split('T')[0]);
  }
  // Also add today
  const today = new Date().toISOString().split('T')[0];
  if (!dates.includes(today)) dates.push(today);

  const cache = {};
  const sports = {};
  matches.forEach(m => sports[m.sport]=true);

  for (const dt of dates) {
    if (sports['NHL']&&!cache['NHL_'+dt]) cache['NHL_'+dt]=await fetchNHL(dt);
    if (sports['MLB']&&!cache['MLB_'+dt]) cache['MLB_'+dt]=await fetchMLB(dt);
    if ((sports['NBA']||sports['BASKETBALL'])&&!cache['NBA_'+dt]) cache['NBA_'+dt]=await fetchESPN('basketball','nba',dt);
    if ((sports['NFL']||sports['FOOTBALL'])&&!cache['NFL_'+dt]) cache['NFL_'+dt]=await fetchESPN('football','nfl',dt);
    if (sports['SOCCER']&&!cache['SOC_'+dt]) cache['SOC_'+dt]=await fetchApiFootball(dt);
    // Soccer fallback via ESPN
    const soccerLeagues=['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions','uefa.europa','ned.1','por.1'];
    if (sports['SOCCER']) {
      for (const lg of soccerLeagues) {
        if (!cache[`ESPN_${lg}_${dt}`]) cache[`ESPN_${lg}_${dt}`]=await fetchESPN('soccer',lg,dt);
      }
    }
  }

  return matches.map(m => {
    for (const dt of dates) {
      let r = null;
      if (m.sport==='NHL') r=resolveNHL(cache['NHL_'+dt]||[],m.team1abbr,m.team2abbr,m.pick);
      else if (m.sport==='MLB') r=resolveMLB(cache['MLB_'+dt]||[],m.team1,m.team2,m.pick);
      else if (m.sport==='NBA'||m.sport==='BASKETBALL') r=resolveESPN(cache['NBA_'+dt]||[],m.team1,m.team2,m.pick);
      else if (m.sport==='NFL'||m.sport==='FOOTBALL') r=resolveESPN(cache['NFL_'+dt]||[],m.team1,m.team2,m.pick);
      else if (m.sport==='SOCCER') {
        r=resolveFootball(cache['SOC_'+dt]||[],m.team1,m.team2,m.pick);
        if (!r) {
          const leagues=['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions','uefa.europa','ned.1','por.1'];
          for (const lg of leagues) { r=resolveESPN(cache[`ESPN_${lg}_${dt}`]||[],m.team1,m.team2,m.pick); if(r) break; }
        }
      }
      if (r) return { ...m, ...r };
    }
    return { ...m, result:'PENDING', score:'N/F' };
  });
}

// ══════════════════════════════════════
// GPT-4o VISION
// ══════════════════════════════════════
async function readSlip(imageBuffer) {
  const base64 = imageBuffer.toString('base64');
  const prompt = `Read this bet365 betting slip image carefully.
Return ONLY valid JSON, nothing else:
{
  "matches": [
    {
      "display": "Bulgarian text shown on slip",
      "team1": "Full English team name",
      "team1abbr": "3-letter abbr like CAR",
      "team2": "Full English team name", 
      "team2abbr": "3-letter abbr like MTL",
      "pick": "1",
      "odds": 1.50,
      "sport": "NHL",
      "league": null
    }
  ],
  "stake": 150,
  "totalOdds": 3.81,
  "potentialWin": 593.48,
  "slipDate": "2026-05-22"
}
Rules:
- sport: NHL, MLB, NBA, NFL, or SOCCER only
- For SOCCER add league: "eng.1" EPL, "esp.1" LaLiga, "ger.1" Bundesliga, "ita.1" SerieA, "fra.1" Ligue1, "uefa.champions" UCL
- slipDate: extract the match date from the slip in YYYY-MM-DD format. If not visible use today.
- pick "1" means first team wins, "2" means second team wins
- Return ONLY the JSON object, no markdown, no explanation`;

  const response = await axios.post('https://api.openai.com/v1/chat/completions', {
    model: 'gpt-4o',
    max_tokens: 1200,
    messages: [{
      role: 'user',
      content: [
        { type: 'image_url', image_url: { url: `data:image/jpeg;base64,${base64}`, detail: 'high' } },
        { type: 'text', text: prompt }
      ]
    }]
  }, { headers: { 'Authorization': `Bearer ${process.env.OPENAI_API_KEY}`, 'Content-Type': 'application/json' } });

  const text = response.data.choices[0].message.content;
  const clean = text.replace(/```[\w]*\n?/g,'').replace(/```/g,'').trim();
  try { return JSON.parse(clean); }
  catch(e) {
    const match = clean.match(/\{[\s\S]*\}/);
    if (match) return JSON.parse(match[0]);
    throw new Error('GPT-4o не върна валиден JSON');
  }
}

// ══════════════════════════════════════
// AUTO-GRADING (every 30s)
// ══════════════════════════════════════
async function autoGrade() {
  try {
    const { rows } = await pool.query(`SELECT * FROM slips WHERE result='PENDING' ORDER BY created_at DESC LIMIT 50`);
    for (const slip of rows) {
      const matches = slip.matches || [];
      if (!matches.length) continue;
      const resolved = await resolveMatches(matches, slip.slip_date);
      const anyLoss = resolved.some(m=>m.result==='LOSS');
      const allWin = resolved.every(m=>m.result==='WIN');
      const anyLive = resolved.some(m=>m.result==='LIVE');
      if (anyLoss||allWin) {
        const isProfit = anyLoss;
        const ourAmount = isProfit ? Number(slip.stake) : Number(slip.potential_win);
        const result = isProfit ? 'LOSS' : 'WIN';
        await pool.query(
          `UPDATE slips SET result=$1, our_amount=$2, is_profit=$3, matches=$4 WHERE id=$5`,
          [result, ourAmount, isProfit, JSON.stringify(resolved), slip.id]
        );
        broadcast({ type:'slip_graded', slip_id:slip.id, result, ourAmount, isProfit, client_id:slip.client_id });
      } else if (anyLive) {
        await pool.query(`UPDATE slips SET matches=$1 WHERE id=$2`, [JSON.stringify(resolved), slip.id]);
        broadcast({ type:'slip_live_update', slip_id:slip.id, matches:resolved });
      }
    }
  } catch(e) { console.error('autoGrade error:', e.message); }
}

// ══════════════════════════════════════
// HELPERS
// ══════════════════════════════════════
async function getClientStats(clientId) {
  const { rows } = await pool.query(`SELECT * FROM slips WHERE client_id=$1 ORDER BY created_at DESC`, [clientId]);
  const lastSettlement = await pool.query(`SELECT settled_at FROM settlements WHERE client_id=$1 ORDER BY settled_at DESC LIMIT 1`, [clientId]);
  const settlementDate = lastSettlement.rows[0]?.settled_at || new Date(0);
  const currentSlips = rows.filter(s => new Date(s.created_at) > new Date(settlementDate));
  const balance = currentSlips.reduce((sum,s) => sum+(s.is_profit?Number(s.our_amount):-Number(s.our_amount)), 0);
  const total = rows.reduce((sum,s) => sum+(s.is_profit?Number(s.our_amount):-Number(s.our_amount)), 0);
  const wins = rows.filter(s=>s.result==='LOSS').length;
  const losses = rows.filter(s=>s.result==='WIN').length;
  const pending = rows.filter(s=>s.result==='PENDING');
  const exposure = pending.reduce((sum,s) => sum+Number(s.potential_win), 0);
  return { balance: Math.round(balance*100)/100, total: Math.round(total*100)/100, wins, losses, exposure: Math.round(exposure*100)/100, total_slips: rows.length };
}

// ══════════════════════════════════════
// API ROUTES
// ══════════════════════════════════════

// GET all clients with stats
app.get('/api/clients', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT * FROM clients WHERE active=true ORDER BY id');
    const result = await Promise.all(rows.map(async c => ({ ...c, ...await getClientStats(c.id) })));
    res.json(result);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// POST add client
app.post('/api/clients', async (req, res) => {
  try {
    const { name, color } = req.body;
    const { rows } = await pool.query(`INSERT INTO clients (name,color) VALUES ($1,$2) RETURNING *`, [name, color||'#3b82f6']);
    broadcast({ type:'client_added', client:rows[0] });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// PATCH edit client
app.patch('/api/clients/:id', async (req, res) => {
  try {
    const { name, color, notes } = req.body;
    const fields = [], vals = [], idx = [];
    if (name) { fields.push(`name=$${fields.length+1}`); vals.push(name); }
    if (color) { fields.push(`color=$${fields.length+1}`); vals.push(color); }
    if (notes !== undefined) { fields.push(`notes=$${fields.length+1}`); vals.push(notes); }
    vals.push(req.params.id);
    const { rows } = await pool.query(`UPDATE clients SET ${fields.join(',')} WHERE id=$${vals.length} RETURNING *`, vals);
    broadcast({ type:'client_updated', client:rows[0] });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// DELETE client
app.delete('/api/clients/:id', async (req, res) => {
  try {
    await pool.query(`UPDATE clients SET active=false WHERE id=$1`, [req.params.id]);
    broadcast({ type:'client_deleted', client_id:req.params.id });
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// POST settle client
app.post('/api/clients/:id/settle', async (req, res) => {
  try {
    const stats = await getClientStats(req.params.id);
    await pool.query(`INSERT INTO settlements (client_id, balance_at_settlement, notes) VALUES ($1,$2,$3)`, [req.params.id, stats.balance, req.body.notes||'']);
    broadcast({ type:'client_settled', client_id:req.params.id, balance:stats.balance });
    res.json({ success:true, balance:stats.balance });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// GET client slips
app.get('/api/clients/:id/slips', async (req, res) => {
  try {
    const { rows } = await pool.query(`SELECT * FROM slips WHERE client_id=$1 ORDER BY created_at DESC`, [req.params.id]);
    res.json(rows);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// GET daily stats for chart
app.get('/api/clients/:id/chart', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT slip_date::text as date,
        SUM(CASE WHEN is_profit THEN our_amount ELSE -our_amount END) as amount
      FROM slips WHERE client_id=$1 AND result!='PENDING'
      GROUP BY slip_date ORDER BY slip_date DESC LIMIT 14
    `, [req.params.id]);
    res.json(rows.reverse());
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// GET overall chart
app.get('/api/chart', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT slip_date::text as date,
        SUM(CASE WHEN is_profit THEN our_amount ELSE -our_amount END) as amount
      FROM slips WHERE result!='PENDING'
      GROUP BY slip_date ORDER BY slip_date DESC LIMIT 14
    `);
    res.json(rows.reverse());
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// POST analyze slip image
app.post('/api/analyze', upload.single('image'), async (req, res) => {
  try {
    const { client_id } = req.body;
    if (!req.file) return res.status(400).json({ error:'Няма снимка' });
    if (!client_id) return res.status(400).json({ error:'Няма клиент' });

    // Compress image
    let imgBuffer = req.file.buffer;

    // Read with GPT-4o
    const slip = await readSlip(imgBuffer);
    const slipDate = slip.slipDate || new Date().toISOString().split('T')[0];

    // Resolve matches using slip date
    const resolved = await resolveMatches(slip.matches||[], slipDate);

    // Calculate
    const anyLoss = resolved.some(m=>m.result==='LOSS');
    const allWin = resolved.every(m=>m.result==='WIN');
    const isProfit = anyLoss, isPending = !anyLoss&&!allWin;
    const ourAmount = isPending ? 0 : isProfit ? Number(slip.stake) : Number(slip.potentialWin);
    const result = isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN';

    // Save image
    const imgDir = path.join(__dirname,'public','uploads');
    if (!fs.existsSync(imgDir)) fs.mkdirSync(imgDir,{recursive:true});
    const imgName = `slip_${Date.now()}.jpg`;
    fs.writeFileSync(path.join(imgDir,imgName), imgBuffer);

    // Save to DB
    const { rows } = await pool.query(
      `INSERT INTO slips (client_id,slip_date,stake,total_odds,potential_win,result,our_amount,is_profit,matches,image_path)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *`,
      [client_id, slipDate, slip.stake, slip.totalOdds, slip.potentialWin, result, ourAmount, isProfit, JSON.stringify(resolved), `/uploads/${imgName}`]
    );

    broadcast({ type:'new_slip', slip:rows[0], client_id });
    res.json({ success:true, slip:rows[0], matches:resolved, isProfit, isPending, ourAmount, result, slipDate });
  } catch(e) {
    console.error('analyze error:', e.message);
    res.status(500).json({ error:e.message });
  }
});

// POST manual slip
app.post('/api/slips/manual', async (req, res) => {
  try {
    const { client_id, stake, total_odds, potential_win, result, our_amount, is_profit, matches, slip_date } = req.body;
    const { rows } = await pool.query(
      `INSERT INTO slips (client_id,slip_date,stake,total_odds,potential_win,result,our_amount,is_profit,matches)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *`,
      [client_id, slip_date||new Date().toISOString().split('T')[0], stake, total_odds||1, potential_win||0, result||'PENDING', our_amount||0, is_profit||false, JSON.stringify(matches||[])]
    );
    broadcast({ type:'new_slip', slip:rows[0], client_id });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// PATCH update slip
app.patch('/api/slips/:id', async (req, res) => {
  try {
    const { result, our_amount, is_profit, matches } = req.body;
    const { rows } = await pool.query(
      `UPDATE slips SET result=$1,our_amount=$2,is_profit=$3,matches=$4 WHERE id=$5 RETURNING *`,
      [result, our_amount, is_profit, JSON.stringify(matches), req.params.id]
    );
    broadcast({ type:'slip_updated', slip:rows[0] });
    res.json(rows[0]);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// DELETE slip
app.delete('/api/slips/:id', async (req, res) => {
  try {
    const { rows } = await pool.query(`DELETE FROM slips WHERE id=$1 RETURNING client_id`, [req.params.id]);
    broadcast({ type:'slip_deleted', slip_id:req.params.id, client_id:rows[0]?.client_id });
    res.json({ success:true });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// GET live matches
app.get('/api/live', async (req, res) => {
  try {
    const today = new Date().toISOString().split('T')[0];
    const [nhl, mlb, nba] = await Promise.all([
      fetchNHL(today), fetchMLB(today), fetchESPN('basketball','nba',today)
    ]);

    const live = [
      ...nhl.filter(g=>g.gameState==='LIVE'||g.gameState==='CRIT').map(g=>({
        sport:'НХЛ', league:'Stanley Cup Playoffs',
        home:g.homeTeam?.abbrev, away:g.awayTeam?.abbrev,
        homeScore:g.homeTeam?.score||0, awayScore:g.awayTeam?.score||0, status:'LIVE'
      })),
      ...nhl.filter(g=>g.gameState==='FUT'||g.gameState==='PRE').slice(0,5).map(g=>({
        sport:'НХЛ', league:'NHL',
        home:g.homeTeam?.abbrev, away:g.awayTeam?.abbrev,
        homeScore:0, awayScore:0, status:'UPCOMING',
        time: g.startTimeUTC
      })),
      ...mlb.filter(g=>g.status?.abstractGameState==='Live').map(g=>({
        sport:'МЛБ', league:'MLB',
        home:g.teams?.home?.team?.abbreviation, away:g.teams?.away?.team?.abbreviation,
        homeScore:g.teams?.home?.score||0, awayScore:g.teams?.away?.score||0, status:'LIVE'
      })),
      ...nba.filter(e=>e.competitions?.[0]?.status?.type?.state==='in').map(e=>{
        const comp=e.competitions[0], cs=comp.competitors;
        return {
          sport:'НБА', league:'NBA Playoffs',
          home:cs.find(c=>c.homeAway==='home')?.team?.abbreviation,
          away:cs.find(c=>c.homeAway==='away')?.team?.abbreviation,
          homeScore:cs.find(c=>c.homeAway==='home')?.score||0,
          awayScore:cs.find(c=>c.homeAway==='away')?.score||0, status:'LIVE'
        };
      })
    ];

    // Find which clients have pending slips on these matches
    const pendingSlips = await pool.query(`SELECT s.*, c.name as client_name, c.color as client_color FROM slips s JOIN clients c ON s.client_id=c.id WHERE s.result='PENDING'`);

    res.json({ live, pendingSlips:pendingSlips.rows });
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// GET exposure
app.get('/api/exposure', async (req, res) => {
  try {
    const { rows } = await pool.query(`
      SELECT c.name, c.color, SUM(s.potential_win) as exposure
      FROM slips s JOIN clients c ON s.client_id=c.id
      WHERE s.result='PENDING'
      GROUP BY c.id, c.name, c.color
    `);
    res.json(rows);
  } catch(e) { res.status(500).json({ error:e.message }); }
});

// Serve frontend
app.get('*', (req, res) => res.sendFile(path.join(__dirname,'public','index.html')));

// Start auto-grading
setInterval(autoGrade, 30000);

// Init & Start
const PORT = process.env.PORT || 3000;
initDB().then(() => {
  server.listen(PORT, () => console.log(`✅ Server running on port ${PORT}`));
  autoGrade(); // Run immediately on start
});
