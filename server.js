'use strict';

require('dotenv').config();
var express = require('express');
var http = require('http');
var WebSocket = require('ws');
var { Pool } = require('pg');
var multer = require('multer');
var axios = require('axios');
var cors = require('cors');
var path = require('path');
var fs = require('fs');

// ── APP SETUP ──────────────────────────────────────────
var app = express();
var server = http.createServer(app);

// WebSocket
var wss = new WebSocket.Server({ server: server });
wss.on('error', function(e) { console.error('WSS error:', e.message); });

// Middleware
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// File upload
var upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// Database
var pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000
});

pool.on('error', function(e) { console.error('DB pool error:', e.message); });

// ── BROADCAST ──────────────────────────────────────────
function broadcast(data) {
  var msg;
  try { msg = JSON.stringify(data); } catch(e) { return; }
  wss.clients.forEach(function(client) {
    try {
      if (client && client.readyState === WebSocket.OPEN) {
        client.send(msg);
      }
    } catch(e) {}
  });
}

// ── DATABASE INIT ──────────────────────────────────────
function initDB() {
  return pool.query(
    'CREATE TABLE IF NOT EXISTS clients (' +
    '  id SERIAL PRIMARY KEY,' +
    '  name TEXT NOT NULL,' +
    '  color TEXT DEFAULT \'#3b82f6\',' +
    '  notes TEXT DEFAULT \'\',' +
    '  active BOOLEAN DEFAULT true,' +
    '  created_at TIMESTAMP DEFAULT NOW()' +
    ');' +
    'CREATE TABLE IF NOT EXISTS slips (' +
    '  id SERIAL PRIMARY KEY,' +
    '  client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,' +
    '  slip_date DATE DEFAULT CURRENT_DATE,' +
    '  stake NUMERIC(10,2) NOT NULL DEFAULT 0,' +
    '  total_odds NUMERIC(10,2) DEFAULT 1,' +
    '  potential_win NUMERIC(10,2) DEFAULT 0,' +
    '  result TEXT DEFAULT \'PENDING\',' +
    '  our_amount NUMERIC(10,2) DEFAULT 0,' +
    '  is_profit BOOLEAN DEFAULT false,' +
    '  matches JSONB DEFAULT \'[]\',' +
    '  image_path TEXT,' +
    '  created_at TIMESTAMP DEFAULT NOW()' +
    ');' +
    'CREATE TABLE IF NOT EXISTS settlements (' +
    '  id SERIAL PRIMARY KEY,' +
    '  client_id INTEGER REFERENCES clients(id) ON DELETE CASCADE,' +
    '  settled_at TIMESTAMP DEFAULT NOW(),' +
    '  balance_at_settlement NUMERIC(10,2) DEFAULT 0,' +
    '  notes TEXT DEFAULT \'\'' +
    ');'
  ).then(function() {
    console.log('DB ready');
  }).catch(function(e) {
    console.error('DB init error:', e.message);
  });
}

// ── CLIENT STATS ───────────────────────────────────────
function getClientStats(clientId) {
  return Promise.all([
    pool.query('SELECT * FROM slips WHERE client_id=$1 ORDER BY created_at DESC', [clientId]),
    pool.query('SELECT settled_at FROM settlements WHERE client_id=$1 ORDER BY settled_at DESC LIMIT 1', [clientId])
  ]).then(function(results) {
    var slips = results[0].rows;
    var settlementRow = results[1].rows[0];
    var settlementDate = settlementRow ? new Date(settlementRow.settled_at) : new Date(0);
    var currentSlips = slips.filter(function(s) { return new Date(s.created_at) > settlementDate; });
    var balance = currentSlips.reduce(function(sum, s) {
      return sum + (s.is_profit ? Number(s.our_amount) : -Number(s.our_amount));
    }, 0);
    var total = slips.reduce(function(sum, s) {
      return sum + (s.is_profit ? Number(s.our_amount) : -Number(s.our_amount));
    }, 0);
    var wins = slips.filter(function(s) { return s.result === 'LOSS'; }).length;
    var losses = slips.filter(function(s) { return s.result === 'WIN'; }).length;
    var pending = slips.filter(function(s) { return s.result === 'PENDING'; });
    var exposure = pending.reduce(function(sum, s) { return sum + Number(s.potential_win); }, 0);
    return {
      balance: Math.round(balance * 100) / 100,
      total: Math.round(total * 100) / 100,
      wins: wins,
      losses: losses,
      exposure: Math.round(exposure * 100) / 100,
      total_slips: slips.length
    };
  });
}

// ── SPORTS APIs ────────────────────────────────────────
function safeGet(url, opts) {
  return axios.get(url, Object.assign({ timeout: 7000 }, opts || {}))
    .then(function(r) { return r.data; })
    .catch(function() { return null; });
}

function norm(s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); }

function fetchNHL(date) {
  return safeGet('https://api-web.nhle.com/v1/score/' + date)
    .then(function(d) { return (d && d.games) ? d.games : []; });
}

function fetchMLB(date) {
  return safeGet('https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=' + date + '&hydrate=linescore')
    .then(function(d) { return (d && d.dates && d.dates[0]) ? d.dates[0].games : []; });
}

function fetchESPN(sport, league, date) {
  return safeGet('https://site.api.espn.com/apis/site/v2/sports/' + sport + '/' + league + '/scoreboard?dates=' + date.replace(/-/g, ''))
    .then(function(d) { return (d && d.events) ? d.events : []; });
}

function fetchFootball(date) {
  if (!process.env.SPORTS_API_KEY) return Promise.resolve([]);
  return safeGet('https://v3.football.api-sports.io/fixtures', {
    headers: { 'x-apisports-key': process.env.SPORTS_API_KEY },
    params: { date: date }
  }).then(function(d) { return (d && d.response) ? d.response : []; });
}

function resolveNHL(games, a1, a2, pick) {
  var u1 = String(a1 || '').toUpperCase();
  var u2 = String(a2 || '').toUpperCase();
  for (var i = 0; i < games.length; i++) {
    var g = games[i];
    var home = g.homeTeam && g.homeTeam.abbrev ? g.homeTeam.abbrev.toUpperCase() : '';
    var away = g.awayTeam && g.awayTeam.abbrev ? g.awayTeam.abbrev.toUpperCase() : '';
    if (!((home === u1 || away === u1) && (home === u2 || away === u2))) continue;
    var st = g.gameState || '';
    if (st === 'FUT' || st === 'PRE') return { result: 'PENDING', score: '-' };
    var hs = Number((g.homeTeam && g.homeTeam.score) || 0);
    var as = Number((g.awayTeam && g.awayTeam.score) || 0);
    var t1Away = away === u1;
    var sc = (t1Away ? as : hs) + ':' + (t1Away ? hs : as);
    if (st === 'LIVE' || st === 'CRIT') return { result: 'LIVE', score: sc };
    var t1Won = t1Away ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveMLB(games, t1, t2, pick) {
  var n1 = norm(t1).slice(-5);
  var n2 = norm(t2).slice(-5);
  for (var i = 0; i < games.length; i++) {
    var g = games[i];
    var aN = norm(g.teams && g.teams.away && g.teams.away.team ? g.teams.away.team.name : '');
    var hN = norm(g.teams && g.teams.home && g.teams.home.team ? g.teams.home.team.name : '');
    var m1 = aN.includes(n1) || n1.includes(aN.slice(-4));
    var m2 = hN.includes(n2) || n2.includes(hN.slice(-4));
    var m3 = aN.includes(n2) || n2.includes(aN.slice(-4));
    var m4 = hN.includes(n1) || n1.includes(hN.slice(-4));
    if (!((m1 && m2) || (m3 && m4))) continue;
    var st = g.status ? g.status.abstractGameState : '';
    if (st === 'Preview') return { result: 'PENDING', score: '-' };
    var as = Number((g.teams && g.teams.away && g.teams.away.score) || 0);
    var hs = Number((g.teams && g.teams.home && g.teams.home.score) || 0);
    var ls = g.linescore;
    if (!as && ls && ls.teams && ls.teams.away) as = Number(ls.teams.away.runs || 0);
    if (!hs && ls && ls.teams && ls.teams.home) hs = Number(ls.teams.home.runs || 0);
    var t1Away = m1;
    var sc = (t1Away ? as : hs) + ':' + (t1Away ? hs : as);
    if (st === 'Live') return { result: 'LIVE', score: sc };
    var t1Won = t1Away ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveESPN(events, t1, t2, pick) {
  var n1 = norm(t1).slice(-4);
  var n2 = norm(t2).slice(-4);
  for (var i = 0; i < events.length; i++) {
    var ev = events[i];
    var comp = ev.competitions && ev.competitions[0];
    if (!comp) continue;
    var cs = comp.competitors || [];
    var names = cs.map(function(c) { return norm(c.team ? (c.team.displayName || '') : ''); });
    if (!names.some(function(n) { return n.includes(n1) || n1.includes(n.slice(-4)); })) continue;
    if (!names.some(function(n) { return n.includes(n2) || n2.includes(n.slice(-4)); })) continue;
    var st = comp.status && comp.status.type;
    if (!st) return { result: 'PENDING', score: '-' };
    if (!st.completed && st.state !== 'in') return { result: 'PENDING', score: '-' };
    if (st.state === 'in') {
      return { result: 'LIVE', score: cs.map(function(c) { return c.score || '0'; }).join(':') };
    }
    var winner = null;
    for (var j = 0; j < cs.length; j++) { if (cs[j].winner) { winner = cs[j]; break; } }
    if (!winner) return { result: 'PENDING', score: '?' };
    var wn = norm(winner.team ? (winner.team.displayName || '') : '');
    var s1 = '?', s2 = '?';
    for (var k = 0; k < cs.length; k++) {
      var nm = norm(cs[k].team ? (cs[k].team.displayName || '') : '');
      if (nm.includes(n1) || n1.includes(nm.slice(-4))) s1 = cs[k].score || '?';
      if (nm.includes(n2) || n2.includes(nm.slice(-4))) s2 = cs[k].score || '?';
    }
    var t1Won = wn.includes(n1) || n1.includes(wn.slice(-4));
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: s1 + ':' + s2 };
  }
  return null;
}

function resolveFootballFixtures(fixtures, t1, t2, pick) {
  var n1 = norm(t1).slice(-4);
  var n2 = norm(t2).slice(-4);
  for (var i = 0; i < fixtures.length; i++) {
    var fx = fixtures[i];
    var ht = norm(fx.teams && fx.teams.home ? fx.teams.home.name : '');
    var at = norm(fx.teams && fx.teams.away ? fx.teams.away.name : '');
    var match = (ht.includes(n1) || n1.includes(ht.slice(-4))) && (at.includes(n2) || n2.includes(at.slice(-4)));
    var matchRev = (ht.includes(n2) || n2.includes(ht.slice(-4))) && (at.includes(n1) || n1.includes(at.slice(-4)));
    if (!match && !matchRev) continue;
    var st = fx.fixture && fx.fixture.status ? fx.fixture.status.short : '';
    if (st === 'NS' || st === 'TBD') return { result: 'PENDING', score: '-' };
    if (['1H','HT','2H','ET','BT','P'].indexOf(st) !== -1) {
      return { result: 'LIVE', score: (fx.goals ? fx.goals.home || 0 : 0) + ':' + (fx.goals ? fx.goals.away || 0 : 0) };
    }
    if (st === 'FT' || st === 'AET' || st === 'PEN') {
      var hg = fx.goals ? Number(fx.goals.home || 0) : 0;
      var ag = fx.goals ? Number(fx.goals.away || 0) : 0;
      var homeIsT1 = match;
      var t1Won = homeIsT1 ? hg > ag : ag > hg;
      return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: homeIsT1 ? hg + ':' + ag : ag + ':' + hg };
    }
  }
  return null;
}

function resolveMatches(matches, slipDate) {
  var baseDate = slipDate ? new Date(slipDate) : new Date();
  var dates = [];
  for (var i = -5; i <= 2; i++) {
    var d = new Date(baseDate.getTime());
    d.setDate(d.getDate() + i);
    dates.push(d.toISOString().split('T')[0]);
  }
  var today = new Date().toISOString().split('T')[0];
  if (dates.indexOf(today) === -1) dates.push(today);

  var sports = {};
  matches.forEach(function(m) { if (m.sport) sports[m.sport] = true; });

  var fetchPromises = [];
  var cacheKeys = [];

  dates.forEach(function(dt) {
    if (sports['NHL']) { fetchPromises.push(fetchNHL(dt)); cacheKeys.push('NHL_' + dt); }
    if (sports['MLB']) { fetchPromises.push(fetchMLB(dt)); cacheKeys.push('MLB_' + dt); }
    if (sports['NBA']) { fetchPromises.push(fetchESPN('basketball', 'nba', dt)); cacheKeys.push('NBA_' + dt); }
    if (sports['NFL']) { fetchPromises.push(fetchESPN('football', 'nfl', dt)); cacheKeys.push('NFL_' + dt); }
    if (sports['SOCCER']) {
      fetchPromises.push(fetchFootball(dt)); cacheKeys.push('SOC_' + dt);
      ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'].forEach(function(lg) {
        fetchPromises.push(fetchESPN('soccer', lg, dt)); cacheKeys.push('ESPN_' + lg + '_' + dt);
      });
    }
  });

  return Promise.all(fetchPromises).then(function(results) {
    var cache = {};
    for (var i = 0; i < cacheKeys.length; i++) {
      cache[cacheKeys[i]] = results[i] || [];
    }

    return matches.map(function(m) {
      for (var di = 0; di < dates.length; di++) {
        var dt = dates[di];
        var r = null;
        if (m.sport === 'NHL') r = resolveNHL(cache['NHL_' + dt] || [], m.team1abbr, m.team2abbr, m.pick);
        else if (m.sport === 'MLB') r = resolveMLB(cache['MLB_' + dt] || [], m.team1, m.team2, m.pick);
        else if (m.sport === 'NBA') r = resolveESPN(cache['NBA_' + dt] || [], m.team1, m.team2, m.pick);
        else if (m.sport === 'NFL') r = resolveESPN(cache['NFL_' + dt] || [], m.team1, m.team2, m.pick);
        else if (m.sport === 'SOCCER') {
          r = resolveFootballFixtures(cache['SOC_' + dt] || [], m.team1, m.team2, m.pick);
          if (!r) {
            var leagues = ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'];
            for (var li = 0; li < leagues.length; li++) {
              r = resolveESPN(cache['ESPN_' + leagues[li] + '_' + dt] || [], m.team1, m.team2, m.pick);
              if (r) break;
            }
          }
        }
        if (r) return Object.assign({}, m, r);
      }
      return Object.assign({}, m, { result: 'PENDING', score: 'N/F' });
    });
  });
}

// ── GPT-4o VISION ──────────────────────────────────────
function readSlip(imageBuffer) {
  var base64 = imageBuffer.toString('base64');
  var prompt = 'Read this bet365 betting slip. Return ONLY valid JSON:\n' +
    '{"matches":[{"display":"text","team1":"English name","team1abbr":"CAR","team2":"English name","team2abbr":"MTL","pick":"1","odds":1.50,"sport":"NHL","league":null}],"stake":150,"totalOdds":3.81,"potentialWin":593.48,"slipDate":"2026-05-22"}\n' +
    'sport: NHL, MLB, NBA, NFL, SOCCER only. slipDate: date from slip YYYY-MM-DD. Return ONLY JSON.';

  return axios.post('https://api.openai.com/v1/chat/completions', {
    model: 'gpt-4o',
    max_tokens: 1200,
    messages: [{
      role: 'user',
      content: [
        { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + base64, detail: 'high' } },
        { type: 'text', text: prompt }
      ]
    }]
  }, {
    headers: { 'Authorization': 'Bearer ' + process.env.OPENAI_API_KEY, 'Content-Type': 'application/json' },
    timeout: 30000
  }).then(function(response) {
    var text = response.data.choices[0].message.content;
    var clean = text.replace(/\u0060{3}[\w]*\n?/g, '').replace(/\u0060{3}/g, '').trim();
    try { return JSON.parse(clean); } catch(e) {
      var match = clean.match(/\{[\s\S]*\}/);
      if (match) return JSON.parse(match[0]);
      throw new Error('GPT-4o did not return valid JSON');
    }
  });
}

// ── AUTO-GRADING ───────────────────────────────────────
function autoGrade() {
  pool.query("SELECT * FROM slips WHERE result='PENDING' LIMIT 30")
    .then(function(result) {
      var pending = result.rows;
      var promises = pending.map(function(slip) {
        var matches = slip.matches || [];
        if (!matches.length) return Promise.resolve();
        return resolveMatches(matches, slip.slip_date).then(function(resolved) {
          var anyLoss = resolved.some(function(m) { return m.result === 'LOSS'; });
          var allWin = resolved.every(function(m) { return m.result === 'WIN'; });
          if (anyLoss || allWin) {
            var isProfit = anyLoss;
            var ourAmount = isProfit ? Number(slip.stake) : Number(slip.potential_win);
            var res = isProfit ? 'LOSS' : 'WIN';
            return pool.query(
              'UPDATE slips SET result=$1,our_amount=$2,is_profit=$3,matches=$4 WHERE id=$5',
              [res, ourAmount, isProfit, JSON.stringify(resolved), slip.id]
            ).then(function() {
              broadcast({ type: 'slip_graded', slip_id: slip.id, result: res, client_id: slip.client_id });
            });
          }
        }).catch(function(e) { console.error('autoGrade slip error:', e.message); });
      });
      return Promise.all(promises);
    })
    .catch(function(e) { console.error('autoGrade error:', e.message); });
}

// ── ROUTES ─────────────────────────────────────────────

// GET all clients
app.get('/api/clients', function(req, res) {
  pool.query('SELECT * FROM clients WHERE active=true ORDER BY id')
    .then(function(result) {
      var clients = result.rows;
      return Promise.all(clients.map(function(c) {
        return getClientStats(c.id).then(function(stats) {
          return Object.assign({}, c, stats);
        });
      }));
    })
    .then(function(data) { res.json(data); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// POST add client
app.post('/api/clients', function(req, res) {
  var name = req.body.name;
  var color = req.body.color || '#3b82f6';
  if (!name) return res.status(400).json({ error: 'Name required' });
  pool.query('INSERT INTO clients (name,color) VALUES ($1,$2) RETURNING *', [name, color])
    .then(function(result) {
      broadcast({ type: 'client_added', client: result.rows[0] });
      res.json(result.rows[0]);
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// PATCH edit client
app.patch('/api/clients/:id', function(req, res) {
  var id = req.params.id;
  var name = req.body.name;
  var color = req.body.color;
  var notes = req.body.notes;
  var sets = [], vals = [];
  if (name !== undefined) { sets.push('name=$' + (sets.length+1)); vals.push(name); }
  if (color !== undefined) { sets.push('color=$' + (sets.length+1)); vals.push(color); }
  if (notes !== undefined) { sets.push('notes=$' + (sets.length+1)); vals.push(notes); }
  if (!sets.length) return res.status(400).json({ error: 'Nothing to update' });
  vals.push(id);
  pool.query('UPDATE clients SET ' + sets.join(',') + ' WHERE id=$' + vals.length + ' RETURNING *', vals)
    .then(function(result) {
      broadcast({ type: 'client_updated', client: result.rows[0] });
      res.json(result.rows[0]);
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// DELETE client
app.delete('/api/clients/:id', function(req, res) {
  pool.query('UPDATE clients SET active=false WHERE id=$1', [req.params.id])
    .then(function() {
      broadcast({ type: 'client_deleted', client_id: req.params.id });
      res.json({ success: true });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// POST settle client
app.post('/api/clients/:id/settle', function(req, res) {
  var id = req.params.id;
  getClientStats(id).then(function(stats) {
    return pool.query(
      'INSERT INTO settlements (client_id,balance_at_settlement,notes) VALUES ($1,$2,$3)',
      [id, stats.balance, req.body.notes || '']
    ).then(function() {
      broadcast({ type: 'client_settled', client_id: id, balance: stats.balance });
      res.json({ success: true, balance: stats.balance });
    });
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET client slips
app.get('/api/clients/:id/slips', function(req, res) {
  pool.query('SELECT * FROM slips WHERE client_id=$1 ORDER BY created_at DESC', [req.params.id])
    .then(function(result) { res.json(result.rows); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET client chart data
app.get('/api/clients/:id/chart', function(req, res) {
  pool.query(
    "SELECT slip_date::text as date, SUM(CASE WHEN is_profit THEN our_amount ELSE -our_amount END) as amount " +
    "FROM slips WHERE client_id=$1 AND result!='PENDING' GROUP BY slip_date ORDER BY slip_date DESC LIMIT 14",
    [req.params.id]
  ).then(function(result) { res.json(result.rows.reverse()); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET overall chart
app.get('/api/chart', function(req, res) {
  pool.query(
    "SELECT slip_date::text as date, SUM(CASE WHEN is_profit THEN our_amount ELSE -our_amount END) as amount " +
    "FROM slips WHERE result!='PENDING' GROUP BY slip_date ORDER BY slip_date DESC LIMIT 14"
  ).then(function(result) { res.json(result.rows.reverse()); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET exposure
app.get('/api/exposure', function(req, res) {
  pool.query(
    "SELECT c.name,c.color,SUM(s.potential_win) as exposure " +
    "FROM slips s JOIN clients c ON s.client_id=c.id WHERE s.result='PENDING' GROUP BY c.id,c.name,c.color"
  ).then(function(result) { res.json(result.rows); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// POST analyze slip image
app.post('/api/analyze', upload.single('image'), function(req, res) {
  if (!req.file) return res.status(400).json({ error: 'No image' });
  var clientId = req.body.client_id;
  if (!clientId) return res.status(400).json({ error: 'No client' });

  readSlip(req.file.buffer).then(function(slip) {
    var slipDate = slip.slipDate || new Date().toISOString().split('T')[0];
    return resolveMatches(slip.matches || [], slipDate).then(function(resolved) {
      var anyLoss = resolved.some(function(m) { return m.result === 'LOSS'; });
      var allWin = resolved.every(function(m) { return m.result === 'WIN'; });
      var isProfit = anyLoss;
      var isPending = !anyLoss && !allWin;
      var ourAmount = isPending ? 0 : isProfit ? Number(slip.stake) : Number(slip.potentialWin);
      var result = isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN';

      // Save image
      var imgDir = path.join(__dirname, 'public', 'uploads');
      if (!fs.existsSync(imgDir)) fs.mkdirSync(imgDir, { recursive: true });
      var imgName = 'slip_' + Date.now() + '.jpg';
      try { fs.writeFileSync(path.join(imgDir, imgName), req.file.buffer); } catch(e) {}

      return pool.query(
        'INSERT INTO slips (client_id,slip_date,stake,total_odds,potential_win,result,our_amount,is_profit,matches,image_path) ' +
        'VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *',
        [clientId, slipDate, slip.stake, slip.totalOdds, slip.potentialWin, result, ourAmount, isProfit, JSON.stringify(resolved), '/uploads/' + imgName]
      ).then(function(dbResult) {
        broadcast({ type: 'new_slip', client_id: clientId });
        res.json({
          success: true,
          slip: { id: dbResult.rows[0].id, stake: slip.stake, totalOdds: slip.totalOdds, potentialWin: slip.potentialWin },
          matches: resolved,
          isProfit: isProfit,
          isPending: isPending,
          ourAmount: ourAmount,
          result: result,
          slipDate: slipDate
        });
      });
    });
  }).catch(function(e) {
    console.error('analyze error:', e.message);
    res.status(500).json({ error: e.message });
  });
});

// POST manual slip
app.post('/api/slips/manual', function(req, res) {
  var b = req.body;
  pool.query(
    'INSERT INTO slips (client_id,slip_date,stake,total_odds,potential_win,result,our_amount,is_profit,matches) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING *',
    [b.client_id, b.slip_date || new Date().toISOString().split('T')[0], b.stake || 0, b.total_odds || 1, b.potential_win || 0, b.result || 'PENDING', b.our_amount || 0, b.is_profit || false, JSON.stringify(b.matches || [])]
  ).then(function(result) {
    broadcast({ type: 'new_slip', client_id: b.client_id });
    res.json(result.rows[0]);
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// PATCH update slip
app.patch('/api/slips/:id', function(req, res) {
  var b = req.body;
  pool.query(
    'UPDATE slips SET result=$1,our_amount=$2,is_profit=$3,matches=$4 WHERE id=$5 RETURNING *',
    [b.result, b.our_amount, b.is_profit, JSON.stringify(b.matches || []), req.params.id]
  ).then(function(result) {
    broadcast({ type: 'slip_updated' });
    res.json(result.rows[0]);
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// DELETE slip
app.delete('/api/slips/:id', function(req, res) {
  pool.query('DELETE FROM slips WHERE id=$1 RETURNING client_id', [req.params.id])
    .then(function(result) {
      broadcast({ type: 'slip_deleted', client_id: result.rows[0] && result.rows[0].client_id });
      res.json({ success: true });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET live
app.get('/api/live', function(req, res) {
  var today = new Date().toISOString().split('T')[0];
  Promise.all([fetchNHL(today), fetchMLB(today), fetchESPN('basketball', 'nba', today)])
    .then(function(results) {
      var nhl = results[0], mlb = results[1], nba = results[2];
      var live = [];
      nhl.forEach(function(g) {
        if (g.gameState === 'LIVE' || g.gameState === 'CRIT') {
          live.push({ sport: 'НХЛ', home: g.homeTeam && g.homeTeam.abbrev, away: g.awayTeam && g.awayTeam.abbrev, homeScore: (g.homeTeam && g.homeTeam.score) || 0, awayScore: (g.awayTeam && g.awayTeam.score) || 0, status: 'LIVE' });
        } else if (g.gameState === 'FUT' || g.gameState === 'PRE') {
          live.push({ sport: 'НХЛ', home: g.homeTeam && g.homeTeam.abbrev, away: g.awayTeam && g.awayTeam.abbrev, homeScore: 0, awayScore: 0, status: 'UPCOMING' });
        }
      });
      mlb.forEach(function(g) {
        var st = g.status && g.status.abstractGameState;
        if (st === 'Live') {
          live.push({ sport: 'МЛБ', home: g.teams && g.teams.home && g.teams.home.team && g.teams.home.team.abbreviation, away: g.teams && g.teams.away && g.teams.away.team && g.teams.away.team.abbreviation, homeScore: (g.teams && g.teams.home && g.teams.home.score) || 0, awayScore: (g.teams && g.teams.away && g.teams.away.score) || 0, status: 'LIVE' });
        }
      });
      nba.forEach(function(ev) {
        var comp = ev.competitions && ev.competitions[0];
        if (comp && comp.status && comp.status.type && comp.status.type.state === 'in') {
          var cs = comp.competitors || [];
          var home = cs.find(function(c) { return c.homeAway === 'home'; });
          var away = cs.find(function(c) { return c.homeAway === 'away'; });
          live.push({ sport: 'НБА', home: home && home.team && home.team.abbreviation, away: away && away.team && away.team.abbreviation, homeScore: (home && home.score) || 0, awayScore: (away && away.score) || 0, status: 'LIVE' });
        }
      });
      res.json({ live: live });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// Serve frontend
app.get('*', function(req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── START ──────────────────────────────────────────────
var PORT = process.env.PORT || 3000;

initDB().then(function() {
  server.listen(PORT, function() {
    console.log('Server running on port ' + PORT);
  });
  setInterval(autoGrade, 30000);
  autoGrade();
}).catch(function(e) {
  console.error('Failed to start:', e.message);
  process.exit(1);
});
