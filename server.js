'use strict';

require('dotenv').config();
var express = require('express');
var http = require('http');
var WebSocket = require('ws');
var multer = require('multer');
var axios = require('axios');
var cors = require('cors');
var path = require('path');
var fs = require('fs');

var app = express();
var server = http.createServer(app);
var wss = new WebSocket.Server({ server: server });
wss.on('error', function() {});

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

var upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// ── AIRTABLE CONFIG ────────────────────────────────────
var AT_KEY = process.env.AIRTABLE_KEY;
var AT_BASE = process.env.AIRTABLE_BASE || 'appil6CpexOd4trpk';

// Клиенти и техните таблици в Airtable
var CLIENT_TABLES = {
  'Румен':      { id: 1, tableId: 'tblXlqTekjWKVUn4B', color: '#3b82f6',
    fields: { date: 'fldreJGr0YY07Vetx', slip: 'fld8N7UCh2e4CnM92', amount: 'fldauOfHluYvw7VIv', odds: 'fld0JITwQ2OHpvlf8', potential: 'fldPq2kWFEm3nEf8M', result: 'fldBO6ChyLdbkFlBR', ourAmount: 'fldi7w9x5nC4QLJJE', notes: 'fldSlH5OXrNyK2XDZ' }
  },
  'Славчо':     { id: 2, tableId: 'tblGX3x0IEDX4leTw', color: '#f59e0b',
    fields: { date: 'fldwqoXiM6ou1SER2', slip: 'fldiMeTu91YT6MrJx', amount: 'fld6V0Sd3LywhqJNI', odds: 'fld7xU2zuQT9TialY', result: 'fld4NYoZDjbnwVxRz', ourAmount: 'fldZzzntT5S1gkp9u' }
  },
  'Близнаците': { id: 3, tableId: 'tblhPTRCtau0BNgHR', color: '#f97316',
    fields: { date: 'fldhKzZVZFzrQ5hTT', slip: 'fldOPFY5eKulE7O8G', amount: 'fldbtqonGgcI4nAwo', odds: 'fldhLJbgTCFPTng42', result: 'fld6Z9Mq56S24ui14', ourAmount: 'fldOFNQg1vKOwKUoQ' }
  },
  'Доги':       { id: 4, tableId: 'tbl972ougIeRQ7om4', color: '#ef4444',
    fields: { date: 'fldghYrk4e3M1ZzZl', slip: 'fldwPfd0V6Zrg8zSK', amount: 'fldykfL5iKQ2pklIZ', odds: 'fldqam0jgWYX5qa8z', result: 'fld980USug9ArqAbo', ourAmount: 'fld16uXyYacBCAY3Q' }
  }
};

var CLIENT_LIST = [
  { id: 1, name: 'Румен',      color: '#3b82f6' },
  { id: 2, name: 'Славчо',     color: '#f59e0b' },
  { id: 3, name: 'Близнаците', color: '#f97316' },
  { id: 4, name: 'Доги',       color: '#ef4444' }
];

// In-memory store за фишове (Airtable няма JSONB)
var slipsStore = {};
var nextSlipId = 1;

// ── AIRTABLE API ───────────────────────────────────────
function atRequest(method, path, data) {
  return axios({
    method: method,
    url: 'https://api.airtable.com/v0/' + AT_BASE + '/' + path,
    headers: {
      'Authorization': 'Bearer ' + AT_KEY,
      'Content-Type': 'application/json'
    },
    data: data,
    timeout: 10000
  }).then(function(r) { return r.data; })
    .catch(function(e) {
      console.error('Airtable error:', e.response ? JSON.stringify(e.response.data) : e.message);
      throw new Error(e.response ? JSON.stringify(e.response.data) : e.message);
    });
}

function atGetRecords(tableId) {
  return atRequest('GET', tableId + '?maxRecords=200&sort[0][field]=Date&sort[0][direction]=desc');
}

function atCreateRecord(tableId, fields) {
  return atRequest('POST', tableId, { fields: fields, typecast: true });
}

function atDeleteRecord(tableId, recordId) {
  return atRequest('DELETE', tableId + '/' + recordId);
}

// ── BROADCAST ──────────────────────────────────────────
function broadcast(data) {
  try {
    var msg = JSON.stringify(data);
    wss.clients.forEach(function(c) {
      try { if (c.readyState === WebSocket.OPEN) c.send(msg); } catch(e) {}
    });
  } catch(e) {}
}

// ── HELPERS ────────────────────────────────────────────
function getClientByName(name) {
  return CLIENT_TABLES[name] || null;
}

function getClientById(id) {
  var num = Number(id);
  for (var name in CLIENT_TABLES) {
    if (CLIENT_TABLES[name].id === num) return { name: name, config: CLIENT_TABLES[name] };
  }
  return null;
}

function calcBalance(records, clientConfig) {
  var balance = 0;
  records.forEach(function(r) {
    var f = r.fields;
    var result = f[clientConfig.fields.result];
    var ourAmt = Number(f[clientConfig.fields.ourAmount] || 0);
    if (result === 'Загубен фиш') balance += ourAmt;
    else if (result === 'Спечелен фиш') balance -= ourAmt;
  });
  return Math.round(balance * 100) / 100;
}

// ── SPORTS APIs ────────────────────────────────────────
function norm(s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); }

function safeGet(url, opts) {
  return axios.get(url, Object.assign({ timeout: 7000 }, opts || {}))
    .then(function(r) { return r.data; })
    .catch(function() { return null; });
}

function fetchNHL(date) {
  return safeGet('https://api-web.nhle.com/v1/score/' + date)
    .then(function(d) { return d && d.games ? d.games : []; });
}

function fetchMLB(date) {
  return safeGet('https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=' + date + '&hydrate=linescore')
    .then(function(d) { return d && d.dates && d.dates[0] ? d.dates[0].games : []; });
}

function fetchESPN(sport, league, date) {
  return safeGet('https://site.api.espn.com/apis/site/v2/sports/' + sport + '/' + league + '/scoreboard?dates=' + date.replace(/-/g, ''))
    .then(function(d) { return d && d.events ? d.events : []; });
}

function fetchFootball(date) {
  if (!process.env.SPORTS_API_KEY) return Promise.resolve([]);
  return safeGet('https://v3.football.api-sports.io/fixtures', {
    headers: { 'x-apisports-key': process.env.SPORTS_API_KEY },
    params: { date: date }
  }).then(function(d) { return d && d.response ? d.response : []; });
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
    var hs = Number(g.homeTeam && g.homeTeam.score || 0);
    var as = Number(g.awayTeam && g.awayTeam.score || 0);
    var t1Away = away === u1;
    var sc = (t1Away ? as : hs) + ':' + (t1Away ? hs : as);
    if (st === 'LIVE' || st === 'CRIT') return { result: 'LIVE', score: sc };
    var t1Won = t1Away ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveMLB(games, t1, t2, pick) {
  var n1 = norm(t1).slice(-5), n2 = norm(t2).slice(-5);
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
    var as = Number(g.teams && g.teams.away && g.teams.away.score || 0);
    var hs = Number(g.teams && g.teams.home && g.teams.home.score || 0);
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
  var n1 = norm(t1).slice(-4), n2 = norm(t2).slice(-4);
  for (var i = 0; i < events.length; i++) {
    var ev = events[i];
    var comp = ev.competitions && ev.competitions[0];
    if (!comp) continue;
    var cs = comp.competitors || [];
    var names = cs.map(function(c) { return norm(c.team ? c.team.displayName || '' : ''); });
    if (!names.some(function(n) { return n.includes(n1) || n1.includes(n.slice(-4)); })) continue;
    if (!names.some(function(n) { return n.includes(n2) || n2.includes(n.slice(-4)); })) continue;
    var st = comp.status && comp.status.type;
    if (!st || (!st.completed && st.state !== 'in')) return { result: 'PENDING', score: '-' };
    if (st.state === 'in') return { result: 'LIVE', score: cs.map(function(c) { return c.score || '0'; }).join(':') };
    var winner = null;
    for (var j = 0; j < cs.length; j++) { if (cs[j].winner) { winner = cs[j]; break; } }
    if (!winner) return { result: 'PENDING', score: '?' };
    var wn = norm(winner.team ? winner.team.displayName || '' : '');
    var s1 = '?', s2 = '?';
    for (var k = 0; k < cs.length; k++) {
      var nm = norm(cs[k].team ? cs[k].team.displayName || '' : '');
      if (nm.includes(n1) || n1.includes(nm.slice(-4))) s1 = cs[k].score || '?';
      if (nm.includes(n2) || n2.includes(nm.slice(-4))) s2 = cs[k].score || '?';
    }
    var t1Won = wn.includes(n1) || n1.includes(wn.slice(-4));
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: s1 + ':' + s2 };
  }
  return null;
}

function resolveMatches(matches, slipDate) {
  if (!matches || !matches.length) return Promise.resolve([]);
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

  var fetches = [], keys = [];
  dates.forEach(function(dt) {
    if (sports['NHL'])   { fetches.push(fetchNHL(dt));  keys.push('NHL_'+dt); }
    if (sports['MLB'])   { fetches.push(fetchMLB(dt));  keys.push('MLB_'+dt); }
    if (sports['NBA'])   { fetches.push(fetchESPN('basketball','nba',dt)); keys.push('NBA_'+dt); }
    if (sports['NFL'])   { fetches.push(fetchESPN('football','nfl',dt));   keys.push('NFL_'+dt); }
    if (sports['SOCCER']) {
      fetches.push(fetchFootball(dt)); keys.push('SOC_'+dt);
      ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'].forEach(function(lg) {
        fetches.push(fetchESPN('soccer',lg,dt)); keys.push('ESPN_'+lg+'_'+dt);
      });
    }
  });

  return Promise.all(fetches).then(function(results) {
    var cache = {};
    for (var i = 0; i < keys.length; i++) cache[keys[i]] = results[i] || [];
    return matches.map(function(m) {
      for (var di = 0; di < dates.length; di++) {
        var dt = dates[di], r = null;
        if (m.sport === 'NHL') r = resolveNHL(cache['NHL_'+dt]||[], m.team1abbr, m.team2abbr, m.pick);
        else if (m.sport === 'MLB') r = resolveMLB(cache['MLB_'+dt]||[], m.team1, m.team2, m.pick);
        else if (m.sport === 'NBA') r = resolveESPN(cache['NBA_'+dt]||[], m.team1, m.team2, m.pick);
        else if (m.sport === 'NFL') r = resolveESPN(cache['NFL_'+dt]||[], m.team1, m.team2, m.pick);
        else if (m.sport === 'SOCCER') {
          r = null;
          var lgs = ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'];
          for (var li = 0; li < lgs.length; li++) {
            r = resolveESPN(cache['ESPN_'+lgs[li]+'_'+dt]||[], m.team1, m.team2, m.pick);
            if (r) break;
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
    var clean = text.replace(/[`]{3}[\w]*\n?/g, '').replace(/[`]{3}/g, '').trim();
    try { return JSON.parse(clean); } catch(e) {
      var m = clean.match(/\{[\s\S]*\}/);
      if (m) return JSON.parse(m[0]);
      throw new Error('GPT-4o did not return valid JSON');
    }
  });
}

// ── ROUTES ─────────────────────────────────────────────

// GET clients
app.get('/api/clients', function(req, res) {
  var promises = CLIENT_LIST.map(function(c) {
    var config = CLIENT_TABLES[c.name];
    return atGetRecords(config.tableId).then(function(data) {
      var records = data.records || [];
      var balance = calcBalance(records, config);
      var wins = records.filter(function(r) { return r.fields[config.fields.result] === 'Загубен фиш'; }).length;
      var losses = records.filter(function(r) { return r.fields[config.fields.result] === 'Спечелен фиш'; }).length;
      return Object.assign({}, c, {
        balance: balance,
        total: balance,
        wins: wins,
        losses: losses,
        total_slips: records.length,
        exposure: 0
      });
    }).catch(function() {
      return Object.assign({}, c, { balance: 0, total: 0, wins: 0, losses: 0, total_slips: 0, exposure: 0 });
    });
  });
  Promise.all(promises).then(function(data) { res.json(data); })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET client slips
app.get('/api/clients/:id/slips', function(req, res) {
  var client = getClientById(req.params.id);
  if (!client) return res.status(404).json({ error: 'Client not found' });
  atGetRecords(client.config.tableId).then(function(data) {
    var records = data.records || [];
    var slips = records.map(function(r) {
      var f = r.fields;
      var result = f[client.config.fields.result];
      var isProfit = result === 'Загубен фиш';
      var isPending = result === 'Изчакване' || !result;
      return {
        id: r.id,
        client_id: client.config.id,
        slip_date: f[client.config.fields.date] || '',
        stake: Number(f[client.config.fields.amount] || 0),
        total_odds: Number(f[client.config.fields.odds] || 1),
        potential_win: Number(f[client.config.fields.potential] || f[client.config.fields.ourAmount] || 0),
        result: isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN',
        our_amount: Number(f[client.config.fields.ourAmount] || 0),
        is_profit: isProfit,
        matches: [{ display: f[client.config.fields.slip] || '' }],
        image_path: null
      };
    });
    res.json(slips);
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET chart
app.get('/api/clients/:id/chart', function(req, res) {
  var client = getClientById(req.params.id);
  if (!client) return res.json([]);
  atGetRecords(client.config.tableId).then(function(data) {
    var records = data.records || [];
    var byDate = {};
    records.forEach(function(r) {
      var f = r.fields;
      var date = f[client.config.fields.date] || new Date().toISOString().slice(0,10);
      var result = f[client.config.fields.result];
      var amt = Number(f[client.config.fields.ourAmount] || 0);
      if (!byDate[date]) byDate[date] = 0;
      if (result === 'Загубен фиш') byDate[date] += amt;
      else if (result === 'Спечелен фиш') byDate[date] -= amt;
    });
    var chartData = Object.keys(byDate).sort().slice(-14).map(function(d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    });
    res.json(chartData);
  }).catch(function() { res.json([]); });
});

app.get('/api/chart', function(req, res) {
  var promises = CLIENT_LIST.map(function(c) {
    var config = CLIENT_TABLES[c.name];
    return atGetRecords(config.tableId).then(function(data) {
      return data.records || [];
    }).catch(function() { return []; });
  });
  Promise.all(promises).then(function(allRecords) {
    var byDate = {};
    allRecords.forEach(function(records, idx) {
      var config = CLIENT_TABLES[CLIENT_LIST[idx].name];
      records.forEach(function(r) {
        var f = r.fields;
        var date = f[config.fields.date] || new Date().toISOString().slice(0,10);
        var result = f[config.fields.result];
        var amt = Number(f[config.fields.ourAmount] || 0);
        if (!byDate[date]) byDate[date] = 0;
        if (result === 'Загубен фиш') byDate[date] += amt;
        else if (result === 'Спечелен фиш') byDate[date] -= amt;
      });
    });
    var chartData = Object.keys(byDate).sort().slice(-14).map(function(d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    });
    res.json(chartData);
  }).catch(function() { res.json([]); });
});

app.get('/api/exposure', function(req, res) {
  res.json([]);
});

// POST analyze slip
app.post('/api/analyze', upload.single('image'), function(req, res) {
  if (!req.file) return res.status(400).json({ error: 'No image' });
  var clientId = req.body.client_id;
  if (!clientId) return res.status(400).json({ error: 'No client' });
  var client = getClientById(clientId);
  if (!client) return res.status(400).json({ error: 'Client not found' });

  readSlip(req.file.buffer).then(function(slip) {
    var slipDate = slip.slipDate || new Date().toISOString().split('T')[0];
    return resolveMatches(slip.matches || [], slipDate).then(function(resolved) {
      var anyLoss = resolved.some(function(m) { return m.result === 'LOSS'; });
      var allWin = resolved.every(function(m) { return m.result === 'WIN'; });
      var isProfit = anyLoss;
      var isPending = !anyLoss && !allWin;
      var ourAmount = isPending ? 0 : isProfit ? Number(slip.stake) : Number(slip.potentialWin);

      // Save image
      var imgDir = path.join(__dirname, 'public', 'uploads');
      if (!fs.existsSync(imgDir)) fs.mkdirSync(imgDir, { recursive: true });
      var imgName = 'slip_' + Date.now() + '.jpg';
      try { fs.writeFileSync(path.join(imgDir, imgName), req.file.buffer); } catch(e) {}

      // Build Airtable record
      var matchDisplay = resolved.map(function(m) { return m.display || m.team1 + ' vs ' + m.team2; }).join(' · ');
      var atResult = isPending ? 'Изчакване' : isProfit ? 'Загубен фиш' : 'Спечелен фиш';
      var f = client.config.fields;
      var fields = {};
      fields[f.date] = slipDate;
      fields[f.slip] = matchDisplay;
      fields[f.amount] = Number(slip.stake) || 0;
      fields[f.odds] = Number(slip.totalOdds) || 1;
      if (f.potential) fields[f.potential] = Number(slip.potentialWin) || 0;
      fields[f.result] = atResult;
      fields[f.ourAmount] = ourAmount;
      if (f.notes) fields[f.notes] = resolved.map(function(m) {
        return (m.display||'') + ' ' + (m.score && m.score!=='N/F'?m.score:'');
      }).join(' | ');

      return atCreateRecord(client.config.tableId, fields).then(function(atRecord) {
        var slipId = nextSlipId++;
        slipsStore[slipId] = {
          id: slipId,
          atId: atRecord.id,
          client_id: Number(clientId),
          tableId: client.config.tableId,
          matches: resolved,
          stake: slip.stake,
          totalOdds: slip.totalOdds,
          potentialWin: slip.potentialWin
        };
        broadcast({ type: 'new_slip', client_id: clientId });
        res.json({
          success: true,
          slip: { id: slipId, stake: slip.stake, totalOdds: slip.totalOdds, potentialWin: slip.potentialWin },
          matches: resolved,
          isProfit: isProfit,
          isPending: isPending,
          ourAmount: ourAmount,
          result: isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN',
          slipDate: slipDate,
          image_path: '/uploads/' + imgName
        });
      });
    });
  }).catch(function(e) {
    console.error('analyze error:', e.message);
    res.status(500).json({ error: e.message });
  });
});

// PATCH update slip
app.patch('/api/slips/:id', function(req, res) {
  var slipData = slipsStore[req.params.id];
  if (!slipData) return res.json({ success: true }); // Slip not in memory, ignore
  var b = req.body;
  var client = getClientById(slipData.client_id);
  if (!client) return res.json({ success: true });

  var atResult = b.result === 'PENDING' ? 'Изчакване' : b.is_profit ? 'Загубен фиш' : 'Спечелен фиш';
  var fields = {};
  fields[client.config.fields.result] = atResult;
  fields[client.config.fields.ourAmount] = Number(b.our_amount) || 0;

  atRequest('PATCH', client.config.tableId + '/' + slipData.atId, { fields: fields })
    .then(function() {
      slipsStore[req.params.id].matches = b.matches || slipData.matches;
      broadcast({ type: 'slip_updated' });
      res.json({ success: true });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// DELETE slip
app.delete('/api/slips/:id', function(req, res) {
  var slipData = slipsStore[req.params.id];
  if (!slipData) return res.json({ success: true });
  atDeleteRecord(slipData.tableId, slipData.atId)
    .then(function() {
      delete slipsStore[req.params.id];
      broadcast({ type: 'slip_deleted' });
      res.json({ success: true });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// POST manual slip
app.post('/api/slips/manual', function(req, res) {
  var b = req.body;
  var client = getClientById(b.client_id);
  if (!client) return res.status(400).json({ error: 'Client not found' });

  var atResult = b.result === 'PENDING' ? 'Изчакване' : b.is_profit ? 'Загубен фиш' : 'Спечелен фиш';
  var f = client.config.fields;
  var fields = {};
  fields[f.date] = b.slip_date || new Date().toISOString().slice(0,10);
  fields[f.slip] = b.matches && b.matches[0] ? b.matches[0].display || '' : '';
  fields[f.amount] = Number(b.stake) || 0;
  fields[f.odds] = Number(b.total_odds) || 1;
  fields[f.result] = atResult;
  fields[f.ourAmount] = Number(b.our_amount) || 0;

  atCreateRecord(client.config.tableId, fields)
    .then(function() {
      broadcast({ type: 'new_slip', client_id: b.client_id });
      res.json({ success: true });
    })
    .catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET live
app.get('/api/live', function(req, res) {
  var today = new Date().toISOString().split('T')[0];
  Promise.all([fetchNHL(today), fetchMLB(today), fetchESPN('basketball','nba',today)])
    .then(function(results) {
      var live = [];
      (results[0] || []).forEach(function(g) {
        if (g.gameState === 'LIVE' || g.gameState === 'CRIT') {
          live.push({ sport: 'НХЛ', home: g.homeTeam && g.homeTeam.abbrev, away: g.awayTeam && g.awayTeam.abbrev, homeScore: g.homeTeam && g.homeTeam.score || 0, awayScore: g.awayTeam && g.awayTeam.score || 0, status: 'LIVE' });
        } else if (g.gameState === 'FUT' || g.gameState === 'PRE') {
          live.push({ sport: 'НХЛ', home: g.homeTeam && g.homeTeam.abbrev, away: g.awayTeam && g.awayTeam.abbrev, homeScore: 0, awayScore: 0, status: 'UPCOMING' });
        }
      });
      (results[1] || []).forEach(function(g) {
        if (g.status && g.status.abstractGameState === 'Live') {
          live.push({ sport: 'МЛБ', home: g.teams && g.teams.home && g.teams.home.team && g.teams.home.team.abbreviation, away: g.teams && g.teams.away && g.teams.away.team && g.teams.away.team.abbreviation, homeScore: g.teams && g.teams.home && g.teams.home.score || 0, awayScore: g.teams && g.teams.away && g.teams.away.score || 0, status: 'LIVE' });
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
process.on('uncaughtException', function(e) { console.error('uncaughtException:', e.message); });
process.on('unhandledRejection', function(e) { console.error('unhandledRejection:', e && e.message); });

var PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('Server running on port ' + PORT);
});
