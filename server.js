'use strict';

// Load env vars first
try { require('dotenv').config(); } catch (e) {}

var express  = require('express');
var http     = require('http');
var ws       = require('ws');
var multer   = require('multer');
var axios    = require('axios');
var cors     = require('cors');
var path     = require('path');
var fs       = require('fs');

var app    = express();
var server = http.createServer(app);
var wss    = new ws.Server({ server: server });

// ── MIDDLEWARE ────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

var upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 20 * 1024 * 1024 } });

// ── WEBSOCKET ─────────────────────────────────────────────
wss.on('error', function () {});

function broadcast(obj) {
  var msg = JSON.stringify(obj);
  wss.clients.forEach(function (c) {
    try { if (c.readyState === 1) c.send(msg); } catch (e) {}
  });
}

// ── AIRTABLE CONFIG ───────────────────────────────────────
var AT_BASE = 'appil6CpexOd4trpk';

// Verified live from Airtable schema
var CLIENTS = {
  1: {
    id: 1, name: 'Румен', color: '#3b82f6',
    tableId: 'tblXlqTekjWKVUn4B',
    f: {
      date: 'fldreJGr0YY07Vetx', slip: 'fld8N7UCh2e4CnM92',
      stake: 'fldauOfHluYvw7VIv', odds: 'fld0JITwQ2OHpvlf8',
      potential: 'fldPq2kWFEm3nEf8M', result: 'fldBO6ChyLdbkFlBR',
      ourAmt: 'fldi7w9x5nC4QLJJE', notes: 'fldSlH5OXrNyK2XDZ'
    },
    win: 'Загубен фиш', loss: 'Спечелен фиш', pending: 'Изчакване'
  },
  2: {
    id: 2, name: 'Славчо', color: '#f59e0b',
    tableId: 'tblGX3x0IEDX4leTw',
    f: {
      date: 'fldwqoXiM6ou1SER2', slip: 'fldiMeTu91YT6MrJx',
      stake: 'fld6V0Sd3LywhqJNI', odds: 'fld7xU2zuQT9TialY',
      result: 'fld4NYoZDjbnwVxRz', ourAmt: 'fldZzzntT5S1gkp9u'
    },
    win: 'Загубен фиш', loss: 'Спечелен фиш', pending: 'Изчакване'
  },
  3: {
    id: 3, name: 'Близнаците', color: '#f97316',
    tableId: 'tblhPTRCtau0BNgHR',
    f: {
      date: 'fldhKzZVZFzrQ5hTT', slip: 'fldOPFY5eKulE7O8G',
      stake: 'fldbtqonGgcI4nAwo', odds: 'fldhLJbgTCFPTng42',
      result: 'fld6Z9Mq56S24ui14', ourAmt: 'fldOFNQg1vKOwKUoQ'
    },
    win: 'Загубен фиш', loss: 'Спечелен фиш', pending: 'Изчакване'
  },
  4: {
    id: 4, name: 'Доги', color: '#ef4444',
    tableId: 'tbl972ougIeRQ7om4',
    f: {
      date: 'fldghYrk4e3M1ZzZl', slip: 'fldwPfd0V6Zrg8zSK',
      stake: 'fldykfL5iKQ2pklIZ', odds: 'fldqam0jgWYX5qa8z',
      result: 'fld980USug9ArqAbo', ourAmt: 'fld16uXyYacBCAY3Q'
    },
    win: 'Загубен фиш', loss: 'Спечелен фиш', pending: 'Изчакване'
  }
};

function getClient(id) { return CLIENTS[Number(id)] || null; }
function clientList() { return [CLIENTS[1], CLIENTS[2], CLIENTS[3], CLIENTS[4]]; }

// ── AIRTABLE HTTP ─────────────────────────────────────────
function atHeaders() {
  return { 'Authorization': 'Bearer ' + process.env.AIRTABLE_KEY, 'Content-Type': 'application/json' };
}

function atGet(tableId) {
  return axios.get('https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '?maxRecords=500', {
    headers: atHeaders(), timeout: 15000
  }).then(function (r) { return r.data.records || []; })
    .catch(function (e) {
      var m = e.response ? JSON.stringify(e.response.data) : e.message;
      console.error('atGet error:', m);
      return [];
    });
}

function atCreate(tableId, fields) {
  return axios.post('https://api.airtable.com/v0/' + AT_BASE + '/' + tableId,
    { fields: fields, typecast: true }, { headers: atHeaders(), timeout: 15000 }
  ).then(function (r) { return r.data; })
    .catch(function (e) {
      var m = e.response ? JSON.stringify(e.response.data) : e.message;
      throw new Error(m);
    });
}

function atUpdate(tableId, recId, fields) {
  return axios.patch('https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '/' + recId,
    { fields: fields, typecast: true }, { headers: atHeaders(), timeout: 15000 }
  ).then(function (r) { return r.data; })
    .catch(function (e) {
      var m = e.response ? JSON.stringify(e.response.data) : e.message;
      throw new Error(m);
    });
}

function atDelete(tableId, recId) {
  return axios.delete('https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '/' + recId,
    { headers: atHeaders(), timeout: 15000 }
  ).then(function (r) { return r.data; })
    .catch(function (e) {
      var m = e.response ? JSON.stringify(e.response.data) : e.message;
      throw new Error(m);
    });
}

// ── BALANCE ───────────────────────────────────────────────
function calcBalance(records, client) {
  var total = 0;
  records.forEach(function (r) {
    var res = r.fields[client.f.result];
    var amt = Number(r.fields[client.f.ourAmt] || 0);
    if (res === client.win)  total += amt;
    if (res === client.loss) total -= amt;
  });
  return Math.round(total * 100) / 100;
}

// ── SPORTS API ────────────────────────────────────────────
function safeGet(url, opts) {
  return axios.get(url, Object.assign({ timeout: 6000 }, opts || {}))
    .then(function (r) { return r.data; })
    .catch(function () { return null; });
}

function norm(s) { return String(s || '').toLowerCase().replace(/[^a-z]/g, ''); }

function fetchESPN(sport, league, date) {
  return safeGet(
    'https://site.api.espn.com/apis/site/v2/sports/' + sport + '/' + league +
    '/scoreboard?dates=' + date.replace(/-/g, '')
  ).then(function (d) { return d && d.events ? d.events : []; });
}

function resolveGame(events, abbr1, abbr2, pick) {
  var u1 = String(abbr1 || '').toUpperCase();
  var u2 = String(abbr2 || '').toUpperCase();
  var n1 = norm(abbr1).slice(-4);
  var n2 = norm(abbr2).slice(-4);

  for (var i = 0; i < events.length; i++) {
    var comp = events[i].competitions && events[i].competitions[0];
    if (!comp) continue;
    var cs = comp.competitors || [];

    var hasT1 = cs.some(function (c) {
      var a = (c.team && c.team.abbreviation || '').toUpperCase();
      var n = norm(c.team && c.team.displayName || '');
      return a === u1 || n.indexOf(n1) >= 0 || n1.indexOf(n.slice(-4)) >= 0;
    });
    var hasT2 = cs.some(function (c) {
      var a = (c.team && c.team.abbreviation || '').toUpperCase();
      var n = norm(c.team && c.team.displayName || '');
      return a === u2 || n.indexOf(n2) >= 0 || n2.indexOf(n.slice(-4)) >= 0;
    });
    if (!hasT1 || !hasT2) continue;

    var st = comp.status && comp.status.type;
    if (!st) return { result: 'PENDING', score: '-' };
    if (!st.completed && st.state !== 'in') return { result: 'PENDING', score: '-' };
    if (st.state === 'in') return { result: 'LIVE', score: cs.map(function (c) { return c.score || '0'; }).join(':') };

    var winner = null;
    for (var j = 0; j < cs.length; j++) { if (cs[j].winner) { winner = cs[j]; break; } }
    if (!winner) return { result: 'PENDING', score: '?' };

    var wA = (winner.team && winner.team.abbreviation || '').toUpperCase();
    var wN = norm(winner.team && winner.team.displayName || '');
    var t1Won = wA === u1 || wN.indexOf(n1) >= 0 || n1.indexOf(wN.slice(-4)) >= 0;

    var s1 = '?', s2 = '?';
    for (var k = 0; k < cs.length; k++) {
      var ka = (cs[k].team && cs[k].team.abbreviation || '').toUpperCase();
      var kn = norm(cs[k].team && cs[k].team.displayName || '');
      if (ka === u1 || kn.indexOf(n1) >= 0) s1 = cs[k].score || '?';
      if (ka === u2 || kn.indexOf(n2) >= 0) s2 = cs[k].score || '?';
    }
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: s1 + ':' + s2 };
  }
  return null;
}

function resolveMatches(matches, slipDate) {
  if (!matches || !matches.length) return Promise.resolve([]);

  var base = slipDate ? new Date(slipDate) : new Date();
  var dates = [];
  for (var i = -5; i <= 2; i++) {
    var d = new Date(base.getTime());
    d.setDate(d.getDate() + i);
    dates.push(d.toISOString().split('T')[0]);
  }
  var today = new Date().toISOString().split('T')[0];
  if (dates.indexOf(today) < 0) dates.push(today);

  var sports = {};
  matches.forEach(function (m) { if (m.sport) sports[m.sport] = true; });

  // ESPN leagues per sport
  var leagueMap = {
    NHL:    [['hockey', 'nhl']],
    NBA:    [['basketball', 'nba']],
    NFL:    [['football', 'nfl']],
    MLB:    [['baseball', 'mlb']],
    SOCCER: [['soccer','eng.1'],['soccer','esp.1'],['soccer','ger.1'],
              ['soccer','ita.1'],['soccer','fra.1'],['soccer','uefa.champions']]
  };

  var fetches = [];
  var keys = [];
  dates.forEach(function (dt) {
    Object.keys(sports).forEach(function (sp) {
      var leagues = leagueMap[sp] || [];
      leagues.forEach(function (lg) {
        fetches.push(fetchESPN(lg[0], lg[1], dt));
        keys.push(sp + ':' + lg[1] + ':' + dt);
      });
    });
  });

  return Promise.all(fetches).then(function (results) {
    var cache = {};
    for (var i = 0; i < keys.length; i++) cache[keys[i]] = results[i] || [];

    return matches.map(function (m) {
      var sp = m.sport;
      var leagues = leagueMap[sp] || [];
      for (var di = 0; di < dates.length; di++) {
        for (var li = 0; li < leagues.length; li++) {
          var key = sp + ':' + leagues[li][1] + ':' + dates[di];
          var r = resolveGame(cache[key] || [], m.team1abbr || m.team1, m.team2abbr || m.team2, m.pick);
          if (r) return Object.assign({}, m, r);
        }
      }
      return Object.assign({}, m, { result: 'PENDING', score: 'N/F' });
    });
  });
}

// ── GPT-4o ────────────────────────────────────────────────
function readSlip(buf) {
  var b64 = buf.toString('base64');
  var prompt =
    'Read this bet365 slip. Return ONLY valid JSON:\n' +
    '{"matches":[{"display":"text","team1":"English name","team1abbr":"CAR","team2":"English name","team2abbr":"MTL","pick":"1","odds":1.5,"sport":"NHL"}],' +
    '"stake":150,"totalOdds":3.81,"potentialWin":571.5,"slipDate":"2026-05-22"}\n' +
    'sport = NHL, MLB, NBA, NFL, or SOCCER only. slipDate = YYYY-MM-DD. Return ONLY JSON.';

  return axios.post('https://api.openai.com/v1/chat/completions', {
    model: 'gpt-4o',
    max_tokens: 1200,
    messages: [{ role: 'user', content: [
      { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + b64, detail: 'high' } },
      { type: 'text', text: prompt }
    ]}]
  }, { headers: { 'Authorization': 'Bearer ' + process.env.OPENAI_API_KEY, 'Content-Type': 'application/json' }, timeout: 30000 })
    .then(function (res) {
      var text = res.data.choices[0].message.content;
      var clean = text.replace(/[\u0060]{3}[a-z]*\n?/gi, '').replace(/[\u0060]{3}/g, '').trim();
      try { return JSON.parse(clean); }
      catch (e) {
        var m = clean.match(/\{[\s\S]*\}/);
        if (m) return JSON.parse(m[0]);
        throw new Error('GPT-4o върна невалиден JSON');
      }
    });
}

// ── ROUTES ────────────────────────────────────────────────

app.get('/api/ping', function (req, res) {
  res.json({ ok: true, airtable: !!process.env.AIRTABLE_KEY, openai: !!process.env.OPENAI_API_KEY });
});

app.get('/api/clients', function (req, res) {
  Promise.all(clientList().map(function (client) {
    return atGet(client.tableId).then(function (records) {
      var bal = calcBalance(records, client);
      var wins = 0, losses = 0;
      records.forEach(function (r) {
        var v = r.fields[client.f.result];
        if (v === client.win)  wins++;
        if (v === client.loss) losses++;
      });
      return { id: client.id, name: client.name, color: client.color,
               balance: bal, wins: wins, losses: losses, total_slips: records.length };
    }).catch(function () {
      return { id: client.id, name: client.name, color: client.color, balance: 0, wins: 0, losses: 0, total_slips: 0 };
    });
  })).then(function (data) { res.json(data); })
     .catch(function (e) { res.status(500).json({ error: e.message }); });
});

app.get('/api/clients/:id/slips', function (req, res) {
  var client = getClient(req.params.id);
  if (!client) return res.status(404).json({ error: 'Not found' });
  atGet(client.tableId).then(function (records) {
    res.json(records.map(function (r) {
      var f = r.fields;
      var result = f[client.f.result] || '';
      var isProfit = result === client.win;
      var isPending = result === client.pending || result === '';
      return {
        id: r.id, tableId: client.tableId, clientId: client.id,
        slip_date: f[client.f.date] || '',
        stake: Number(f[client.f.stake] || 0),
        total_odds: Number(f[client.f.odds] || 1),
        result: isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN',
        our_amount: Number(f[client.f.ourAmt] || 0),
        is_profit: isProfit,
        matches: [{ display: f[client.f.slip] || '' }]
      };
    }));
  }).catch(function (e) { res.status(500).json({ error: e.message }); });
});

app.get('/api/clients/:id/chart', function (req, res) {
  var client = getClient(req.params.id);
  if (!client) return res.json([]);
  atGet(client.tableId).then(function (records) {
    var byDate = {};
    records.forEach(function (r) {
      var date = r.fields[client.f.date] || new Date().toISOString().slice(0, 10);
      var result = r.fields[client.f.result] || '';
      var amt = Number(r.fields[client.f.ourAmt] || 0);
      if (!byDate[date]) byDate[date] = 0;
      if (result === client.win)  byDate[date] += amt;
      if (result === client.loss) byDate[date] -= amt;
    });
    res.json(Object.keys(byDate).sort().slice(-14).map(function (d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    }));
  }).catch(function () { res.json([]); });
});

app.get('/api/chart', function (req, res) {
  Promise.all(clientList().map(function (client) {
    return atGet(client.tableId).then(function (records) { return { client: client, records: records }; })
           .catch(function () { return { client: client, records: [] }; });
  })).then(function (all) {
    var byDate = {};
    all.forEach(function (item) {
      item.records.forEach(function (r) {
        var date = r.fields[item.client.f.date] || new Date().toISOString().slice(0, 10);
        var result = r.fields[item.client.f.result] || '';
        var amt = Number(r.fields[item.client.f.ourAmt] || 0);
        if (!byDate[date]) byDate[date] = 0;
        if (result === item.client.win)  byDate[date] += amt;
        if (result === item.client.loss) byDate[date] -= amt;
      });
    });
    res.json(Object.keys(byDate).sort().slice(-14).map(function (d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    }));
  }).catch(function () { res.json([]); });
});

app.get('/api/exposure', function (req, res) { res.json([]); });

app.post('/api/analyze', upload.single('image'), function (req, res) {
  if (!req.file) return res.status(400).json({ error: 'Няма снимка' });
  var clientId = req.body && req.body.client_id;
  var client = getClient(clientId);
  if (!client) return res.status(400).json({ error: 'Невалиден клиент' });

  readSlip(req.file.buffer).then(function (slip) {
    var slipDate = slip.slipDate || new Date().toISOString().split('T')[0];
    return resolveMatches(slip.matches || [], slipDate).then(function (resolved) {
      var anyLoss   = resolved.some(function (m) { return m.result === 'LOSS'; });
      var allWin    = resolved.length > 0 && resolved.every(function (m) { return m.result === 'WIN'; });
      var isProfit  = anyLoss;
      var isPending = !anyLoss && !allWin;
      var ourAmount = isPending ? 0 : isProfit ? Number(slip.stake || 0) : Number(slip.potentialWin || 0);
      var atResult  = isPending ? client.pending : isProfit ? client.win : client.loss;

      var matchText = resolved.map(function (m) { return m.display || m.team1 + ' - ' + m.team2; }).join(' · ');

      var fields = {};
      fields[client.f.date]   = slipDate;
      fields[client.f.slip]   = matchText;
      fields[client.f.stake]  = Number(slip.stake || 0);
      fields[client.f.odds]   = Number(slip.totalOdds || 1);
      fields[client.f.result] = atResult;
      fields[client.f.ourAmt] = ourAmount;
      if (client.f.potential) fields[client.f.potential] = Number(slip.potentialWin || 0);
      if (client.f.notes)     fields[client.f.notes]     = resolved.map(function (m) { return (m.display || '') + (m.score && m.score !== 'N/F' ? ' ' + m.score : ''); }).join(' | ');

      return atCreate(client.tableId, fields).then(function (rec) {
        broadcast({ type: 'new_slip', client_id: client.id });
        res.json({
          success: true,
          atId: rec.id, tableId: client.tableId, clientId: client.id,
          slip: { stake: slip.stake, totalOdds: slip.totalOdds, potentialWin: slip.potentialWin },
          matches: resolved,
          isProfit: isProfit, isPending: isPending, ourAmount: ourAmount,
          result: isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN',
          slipDate: slipDate
        });
      });
    });
  }).catch(function (e) {
    console.error('analyze:', e.message);
    res.status(500).json({ error: e.message });
  });
});

// PATCH slip — atId, tableId, clientId all come from frontend
app.patch('/api/slips/:atId', function (req, res) {
  var b = req.body || {};
  var atId    = req.params.atId;
  var tableId = b.tableId;
  var client  = getClient(b.clientId);
  if (!tableId || !client) return res.json({ success: true });

  var atResult = b.isPending ? client.pending : b.isProfit ? client.win : client.loss;
  var fields = {};
  fields[client.f.result] = atResult;
  fields[client.f.ourAmt] = Number(b.ourAmount || 0);

  atUpdate(tableId, atId, fields).then(function () {
    broadcast({ type: 'slip_updated' });
    res.json({ success: true });
  }).catch(function (e) { res.status(500).json({ error: e.message }); });
});

// DELETE slip
app.delete('/api/slips/:atId', function (req, res) {
  var tableId  = req.query.tableId;
  var atId     = req.params.atId;
  if (!tableId) return res.status(400).json({ error: 'tableId required' });
  atDelete(tableId, atId).then(function () {
    broadcast({ type: 'slip_deleted' });
    res.json({ success: true });
  }).catch(function (e) { res.status(500).json({ error: e.message }); });
});

// Manual slip
app.post('/api/slips/manual', function (req, res) {
  var b = req.body || {};
  var client = getClient(b.client_id);
  if (!client) return res.status(400).json({ error: 'Невалиден клиент' });

  var isProfit  = b.result === 'LOSS';
  var isPending = b.result === 'PENDING';
  var atResult  = isPending ? client.pending : isProfit ? client.win : client.loss;

  var fields = {};
  fields[client.f.date]   = b.slip_date || new Date().toISOString().slice(0, 10);
  fields[client.f.slip]   = b.match_text || '';
  fields[client.f.stake]  = Number(b.stake || 0);
  fields[client.f.odds]   = Number(b.odds || 1);
  fields[client.f.result] = atResult;
  fields[client.f.ourAmt] = Number(b.our_amount || 0);

  atCreate(client.tableId, fields).then(function () {
    broadcast({ type: 'new_slip', client_id: client.id });
    res.json({ success: true });
  }).catch(function (e) { res.status(500).json({ error: e.message }); });
});

// Live scores
app.get('/api/live', function (req, res) {
  var today = new Date().toISOString().split('T')[0];
  Promise.all([
    fetchESPN('hockey', 'nhl', today),
    fetchESPN('basketball', 'nba', today),
    fetchESPN('baseball', 'mlb', today)
  ]).then(function (results) {
    var live = [];
    [['НХЛ', results[0]], ['НБА', results[1]], ['МЛБ', results[2]]].forEach(function (pair) {
      var sport = pair[0], events = pair[1];
      (events || []).forEach(function (ev) {
        var comp = ev.competitions && ev.competitions[0];
        if (!comp) return;
        var st = comp.status && comp.status.type;
        var cs = comp.competitors || [];
        var home = cs.find(function (c) { return c.homeAway === 'home'; });
        var away = cs.find(function (c) { return c.homeAway === 'away'; });
        if (!st || !home || !away) return;
        if (st.state === 'in') {
          live.push({ sport: sport, status: 'LIVE',
            home: home.team && home.team.abbreviation || '',
            away: away.team && away.team.abbreviation || '',
            homeScore: home.score || '0', awayScore: away.score || '0' });
        } else if (!st.completed && st.state === 'pre') {
          live.push({ sport: sport, status: 'UPCOMING',
            home: home.team && home.team.abbreviation || '',
            away: away.team && away.team.abbreviation || '',
            homeScore: '0', awayScore: '0' });
        }
      });
    });
    res.json({ live: live });
  }).catch(function (e) { res.status(500).json({ error: e.message }); });
});

// Serve frontend
app.get('*', function (req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── GLOBAL ERROR HANDLERS ─────────────────────────────────
process.on('uncaughtException',  function (e) { console.error('uncaughtException:', e.message); });
process.on('unhandledRejection', function (e) { console.error('unhandledRejection:', e && e.message); });

// ── START ─────────────────────────────────────────────────
var PORT = process.env.PORT || 3000;
server.listen(PORT, function () {
  console.log('Server started on port', PORT);
  console.log('AIRTABLE_KEY:', !!process.env.AIRTABLE_KEY);
  console.log('OPENAI_API_KEY:', !!process.env.OPENAI_API_KEY);
});
