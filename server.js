'use strict';

var express    = require('express');
var http       = require('http');
var WebSocket  = require('ws');
var multer     = require('multer');
var axios      = require('axios');
var cors       = require('cors');
var path       = require('path');
var fs         = require('fs');

// ─── APP SETUP ───────────────────────────────────────────────────────────────
var app    = express();
var server = http.createServer(app);
var wss    = new WebSocket.Server({ server: server });

wss.on('error', function() {});

app.use(cors());
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(express.static(path.join(__dirname, 'public')));

var upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }
});

// ─── CONSTANTS ───────────────────────────────────────────────────────────────
var AT_BASE = 'appil6CpexOd4trpk';

// Tested live - all 4 tables accept CREATE/UPDATE/DELETE
var CLIENTS = [
  {
    id: 1, name: 'Румен', color: '#3b82f6',
    tableId: 'tblXlqTekjWKVUn4B',
    f: {
      date:      'fldreJGr0YY07Vetx',
      slip:      'fld8N7UCh2e4CnM92',
      stake:     'fldauOfHluYvw7VIv',
      odds:      'fld0JITwQ2OHpvlf8',
      potential: 'fldPq2kWFEm3nEf8M',
      result:    'fldBO6ChyLdbkFlBR',
      ourAmt:    'fldi7w9x5nC4QLJJE',
      notes:     'fldSlH5OXrNyK2XDZ'
    }
  },
  {
    id: 2, name: 'Славчо', color: '#f59e0b',
    tableId: 'tblGX3x0IEDX4leTw',
    f: {
      date:   'fldwqoXiM6ou1SER2',
      slip:   'fldiMeTu91YT6MrJx',
      stake:  'fld6V0Sd3LywhqJNI',
      odds:   'fld7xU2zuQT9TialY',
      result: 'fld4NYoZDjbnwVxRz',
      ourAmt: 'fldZzzntT5S1gkp9u'
    }
  },
  {
    id: 3, name: 'Близнаците', color: '#f97316',
    tableId: 'tblhPTRCtau0BNgHR',
    f: {
      date:   'fldhKzZVZFzrQ5hTT',
      slip:   'fldOPFY5eKulE7O8G',
      stake:  'fldbtqonGgcI4nAwo',
      odds:   'fldhLJbgTCFPTng42',
      result: 'fld6Z9Mq56S24ui14',
      ourAmt: 'fldOFNQg1vKOwKUoQ'
    }
  },
  {
    id: 4, name: 'Доги', color: '#ef4444',
    tableId: 'tbl972ougIeRQ7om4',
    f: {
      date:   'fldghYrk4e3M1ZzZl',
      slip:   'fldwPfd0V6Zrg8zSK',
      stake:  'fldykfL5iKQ2pklIZ',
      odds:   'fldqam0jgWYX5qa8z',
      result: 'fld980USug9ArqAbo',
      ourAmt: 'fld16uXyYacBCAY3Q'
    }
  }
];

// Airtable select field values (tested live)
var AT_WIN     = 'Загубен фиш';   // фишът загубен = НИЕ ПЕЧЕЛИМ
var AT_LOSS    = 'Спечелен фиш';  // фишът спечелен = НИЕ ГУБИМ
var AT_PENDING = 'Изчакване';

// In-memory: maps local slipId -> {atId, tableId, clientId}
var slipMap = {};
var slipCounter = 1;

// ─── HELPERS ─────────────────────────────────────────────────────────────────
function getClient(id) {
  var n = Number(id);
  for (var i = 0; i < CLIENTS.length; i++) {
    if (CLIENTS[i].id === n) return CLIENTS[i];
  }
  return null;
}

function broadcast(obj) {
  try {
    var msg = JSON.stringify(obj);
    wss.clients.forEach(function(c) {
      try { if (c.readyState === 1) c.send(msg); } catch(e) {}
    });
  } catch(e) {}
}

function norm(s) {
  return String(s || '').toLowerCase().replace(/[^a-z]/g, '');
}

function calcBalance(records, client) {
  var total = 0;
  for (var i = 0; i < records.length; i++) {
    var r = records[i];
    var result = r.fields[client.f.result];
    var amt    = Number(r.fields[client.f.ourAmt] || 0);
    if (result === AT_WIN)  total += amt;
    if (result === AT_LOSS) total -= amt;
  }
  return Math.round(total * 100) / 100;
}

// ─── AIRTABLE API ─────────────────────────────────────────────────────────────
function atHeaders() {
  return {
    'Authorization': 'Bearer ' + process.env.AIRTABLE_KEY,
    'Content-Type':  'application/json'
  };
}

function atGet(tableId) {
  return axios({
    method:  'GET',
    url:     'https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '?maxRecords=500',
    headers: atHeaders(),
    timeout: 15000
  }).then(function(r) {
    return r.data.records || [];
  }).catch(function(e) {
    var msg = e.response ? JSON.stringify(e.response.data) : e.message;
    console.error('atGet error [' + tableId + ']:', msg);
    return [];
  });
}

function atCreate(tableId, fields) {
  return axios({
    method:  'POST',
    url:     'https://api.airtable.com/v0/' + AT_BASE + '/' + tableId,
    headers: atHeaders(),
    data:    { fields: fields, typecast: true },
    timeout: 15000
  }).then(function(r) {
    return r.data;
  }).catch(function(e) {
    var msg = e.response ? JSON.stringify(e.response.data) : e.message;
    console.error('atCreate error:', msg);
    throw new Error(msg);
  });
}

function atUpdate(tableId, recId, fields) {
  return axios({
    method:  'PATCH',
    url:     'https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '/' + recId,
    headers: atHeaders(),
    data:    { fields: fields, typecast: true },
    timeout: 15000
  }).then(function(r) {
    return r.data;
  }).catch(function(e) {
    var msg = e.response ? JSON.stringify(e.response.data) : e.message;
    console.error('atUpdate error:', msg);
    throw new Error(msg);
  });
}

function atDelete(tableId, recId) {
  return axios({
    method:  'DELETE',
    url:     'https://api.airtable.com/v0/' + AT_BASE + '/' + tableId + '/' + recId,
    headers: { 'Authorization': 'Bearer ' + process.env.AIRTABLE_KEY },
    timeout: 15000
  }).then(function(r) {
    return r.data;
  }).catch(function(e) {
    var msg = e.response ? JSON.stringify(e.response.data) : e.message;
    console.error('atDelete error:', msg);
    throw new Error(msg);
  });
}

// ─── SPORTS APIs ──────────────────────────────────────────────────────────────
function safeGet(url, opts) {
  return axios.get(url, Object.assign({ timeout: 6000 }, opts || {}))
    .then(function(r) { return r.data; })
    .catch(function() { return null; });
}

function fetchNHL(date) {
  return safeGet('https://api-web.nhle.com/v1/score/' + date)
    .then(function(d) { return (d && d.games) ? d.games : []; });
}

function fetchMLB(date) {
  return safeGet(
    'https://statsapi.mlb.com/api/v1/schedule?sportId=1&date=' + date + '&hydrate=linescore'
  ).then(function(d) {
    return (d && d.dates && d.dates[0]) ? d.dates[0].games : [];
  });
}

function fetchESPN(sport, league, date) {
  return safeGet(
    'https://site.api.espn.com/apis/site/v2/sports/' + sport + '/' + league +
    '/scoreboard?dates=' + date.replace(/-/g, '')
  ).then(function(d) { return (d && d.events) ? d.events : []; });
}

function fetchFootball(date) {
  if (!process.env.SPORTS_API_KEY) return Promise.resolve([]);
  return safeGet('https://v3.football.api-sports.io/fixtures', {
    headers: { 'x-apisports-key': process.env.SPORTS_API_KEY },
    params:  { date: date }
  }).then(function(d) { return (d && d.response) ? d.response : []; });
}

// ─── RESOLVE FUNCTIONS ────────────────────────────────────────────────────────
function resolveNHL(games, a1, a2, pick) {
  var u1 = String(a1 || '').toUpperCase();
  var u2 = String(a2 || '').toUpperCase();
  for (var i = 0; i < games.length; i++) {
    var g    = games[i];
    var home = g.homeTeam && g.homeTeam.abbrev ? g.homeTeam.abbrev.toUpperCase() : '';
    var away = g.awayTeam && g.awayTeam.abbrev ? g.awayTeam.abbrev.toUpperCase() : '';
    if (!((home === u1 || away === u1) && (home === u2 || away === u2))) continue;
    var st = g.gameState || '';
    if (st === 'FUT' || st === 'PRE') return { result: 'PENDING', score: '-' };
    var hs = Number((g.homeTeam && g.homeTeam.score) || 0);
    var as = Number((g.awayTeam && g.awayTeam.score) || 0);
    var awayIsT1 = away === u1;
    var sc = (awayIsT1 ? as : hs) + ':' + (awayIsT1 ? hs : as);
    if (st === 'LIVE' || st === 'CRIT') return { result: 'LIVE', score: sc };
    var t1Won = awayIsT1 ? as > hs : hs > as;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc };
  }
  return null;
}

function resolveMLB(games, t1, t2, pick) {
  var n1 = norm(t1).slice(-5);
  var n2 = norm(t2).slice(-5);
  for (var i = 0; i < games.length; i++) {
    var g  = games[i];
    var aN = norm(g.teams && g.teams.away && g.teams.away.team ? g.teams.away.team.name : '');
    var hN = norm(g.teams && g.teams.home && g.teams.home.team ? g.teams.home.team.name : '');
    var fwd = (aN.indexOf(n1) >= 0 || n1.indexOf(aN.slice(-4)) >= 0) &&
              (hN.indexOf(n2) >= 0 || n2.indexOf(hN.slice(-4)) >= 0);
    var rev = (aN.indexOf(n2) >= 0 || n2.indexOf(aN.slice(-4)) >= 0) &&
              (hN.indexOf(n1) >= 0 || n1.indexOf(hN.slice(-4)) >= 0);
    if (!fwd && !rev) continue;
    var st = g.status ? g.status.abstractGameState : '';
    if (st === 'Preview') return { result: 'PENDING', score: '-' };
    var as2 = Number((g.teams && g.teams.away && g.teams.away.score) || 0);
    var hs2 = Number((g.teams && g.teams.home && g.teams.home.score) || 0);
    var ls  = g.linescore;
    if (!as2 && ls && ls.teams && ls.teams.away) as2 = Number(ls.teams.away.runs || 0);
    if (!hs2 && ls && ls.teams && ls.teams.home) hs2 = Number(ls.teams.home.runs || 0);
    var awayIsT1 = fwd;
    var sc2 = (awayIsT1 ? as2 : hs2) + ':' + (awayIsT1 ? hs2 : as2);
    if (st === 'Live') return { result: 'LIVE', score: sc2 };
    var t1Won = awayIsT1 ? as2 > hs2 : hs2 > as2;
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: sc2 };
  }
  return null;
}

function resolveESPN(events, t1, t2, pick) {
  var n1 = norm(t1).slice(-4);
  var n2 = norm(t2).slice(-4);
  for (var i = 0; i < events.length; i++) {
    var ev   = events[i];
    var comp = ev.competitions && ev.competitions[0];
    if (!comp) continue;
    var cs    = comp.competitors || [];
    var hasT1 = cs.some(function(c) {
      var nm = norm(c.team ? (c.team.displayName || '') : '');
      return nm.indexOf(n1) >= 0 || n1.indexOf(nm.slice(-4)) >= 0;
    });
    var hasT2 = cs.some(function(c) {
      var nm = norm(c.team ? (c.team.displayName || '') : '');
      return nm.indexOf(n2) >= 0 || n2.indexOf(nm.slice(-4)) >= 0;
    });
    if (!hasT1 || !hasT2) continue;
    var st = comp.status && comp.status.type;
    if (!st) return { result: 'PENDING', score: '-' };
    if (!st.completed && st.state !== 'in') return { result: 'PENDING', score: '-' };
    if (st.state === 'in') {
      return { result: 'LIVE', score: cs.map(function(c) { return c.score || '0'; }).join(':') };
    }
    var winner = null;
    for (var j = 0; j < cs.length; j++) { if (cs[j].winner) { winner = cs[j]; break; } }
    if (!winner) return { result: 'PENDING', score: '?' };
    var wn   = norm(winner.team ? (winner.team.displayName || '') : '');
    var t1Won = wn.indexOf(n1) >= 0 || n1.indexOf(wn.slice(-4)) >= 0;
    var s1 = '?', s2 = '?';
    for (var k = 0; k < cs.length; k++) {
      var cnm = norm(cs[k].team ? (cs[k].team.displayName || '') : '');
      if (cnm.indexOf(n1) >= 0 || n1.indexOf(cnm.slice(-4)) >= 0) s1 = cs[k].score || '?';
      if (cnm.indexOf(n2) >= 0 || n2.indexOf(cnm.slice(-4)) >= 0) s2 = cs[k].score || '?';
    }
    return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS', score: s1 + ':' + s2 };
  }
  return null;
}

function resolveFootball(fixtures, t1, t2, pick) {
  var n1 = norm(t1).slice(-4);
  var n2 = norm(t2).slice(-4);
  for (var i = 0; i < fixtures.length; i++) {
    var fx = fixtures[i];
    var ht = norm(fx.teams && fx.teams.home ? fx.teams.home.name : '');
    var at = norm(fx.teams && fx.teams.away ? fx.teams.away.name : '');
    var fwd = (ht.indexOf(n1) >= 0 || n1.indexOf(ht.slice(-4)) >= 0) &&
              (at.indexOf(n2) >= 0 || n2.indexOf(at.slice(-4)) >= 0);
    var rev = (ht.indexOf(n2) >= 0 || n2.indexOf(ht.slice(-4)) >= 0) &&
              (at.indexOf(n1) >= 0 || n1.indexOf(at.slice(-4)) >= 0);
    if (!fwd && !rev) continue;
    var st = fx.fixture && fx.fixture.status ? fx.fixture.status.short : '';
    if (st === 'NS' || st === 'TBD') return { result: 'PENDING', score: '-' };
    if (['1H','HT','2H','ET','BT','P'].indexOf(st) >= 0) {
      var hg0 = fx.goals ? Number(fx.goals.home || 0) : 0;
      var ag0 = fx.goals ? Number(fx.goals.away || 0) : 0;
      return { result: 'LIVE', score: hg0 + ':' + ag0 };
    }
    if (st === 'FT' || st === 'AET' || st === 'PEN') {
      var hg = fx.goals ? Number(fx.goals.home || 0) : 0;
      var ag = fx.goals ? Number(fx.goals.away || 0) : 0;
      var t1Won = fwd ? hg > ag : ag > hg;
      return { result: (pick === '1' ? t1Won : !t1Won) ? 'WIN' : 'LOSS',
               score: fwd ? hg + ':' + ag : ag + ':' + hg };
    }
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
  matches.forEach(function(m) { if (m.sport) sports[m.sport] = true; });

  var fetches = [];
  var keys    = [];

  dates.forEach(function(dt) {
    if (sports['NHL']) {
      fetches.push(fetchNHL(dt)); keys.push('NHL:' + dt);
    }
    if (sports['MLB']) {
      fetches.push(fetchMLB(dt)); keys.push('MLB:' + dt);
    }
    if (sports['NBA']) {
      fetches.push(fetchESPN('basketball', 'nba', dt)); keys.push('NBA:' + dt);
    }
    if (sports['NFL']) {
      fetches.push(fetchESPN('football', 'nfl', dt)); keys.push('NFL:' + dt);
    }
    if (sports['SOCCER']) {
      fetches.push(fetchFootball(dt)); keys.push('SOC:' + dt);
      ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'].forEach(function(lg) {
        fetches.push(fetchESPN('soccer', lg, dt));
        keys.push('SC:' + lg + ':' + dt);
      });
    }
  });

  return Promise.all(fetches).then(function(results) {
    var cache = {};
    for (var i = 0; i < keys.length; i++) cache[keys[i]] = results[i] || [];

    return matches.map(function(m) {
      for (var di = 0; di < dates.length; di++) {
        var dt = dates[di];
        var r  = null;
        if (m.sport === 'NHL') {
          r = resolveNHL(cache['NHL:'+dt] || [], m.team1abbr, m.team2abbr, m.pick);
        } else if (m.sport === 'MLB') {
          r = resolveMLB(cache['MLB:'+dt] || [], m.team1, m.team2, m.pick);
        } else if (m.sport === 'NBA') {
          r = resolveESPN(cache['NBA:'+dt] || [], m.team1, m.team2, m.pick);
        } else if (m.sport === 'NFL') {
          r = resolveESPN(cache['NFL:'+dt] || [], m.team1, m.team2, m.pick);
        } else if (m.sport === 'SOCCER') {
          r = resolveFootball(cache['SOC:'+dt] || [], m.team1, m.team2, m.pick);
          if (!r) {
            var lgs = ['eng.1','esp.1','ger.1','ita.1','fra.1','uefa.champions'];
            for (var li = 0; li < lgs.length; li++) {
              r = resolveESPN(cache['SC:'+lgs[li]+':'+dt] || [], m.team1, m.team2, m.pick);
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

// ─── GPT-4o ───────────────────────────────────────────────────────────────────
function readSlip(buf) {
  var b64 = buf.toString('base64');
  var prompt =
    'Read this bet365 betting slip carefully.\n' +
    'Return ONLY this exact JSON format, no other text:\n' +
    '{"matches":[{"display":"shown text","team1":"English name","team1abbr":"CAR",' +
    '"team2":"English name","team2abbr":"MTL","pick":"1","odds":1.50,"sport":"NHL"}],' +
    '"stake":150,"totalOdds":3.81,"potentialWin":571.5,"slipDate":"2026-05-22"}\n' +
    'Rules:\n' +
    '- sport: NHL, MLB, NBA, NFL, or SOCCER only\n' +
    '- slipDate: extract from slip as YYYY-MM-DD, if not visible use today\n' +
    '- pick "1" = first team to win, "2" = second team to win\n' +
    '- Return ONLY the JSON object';

  return axios.post('https://api.openai.com/v1/chat/completions', {
    model: 'gpt-4o',
    max_tokens: 1500,
    messages: [{
      role: 'user',
      content: [
        { type: 'image_url', image_url: { url: 'data:image/jpeg;base64,' + b64, detail: 'high' } },
        { type: 'text',      text: prompt }
      ]
    }]
  }, {
    headers:  { 'Authorization': 'Bearer ' + process.env.OPENAI_API_KEY, 'Content-Type': 'application/json' },
    timeout:  30000
  }).then(function(res) {
    var text  = res.data.choices[0].message.content;
    var clean = text.replace(/[\u0060]{3}[a-z]*\n?/gi, '').replace(/[\u0060]{3}/g, '').trim();
    try {
      return JSON.parse(clean);
    } catch(e) {
      var m = clean.match(/\{[\s\S]*\}/);
      if (m) return JSON.parse(m[0]);
      throw new Error('GPT-4o response not valid JSON: ' + clean.slice(0, 200));
    }
  });
}

// ─── ROUTES ──────────────────────────────────────────────────────────────────

// GET /api/clients
app.get('/api/clients', function(req, res) {
  var promises = CLIENTS.map(function(client) {
    return atGet(client.tableId).then(function(records) {
      var balance = calcBalance(records, client);
      var wins    = 0, losses = 0;
      records.forEach(function(r) {
        var result = r.fields[client.f.result];
        if (result === AT_WIN)  wins++;
        if (result === AT_LOSS) losses++;
      });
      return {
        id:          client.id,
        name:        client.name,
        color:       client.color,
        balance:     balance,
        total:       balance,
        wins:        wins,
        losses:      losses,
        total_slips: records.length
      };
    }).catch(function(e) {
      console.error('GET clients error for', client.name, e.message);
      return { id: client.id, name: client.name, color: client.color,
               balance: 0, total: 0, wins: 0, losses: 0, total_slips: 0 };
    });
  });

  Promise.all(promises)
    .then(function(data) { res.json(data); })
    .catch(function(e)   { res.status(500).json({ error: e.message }); });
});

// GET /api/clients/:id/slips
app.get('/api/clients/:id/slips', function(req, res) {
  var client = getClient(req.params.id);
  if (!client) return res.status(404).json({ error: 'Client not found' });

  atGet(client.tableId).then(function(records) {
    var slips = records.map(function(r) {
      var f      = r.fields;
      var result = f[client.f.result] || '';
      var isP    = result === AT_WIN;
      var isPend = result === AT_PENDING || result === '';
      return {
        id:          r.id,
        client_id:   client.id,
        slip_date:   f[client.f.date] || '',
        stake:       Number(f[client.f.stake] || 0),
        total_odds:  Number(f[client.f.odds]  || 1),
        result:      isPend ? 'PENDING' : isP ? 'LOSS' : 'WIN',
        our_amount:  Number(f[client.f.ourAmt] || 0),
        is_profit:   isP,
        matches:     [{ display: f[client.f.slip] || '—' }],
        image_path:  null
      };
    });
    res.json(slips);
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET /api/clients/:id/chart
app.get('/api/clients/:id/chart', function(req, res) {
  var client = getClient(req.params.id);
  if (!client) return res.json([]);

  atGet(client.tableId).then(function(records) {
    var byDate = {};
    records.forEach(function(r) {
      var date   = r.fields[client.f.date] || new Date().toISOString().slice(0, 10);
      var result = r.fields[client.f.result] || '';
      var amt    = Number(r.fields[client.f.ourAmt] || 0);
      if (!byDate[date]) byDate[date] = 0;
      if (result === AT_WIN)  byDate[date] += amt;
      if (result === AT_LOSS) byDate[date] -= amt;
    });
    var data = Object.keys(byDate).sort().slice(-14).map(function(d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    });
    res.json(data);
  }).catch(function() { res.json([]); });
});

// GET /api/chart
app.get('/api/chart', function(req, res) {
  var promises = CLIENTS.map(function(client) {
    return atGet(client.tableId)
      .then(function(records) { return { records: records, client: client }; })
      .catch(function()       { return { records: [],  client: client }; });
  });

  Promise.all(promises).then(function(all) {
    var byDate = {};
    all.forEach(function(item) {
      item.records.forEach(function(r) {
        var date   = r.fields[item.client.f.date] || new Date().toISOString().slice(0, 10);
        var result = r.fields[item.client.f.result] || '';
        var amt    = Number(r.fields[item.client.f.ourAmt] || 0);
        if (!byDate[date]) byDate[date] = 0;
        if (result === AT_WIN)  byDate[date] += amt;
        if (result === AT_LOSS) byDate[date] -= amt;
      });
    });
    var data = Object.keys(byDate).sort().slice(-14).map(function(d) {
      return { date: d, amount: Math.round(byDate[d] * 100) / 100 };
    });
    res.json(data);
  }).catch(function() { res.json([]); });
});

// GET /api/exposure
app.get('/api/exposure', function(req, res) { res.json([]); });

// POST /api/analyze
app.post('/api/analyze', upload.single('image'), function(req, res) {
  if (!req.file) return res.status(400).json({ error: 'Няма снимка' });

  var clientId = req.body && req.body.client_id;
  if (!clientId) return res.status(400).json({ error: 'Няма клиент' });

  var client = getClient(clientId);
  if (!client) return res.status(400).json({ error: 'Клиентът не е намерен' });

  console.log('Analyze: client=' + client.name + ', file=' + req.file.size + 'b');

  readSlip(req.file.buffer).then(function(slip) {
    console.log('GPT-4o parsed slip, matches:', slip.matches && slip.matches.length);

    var slipDate = slip.slipDate || new Date().toISOString().split('T')[0];
    var matches  = slip.matches || [];

    return resolveMatches(matches, slipDate).then(function(resolved) {
      var anyLoss = resolved.some(function(m)  { return m.result === 'LOSS'; });
      var allWin  = resolved.length > 0 && resolved.every(function(m) { return m.result === 'WIN'; });
      var isProfit  = anyLoss;
      var isPending = !anyLoss && !allWin;
      var ourAmount = isPending ? 0 : isProfit ? Number(slip.stake || 0) : Number(slip.potentialWin || 0);
      var atResult  = isPending ? AT_PENDING : isProfit ? AT_WIN : AT_LOSS;

      console.log('Result: isProfit=' + isProfit + ', isPending=' + isPending + ', ourAmount=' + ourAmount);

      // Save image
      var imgPath = null;
      try {
        var imgDir  = path.join(__dirname, 'public', 'uploads');
        if (!fs.existsSync(imgDir)) fs.mkdirSync(imgDir, { recursive: true });
        var imgName = 'slip_' + Date.now() + '.jpg';
        fs.writeFileSync(path.join(imgDir, imgName), req.file.buffer);
        imgPath = '/uploads/' + imgName;
      } catch(e) { console.error('Image save error:', e.message); }

      // Build Airtable fields
      var matchText = resolved.map(function(m) {
        return m.display || (m.team1 || '') + ' - ' + (m.team2 || '');
      }).join(' · ');

      var fields = {};
      fields[client.f.date]   = slipDate;
      fields[client.f.slip]   = matchText;
      fields[client.f.stake]  = Number(slip.stake || 0);
      fields[client.f.odds]   = Number(slip.totalOdds || 1);
      fields[client.f.result] = atResult;
      fields[client.f.ourAmt] = ourAmount;
      if (client.f.potential) fields[client.f.potential] = Number(slip.potentialWin || 0);
      if (client.f.notes) {
        fields[client.f.notes] = resolved.map(function(m) {
          var sc = (m.score && m.score !== 'N/F') ? ' ' + m.score : '';
          return (m.display || '') + sc;
        }).join(' | ');
      }

      return atCreate(client.tableId, fields).then(function(atRec) {
        console.log('Airtable record created:', atRec.id);
        var slipId = String(slipCounter++);
        slipMap[slipId] = { atId: atRec.id, tableId: client.tableId, clientId: client.id };

        broadcast({ type: 'new_slip', client_id: client.id });

        res.json({
          success:    true,
          slip:       { id: slipId, stake: slip.stake, totalOdds: slip.totalOdds, potentialWin: slip.potentialWin },
          matches:    resolved,
          isProfit:   isProfit,
          isPending:  isPending,
          ourAmount:  ourAmount,
          result:     isPending ? 'PENDING' : isProfit ? 'LOSS' : 'WIN',
          slipDate:   slipDate,
          image_path: imgPath
        });
      });
    });
  }).catch(function(e) {
    console.error('Analyze error:', e.message);
    res.status(500).json({ error: e.message });
  });
});

// PATCH /api/slips/:id
app.patch('/api/slips/:id', function(req, res) {
  var stored = slipMap[req.params.id];
  if (!stored) return res.json({ success: true });

  var client = getClient(stored.clientId);
  if (!client) return res.json({ success: true });

  var b        = req.body || {};
  var atResult = b.result === 'PENDING' ? AT_PENDING : b.is_profit ? AT_WIN : AT_LOSS;

  var fields = {};
  fields[client.f.result] = atResult;
  fields[client.f.ourAmt] = Number(b.our_amount || 0);

  atUpdate(client.tableId, stored.atId, fields).then(function() {
    broadcast({ type: 'slip_updated', client_id: client.id });
    res.json({ success: true });
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// DELETE /api/slips/:id
app.delete('/api/slips/:id', function(req, res) {
  var stored = slipMap[req.params.id];
  if (!stored) return res.json({ success: true });

  atDelete(stored.tableId, stored.atId).then(function() {
    delete slipMap[req.params.id];
    broadcast({ type: 'slip_deleted' });
    res.json({ success: true });
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// POST /api/slips/manual  (MUST come before PATCH/DELETE /:id)
app.post('/api/slips/manual', function(req, res) {
  var b      = req.body || {};
  var client = getClient(b.client_id);
  if (!client) return res.status(400).json({ error: 'Client not found' });

  var isP      = b.result === 'LOSS';
  var atResult = b.result === 'PENDING' ? AT_PENDING : isP ? AT_WIN : AT_LOSS;

  var fields = {};
  fields[client.f.date]   = b.slip_date || new Date().toISOString().slice(0, 10);
  fields[client.f.slip]   = (b.matches && b.matches[0] && b.matches[0].display) ? b.matches[0].display : '';
  fields[client.f.stake]  = Number(b.stake || 0);
  fields[client.f.odds]   = Number(b.total_odds || 1);
  fields[client.f.result] = atResult;
  fields[client.f.ourAmt] = Number(b.our_amount || 0);

  atCreate(client.tableId, fields).then(function() {
    broadcast({ type: 'new_slip', client_id: client.id });
    res.json({ success: true });
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// GET /api/live
app.get('/api/live', function(req, res) {
  var today = new Date().toISOString().split('T')[0];
  Promise.all([
    fetchNHL(today),
    fetchMLB(today),
    fetchESPN('basketball', 'nba', today)
  ]).then(function(results) {
    var live = [];

    (results[0] || []).forEach(function(g) {
      var st = g.gameState || '';
      if (st === 'LIVE' || st === 'CRIT') {
        live.push({ sport: 'НХЛ', status: 'LIVE',
          home: (g.homeTeam && g.homeTeam.abbrev) || '',
          away: (g.awayTeam && g.awayTeam.abbrev) || '',
          homeScore: (g.homeTeam && g.homeTeam.score) || 0,
          awayScore: (g.awayTeam && g.awayTeam.score) || 0 });
      } else if (st === 'FUT' || st === 'PRE') {
        live.push({ sport: 'НХЛ', status: 'UPCOMING',
          home: (g.homeTeam && g.homeTeam.abbrev) || '',
          away: (g.awayTeam && g.awayTeam.abbrev) || '',
          homeScore: 0, awayScore: 0 });
      }
    });

    (results[1] || []).forEach(function(g) {
      if (g.status && g.status.abstractGameState === 'Live') {
        live.push({ sport: 'МЛБ', status: 'LIVE',
          home: (g.teams && g.teams.home && g.teams.home.team && g.teams.home.team.abbreviation) || '',
          away: (g.teams && g.teams.away && g.teams.away.team && g.teams.away.team.abbreviation) || '',
          homeScore: (g.teams && g.teams.home && g.teams.home.score) || 0,
          awayScore: (g.teams && g.teams.away && g.teams.away.score) || 0 });
      }
    });

    (results[2] || []).forEach(function(ev) {
      var comp = ev.competitions && ev.competitions[0];
      if (!comp) return;
      var st = comp.status && comp.status.type;
      if (!st || st.state !== 'in') return;
      var cs   = comp.competitors || [];
      var home = cs.find(function(c) { return c.homeAway === 'home'; });
      var away = cs.find(function(c) { return c.homeAway === 'away'; });
      live.push({ sport: 'НБА', status: 'LIVE',
        home: (home && home.team && home.team.abbreviation) || '',
        away: (away && away.team && away.team.abbreviation) || '',
        homeScore: (home && home.score) || 0,
        awayScore: (away && away.score) || 0 });
    });

    res.json({ live: live });
  }).catch(function(e) { res.status(500).json({ error: e.message }); });
});

// Catch-all: serve frontend
app.get('*', function(req, res) {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ─── GLOBAL SAFETY ────────────────────────────────────────────────────────────
process.on('uncaughtException',  function(e) { console.error('uncaughtException:',  e.message); });
process.on('unhandledRejection', function(e) { console.error('unhandledRejection:', e && e.message); });

// ─── START ────────────────────────────────────────────────────────────────────
var PORT = process.env.PORT || 3000;
server.listen(PORT, function() {
  console.log('=== SERVER STARTED on port ' + PORT + ' ===');
  console.log('AIRTABLE_KEY:    ' + (!!process.env.AIRTABLE_KEY));
  console.log('OPENAI_API_KEY:  ' + (!!process.env.OPENAI_API_KEY));
  console.log('SPORTS_API_KEY:  ' + (!!process.env.SPORTS_API_KEY));
});
