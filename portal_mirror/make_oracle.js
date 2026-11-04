'use strict';
// Emit an ORACLE grid from the REAL portal offer.js + rules.json, for Python to
// check portal_pricing.py against. Oracle = ground truth (the app's own decision).
// Run: node make_oracle.js   (needs ~/kts-seller-app present for offer.js)
const path = require('path');
const fs = require('fs');
const APP = process.env.KTS_SELLER_APP || path.join(require('os').homedir(), 'kts-seller-app');
const offer = require(path.join(APP, 'offer.js'));
const RULES = require('./rules.json');

// portal category -> bot CardLadder sport string (what portal_pricing.py consumes)
const CAT_TO_BOTSPORT = { pokemon: 'pokemon', onepiece: 'one piece', basketball: 'basketball', baseball: 'mlb', football: 'football' };
const TRIUMPH = new Set(['onepiece', 'basketball', 'baseball', 'football']);

function oracle(cat, value, grade, conf, cert) {
  const row = {
    certId: String(cert), grader: 'PSA', grade: (grade == null ? '' : String(grade)),
    cardTitle: '', category: cat, imageUrl: '',
    marketValue: value, lastSale: null,
    payout: (TRIUMPH.has(cat) ? 1 : null), status: (TRIUMPH.has(cat) ? 'accepted' : 'priced'),
    clConfidence: conf, rejectionReason: '',
  };
  const ev = offer.evaluate(row, RULES, { rateOverrides: null });
  return { decision: ev.decision === 'BUY' ? 'accept' : 'reject', offer: ev.offer == null ? null : ev.offer };
}

const rows = [];
const cats = Object.keys(CAT_TO_BOTSPORT);
// Main grid: every $0.25, grade 8 (passes Pokémon 7), conf 5 (passes all), cert ok.
for (const cat of cats) {
  for (let c = 0; c <= 580000; c += 25) {
    const v = c / 400;             // step = 25/100 = $0.25  (c in quarter-cents)
    const val = Math.round(v * 100) / 100;
    const o = oracle(cat, val, 8, 5, '90000001');
    rows.push({ sport: CAT_TO_BOTSPORT[cat], value: val, grade: 8, cert: '90000001', decision: o.decision, offer: o.offer });
  }
}
// Grade sweep.
for (const cat of cats) for (const g of [1, 3, 5, 6, 7, 8, 9, 10]) for (const v of [5, 20, 75, 150, 300, 500, 650, 900, 1000, 1300]) {
  const o = oracle(cat, v, g, 5, '90000001');
  rows.push({ sport: CAT_TO_BOTSPORT[cat], value: v, grade: g, cert: '90000001', decision: o.decision, offer: o.offer });
}
// Cert sweep (Pokémon).
for (const cert of ['090000000', '69999999', '70000000', '70000001', '150000000', '1234567', '9999999']) for (const v of [50, 200, 500, 900]) {
  const o = oracle('pokemon', v, 8, 5, cert);
  rows.push({ sport: 'pokemon', value: v, grade: 8, cert: cert, decision: o.decision, offer: o.offer });
}
fs.writeFileSync(path.join(__dirname, 'oracle_grid.json'), JSON.stringify({ generatedFrom: 'offer.js', rules_updated_at: '2026-10-30', count: rows.length, rows }));
console.log('wrote oracle_grid.json with', rows.length, 'rows');
