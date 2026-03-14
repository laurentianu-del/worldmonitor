#!/usr/bin/env node

import { loadEnvFile, runSeed, getRedisCredentials } from './_seed-utils.mjs';

loadEnvFile(import.meta.url);

const CANONICAL_KEY = 'correlation:cards-bootstrap:v1';
const CACHE_TTL = 600;

const INPUT_KEYS = [
  'military:flights:v1',
  'military:flights:stale:v1',
  'unrest:events:v1',
  'infra:outages:v1',
  'seismology:earthquakes:v1',
  'market:stocks-bootstrap:v1',
  'market:commodities-bootstrap:v1',
  'market:crypto:v1',
  'news:insights:v1',
];

async function fetchInputData() {
  const { url, token } = getRedisCredentials();
  const pipeline = INPUT_KEYS.map(k => ['GET', k]);
  const resp = await fetch(`${url}/pipeline`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(pipeline),
    signal: AbortSignal.timeout(10_000),
  });
  if (!resp.ok) throw new Error(`Redis pipeline: HTTP ${resp.status}`);
  const results = await resp.json();
  const data = {};
  for (let i = 0; i < INPUT_KEYS.length; i++) {
    const raw = results[i]?.result;
    if (raw) {
      try { data[INPUT_KEYS[i]] = JSON.parse(raw); } catch { /* skip */ }
    }
  }
  return data;
}

// ── Haversine ───────────────────────────────────────────────
function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
    Math.cos((lat2 * Math.PI) / 180) *
    Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

// ── Country Name Resolution ─────────────────────────────────
const COUNTRY_NAME_TO_ISO2 = {
  'afghanistan': 'AF', 'albania': 'AL', 'algeria': 'DZ', 'angola': 'AO',
  'argentina': 'AR', 'armenia': 'AM', 'australia': 'AU', 'austria': 'AT',
  'azerbaijan': 'AZ', 'bahrain': 'BH', 'bangladesh': 'BD', 'belarus': 'BY',
  'belgium': 'BE', 'bolivia': 'BO', 'bosnia and herzegovina': 'BA',
  'brazil': 'BR', 'bulgaria': 'BG', 'burkina faso': 'BF', 'burma': 'MM',
  'cambodia': 'KH', 'cameroon': 'CM', 'canada': 'CA', 'chad': 'TD',
  'chile': 'CL', 'china': 'CN', 'colombia': 'CO', 'congo': 'CG',
  'costa rica': 'CR', 'croatia': 'HR', 'cuba': 'CU', 'cyprus': 'CY',
  'czech republic': 'CZ', 'czechia': 'CZ',
  'democratic republic of the congo': 'CD', 'dr congo': 'CD', 'drc': 'CD',
  'denmark': 'DK', 'djibouti': 'DJ', 'dominican republic': 'DO',
  'ecuador': 'EC', 'egypt': 'EG', 'el salvador': 'SV', 'eritrea': 'ER',
  'estonia': 'EE', 'ethiopia': 'ET', 'finland': 'FI', 'france': 'FR',
  'gabon': 'GA', 'georgia': 'GE', 'germany': 'DE', 'ghana': 'GH',
  'greece': 'GR', 'guatemala': 'GT', 'guinea': 'GN', 'haiti': 'HT',
  'honduras': 'HN', 'hungary': 'HU', 'iceland': 'IS', 'india': 'IN',
  'indonesia': 'ID', 'iran': 'IR', 'iraq': 'IQ', 'ireland': 'IE',
  'israel': 'IL', 'italy': 'IT', 'ivory coast': 'CI', "cote d'ivoire": 'CI',
  'jamaica': 'JM', 'japan': 'JP', 'jordan': 'JO', 'kazakhstan': 'KZ',
  'kenya': 'KE', 'kosovo': 'XK', 'kuwait': 'KW', 'kyrgyzstan': 'KG',
  'laos': 'LA', 'latvia': 'LV', 'lebanon': 'LB', 'libya': 'LY',
  'lithuania': 'LT', 'madagascar': 'MG', 'malawi': 'MW', 'malaysia': 'MY',
  'mali': 'ML', 'mauritania': 'MR', 'mexico': 'MX', 'moldova': 'MD',
  'mongolia': 'MN', 'montenegro': 'ME', 'morocco': 'MA', 'mozambique': 'MZ',
  'myanmar': 'MM', 'namibia': 'NA', 'nepal': 'NP', 'netherlands': 'NL',
  'new zealand': 'NZ', 'nicaragua': 'NI', 'niger': 'NE', 'nigeria': 'NG',
  'north korea': 'KP', 'north macedonia': 'MK', 'norway': 'NO',
  'oman': 'OM', 'pakistan': 'PK', 'palestine': 'PS', 'panama': 'PA',
  'papua new guinea': 'PG', 'paraguay': 'PY', 'peru': 'PE',
  'philippines': 'PH', 'poland': 'PL', 'portugal': 'PT', 'qatar': 'QA',
  'romania': 'RO', 'russia': 'RU', 'rwanda': 'RW', 'saudi arabia': 'SA',
  'senegal': 'SN', 'serbia': 'RS', 'sierra leone': 'SL', 'singapore': 'SG',
  'slovakia': 'SK', 'slovenia': 'SI', 'somalia': 'SO', 'south africa': 'ZA',
  'south korea': 'KR', 'south sudan': 'SS', 'spain': 'ES',
  'sri lanka': 'LK', 'sudan': 'SD', 'sweden': 'SE', 'switzerland': 'CH',
  'syria': 'SY', 'taiwan': 'TW', 'tajikistan': 'TJ', 'tanzania': 'TZ',
  'thailand': 'TH', 'togo': 'TG', 'trinidad and tobago': 'TT',
  'tunisia': 'TN', 'turkey': 'TR', 'turkmenistan': 'TM', 'uganda': 'UG',
  'ukraine': 'UA', 'united arab emirates': 'AE', 'uae': 'AE',
  'united kingdom': 'GB', 'uk': 'GB', 'united states': 'US', 'usa': 'US',
  'uruguay': 'UY', 'uzbekistan': 'UZ', 'venezuela': 'VE', 'vietnam': 'VN',
  'yemen': 'YE', 'zambia': 'ZM', 'zimbabwe': 'ZW',
  'east timor': 'TL', 'cape verde': 'CV', 'swaziland': 'SZ',
  'republic of the congo': 'CG',
};

const ISO3_TO_ISO2 = {
  'AFG': 'AF', 'ALB': 'AL', 'DZA': 'DZ', 'AGO': 'AO', 'ARG': 'AR',
  'ARM': 'AM', 'AUS': 'AU', 'AUT': 'AT', 'AZE': 'AZ', 'BHR': 'BH',
  'BGD': 'BD', 'BLR': 'BY', 'BEL': 'BE', 'BOL': 'BO', 'BIH': 'BA',
  'BRA': 'BR', 'BGR': 'BG', 'BFA': 'BF', 'KHM': 'KH', 'CMR': 'CM',
  'CAN': 'CA', 'TCD': 'TD', 'CHL': 'CL', 'CHN': 'CN', 'COL': 'CO',
  'COG': 'CG', 'CRI': 'CR', 'HRV': 'HR', 'CUB': 'CU', 'CYP': 'CY',
  'CZE': 'CZ', 'COD': 'CD', 'DNK': 'DK', 'DJI': 'DJ', 'DOM': 'DO',
  'ECU': 'EC', 'EGY': 'EG', 'SLV': 'SV', 'ERI': 'ER', 'EST': 'EE',
  'ETH': 'ET', 'FIN': 'FI', 'FRA': 'FR', 'GAB': 'GA', 'GEO': 'GE',
  'DEU': 'DE', 'GHA': 'GH', 'GRC': 'GR', 'GTM': 'GT', 'GIN': 'GN',
  'HTI': 'HT', 'HND': 'HN', 'HUN': 'HU', 'ISL': 'IS', 'IND': 'IN',
  'IDN': 'ID', 'IRN': 'IR', 'IRQ': 'IQ', 'IRL': 'IE', 'ISR': 'IL',
  'ITA': 'IT', 'CIV': 'CI', 'JAM': 'JM', 'JPN': 'JP', 'JOR': 'JO',
  'KAZ': 'KZ', 'KEN': 'KE', 'XKX': 'XK', 'KWT': 'KW', 'KGZ': 'KG',
  'LAO': 'LA', 'LVA': 'LV', 'LBN': 'LB', 'LBY': 'LY', 'LTU': 'LT',
  'MDG': 'MG', 'MWI': 'MW', 'MYS': 'MY', 'MLI': 'ML', 'MRT': 'MR',
  'MEX': 'MX', 'MDA': 'MD', 'MNG': 'MN', 'MNE': 'ME', 'MAR': 'MA',
  'MOZ': 'MZ', 'MMR': 'MM', 'NAM': 'NA', 'NPL': 'NP', 'NLD': 'NL',
  'NZL': 'NZ', 'NIC': 'NI', 'NER': 'NE', 'NGA': 'NG', 'PRK': 'KP',
  'MKD': 'MK', 'NOR': 'NO', 'OMN': 'OM', 'PAK': 'PK', 'PSE': 'PS',
  'PAN': 'PA', 'PNG': 'PG', 'PRY': 'PY', 'PER': 'PE', 'PHL': 'PH',
  'POL': 'PL', 'PRT': 'PT', 'QAT': 'QA', 'ROU': 'RO', 'RUS': 'RU',
  'RWA': 'RW', 'SAU': 'SA', 'SEN': 'SN', 'SRB': 'RS', 'SLE': 'SL',
  'SGP': 'SG', 'SVK': 'SK', 'SVN': 'SI', 'SOM': 'SO', 'ZAF': 'ZA',
  'KOR': 'KR', 'SSD': 'SS', 'ESP': 'ES', 'LKA': 'LK', 'SDN': 'SD',
  'SWE': 'SE', 'CHE': 'CH', 'SYR': 'SY', 'TWN': 'TW', 'TJK': 'TJ',
  'TZA': 'TZ', 'THA': 'TH', 'TGO': 'TG', 'TTO': 'TT', 'TUN': 'TN',
  'TUR': 'TR', 'TKM': 'TM', 'UGA': 'UG', 'UKR': 'UA', 'ARE': 'AE',
  'GBR': 'GB', 'USA': 'US', 'URY': 'UY', 'UZB': 'UZ', 'VEN': 'VE',
  'VNM': 'VN', 'YEM': 'YE', 'ZMB': 'ZM', 'ZWE': 'ZW',
};

function normalizeToCode(country) {
  if (!country) return undefined;
  const t = country.trim();
  if (t.length === 2) return t.toUpperCase();
  if (t.length === 3) return ISO3_TO_ISO2[t.toUpperCase()] ?? undefined;
  return COUNTRY_NAME_TO_ISO2[t.toLowerCase()] ?? undefined;
}

const COUNTRY_NAME_ENTRIES = Object.entries(COUNTRY_NAME_TO_ISO2)
  .filter(([name]) => name.length >= 4)
  .sort((a, b) => b[0].length - a[0].length)
  .map(([name, code]) => ({ name, code, regex: new RegExp(`\\b${name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i') }));

function matchCountryNamesInText(text) {
  const matched = [];
  let remaining = text.toLowerCase();
  for (const { code, regex } of COUNTRY_NAME_ENTRIES) {
    if (regex.test(remaining)) {
      matched.push(code);
      remaining = remaining.replace(regex, '');
    }
  }
  return matched;
}

// ── Adapter: Military ───────────────────────────────────────
const STRIKE_TYPES = new Set(['fighter', 'bomber', 'attack']);
const SUPPORT_TYPES = new Set(['tanker', 'awacs', 'surveillance', 'electronic_warfare']);

function collectMilitarySignals(flights) {
  const signals = [];
  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;
  for (const f of flights) {
    const ts = typeof f.lastSeen === 'number' ? f.lastSeen : (f.lastSeen ? new Date(f.lastSeen).getTime() : now);
    if (now - ts > windowMs) continue;
    const isStrike = STRIKE_TYPES.has(f.aircraftType);
    const isSupport = SUPPORT_TYPES.has(f.aircraftType);
    const severity = isStrike ? 80 : isSupport ? 60 : 55;
    signals.push({
      type: 'military_flight',
      source: 'signal-aggregator',
      severity,
      lat: f.lat,
      lon: f.lon,
      country: f.operatorCountry,
      timestamp: ts,
      label: `${f.operator || ''} ${f.aircraftType || ''} ${f.callsign || ''}`.trim(),
      aircraftType: f.aircraftType,
    });
  }
  return signals;
}

function generateMilitaryTitle(cluster) {
  const types = new Set(cluster.map(s => s.type));
  const countries = [...new Set(cluster.map(s => s.country).filter(Boolean))];
  const countryLabel = countries.slice(0, 2).join('/') || 'Unknown region';
  const flightTypes = new Set(
    cluster.filter(s => s.type === 'military_flight').map(s => s.aircraftType).filter(Boolean),
  );
  const hasStrikePackage = [...STRIKE_TYPES].some(t => flightTypes.has(t)) &&
                           [...SUPPORT_TYPES].some(t => flightTypes.has(t));
  if (hasStrikePackage) return `Strike packaging detected \u2014 ${countryLabel}`;
  if (types.has('military_flight')) return `Military flight cluster \u2014 ${countryLabel}`;
  return `Military activity convergence \u2014 ${countryLabel}`;
}

// ── Adapter: Escalation ─────────────────────────────────────
const ESCALATION_KEYWORDS = /\b((?:military|armed|air)\s*(?:strike|attack|offensive)|invasion|bombing|missile|airstrike|shelling|drone\s+strike|war(?:fare)?|ceasefire|martial\s+law|armed\s+clash(?:es)?|gunfire|coup(?:\s+attempt)?|insurgent|rebel|militia|terror(?:ist|ism)|hostage|siege|blockade|mobiliz(?:ation|e)|escalat(?:ion|ing|e)|retaliat|deploy(?:ment|ed)|incursion|annex(?:ation|ed)|occupation|humanitarian\s+crisis|refugee|evacuat|nuclear|chemical\s+weapon|biological\s+weapon)\b/i;

function collectEscalationSignals(protests, outages, newsClusters) {
  const signals = [];
  const now = Date.now();
  const windowMs = 48 * 60 * 60 * 1000;

  for (const p of protests) {
    const ts = typeof p.time === 'number' ? p.time : (p.time ? new Date(p.time).getTime() : now);
    if (now - ts > windowMs) continue;
    const code = normalizeToCode(p.country);
    if (!code) continue;
    const severityMap = { high: 85, medium: 55, low: 30 };
    signals.push({
      type: 'conflict_event',
      source: 'signal-aggregator',
      severity: severityMap[p.severity] ?? 40,
      lat: p.lat,
      lon: p.lon,
      country: code,
      timestamp: ts,
      label: `${p.eventType || 'event'}: ${p.title || ''}`,
    });
  }

  for (const o of outages) {
    const ts = typeof o.pubDate === 'number' ? o.pubDate : (o.pubDate ? new Date(o.pubDate).getTime() : now);
    if (now - ts > windowMs) continue;
    if (o.lat != null && o.lon != null && o.lat === 0 && o.lon === 0) continue;
    const code = normalizeToCode(o.country);
    if (!code) continue;
    const severityMap = { total: 90, major: 70, partial: 40 };
    signals.push({
      type: 'escalation_outage',
      source: 'signal-aggregator',
      severity: severityMap[o.severity] ?? 30,
      lat: o.lat,
      lon: o.lon,
      country: code,
      timestamp: ts,
      label: `${o.severity || ''} outage: ${o.title || ''}`,
    });
  }

  for (const c of newsClusters) {
    if (!c.threat || c.threat.level === 'info' || c.threat.level === 'low') continue;
    const ts = c.lastUpdated ?? now;
    if (now - ts > windowMs) continue;
    if (!ESCALATION_KEYWORDS.test(c.primaryTitle)) continue;
    const severity = c.threat.level === 'critical' ? 85 : c.threat.level === 'high' ? 65 : 45;
    const matched = matchCountryNamesInText(c.primaryTitle);
    const code = normalizeToCode(matched[0]);
    if (!code) continue;
    signals.push({
      type: 'news_severity',
      source: 'analysis-core',
      severity,
      lat: c.lat,
      lon: c.lon,
      country: code,
      timestamp: ts,
      label: c.primaryTitle,
    });
  }

  const conflictCountries = new Set(
    signals.filter(s => s.type === 'conflict_event').map(s => s.country).filter(Boolean),
  );
  return signals.filter(s => s.type !== 'escalation_outage' || conflictCountries.has(s.country));
}

function generateEscalationTitle(cluster) {
  const types = new Set(cluster.map(s => s.type));
  const countries = [...new Set(cluster.map(s => s.country).filter(Boolean))];
  const countryLabel = countries[0] || 'Unknown';
  const parts = [];
  if (types.has('conflict_event')) parts.push('conflict');
  if (types.has('escalation_outage')) parts.push('comms disruption');
  if (types.has('news_severity')) parts.push('news escalation');
  return parts.length > 0
    ? `${parts.join(' + ')} \u2014 ${countryLabel}`
    : `Escalation signals \u2014 ${countryLabel}`;
}

// ── Adapter: Economic ───────────────────────────────────────
const SANCTIONS_KEYWORDS = /\b(sanction|tariff|embargo|trade\s+war|ban|restrict|block|seize|freeze\s+assets|export\s+control|blacklist|decouple|decoupl|subsid|dumping|countervail|quota|levy|excise|retaliat|currency\s+manipulat|capital\s+controls|swift|cbdc|petrodollar|de-?dollar|opec|cartel|price\s+cap|oil|crude|commodity|shortage|stockpile|strategic\s+reserve|supply\s+chain|rare\s+earth|chip\s+ban|semiconductor|economic\s+warfare|financial\s+weapon)\b/i;
const COMMODITY_SYMBOLS = new Set(['CL=F', 'GC=F', 'NG=F', 'SI=F', 'HG=F', 'ZW=F', 'BTC-USD', 'BZ=F', 'ETH-USD', 'KC=F', 'SB=F', 'CT=F', 'CC=F']);
const SIGNIFICANT_CHANGE_PCT = 1.5;

function collectEconomicSignals(markets, newsClusters) {
  const signals = [];
  const now = Date.now();
  const windowMs = 24 * 60 * 60 * 1000;

  for (const m of markets) {
    if (m.change == null || m.price == null) continue;
    const absPct = Math.abs(m.change);
    if (absPct < SIGNIFICANT_CHANGE_PCT) continue;
    const isCommodity = COMMODITY_SYMBOLS.has(m.symbol);
    const type = isCommodity ? 'commodity_spike' : 'market_move';
    signals.push({
      type,
      source: 'markets',
      severity: Math.min(100, absPct * 10),
      timestamp: now,
      label: `${m.display ?? m.symbol} ${m.change > 0 ? '+' : ''}${m.change.toFixed(1)}%`,
      symbol: m.symbol,
      display: m.display,
      change: m.change,
    });
  }

  for (const c of newsClusters) {
    const ts = c.lastUpdated ?? now;
    if (now - ts > windowMs) continue;
    if (!SANCTIONS_KEYWORDS.test(c.primaryTitle)) continue;
    const severity = c.threat?.level === 'critical' ? 85 : c.threat?.level === 'high' ? 70 : 50;
    signals.push({
      type: 'sanctions_news',
      source: 'analysis-core',
      severity,
      timestamp: ts,
      label: c.primaryTitle,
    });
  }

  return signals;
}

const KNOWN_ENTITIES = /\b(Iran|Russia|China|North Korea|Venezuela|Cuba|Syria|Myanmar|Belarus|Turkey|Saudi|OPEC|EU|USA?|United States|India)\b(?![A-Za-z])/i;
const GENERIC_ENTITY_KEYS = new Set([
  'sanctions', 'trade', 'tariff', 'commodity', 'currency', 'energy',
  'embargo', 'semiconductor', 'crypto', 'inflation',
]);

function generateEconomicTitle(cluster, entityKey) {
  const types = new Set(cluster.map(s => s.type));

  if (types.has('commodity_spike')) {
    const spikes = cluster.filter(s => s.type === 'commodity_spike');
    const names = spikes.map(s => s.display ?? s.symbol ?? s.label.split(' ')[0]).slice(0, 2);
    const change = spikes[0]?.change;
    const pctSuffix = change != null ? ` (${change > 0 ? '+' : ''}${change.toFixed(1)}%)` : '';
    const base = `${names.join('/')} spike${pctSuffix}`;
    if (types.has('sanctions_news')) return `${base} + sanctions`;
    return base;
  }

  if (types.has('sanctions_news')) {
    const labels = cluster.filter(s => s.type === 'sanctions_news').map(s => s.label);
    let qualifier = '';
    for (const label of labels) {
      const match = KNOWN_ENTITIES.exec(label);
      if (match) { qualifier = match[1]; break; }
    }
    if (!qualifier && entityKey && !GENERIC_ENTITY_KEYS.has(entityKey)) {
      qualifier = entityKey.charAt(0).toUpperCase() + entityKey.slice(1);
    }
    const sanctionsBase = qualifier ? `${qualifier} sanctions activity` : 'Sanctions activity';
    if (types.has('market_move')) {
      const movers = cluster.filter(s => s.type === 'market_move');
      const moverNames = movers.map(s => s.display ?? s.symbol ?? s.label.split(' ')[0]).slice(0, 2);
      return `${sanctionsBase} + ${moverNames.join('/')} disruption`;
    }
    return sanctionsBase;
  }

  if (types.has('market_move')) {
    const movers = cluster.filter(s => s.type === 'market_move');
    const names = movers.map(s => s.display ?? s.symbol ?? s.label.split(' ')[0]).slice(0, 2);
    return `Market disruption: ${names.join('/')}`;
  }

  const fallback = entityKey && !GENERIC_ENTITY_KEYS.has(entityKey)
    ? entityKey.charAt(0).toUpperCase() + entityKey.slice(1) : '';
  return fallback ? `Economic convergence: ${fallback}` : 'Economic convergence detected';
}

// ── Adapter: Disaster ───────────────────────────────────────
function collectDisasterSignals(earthquakes, outages, protests) {
  const signals = [];
  const now = Date.now();
  const windowMs = 96 * 60 * 60 * 1000;

  for (const q of earthquakes) {
    const ts = q.occurredAt ?? now;
    if (now - ts > windowMs) continue;
    if (q.location?.latitude == null || q.location?.longitude == null) continue;
    const severity = Math.min(100, Math.max(10, (q.magnitude - 1.5) * 17));
    signals.push({
      type: 'earthquake',
      source: 'usgs',
      severity,
      lat: q.location.latitude,
      lon: q.location.longitude,
      timestamp: ts,
      label: `M${q.magnitude.toFixed(1)} \u2014 ${q.place}`,
      magnitude: q.magnitude,
    });
  }

  const conflictCountries = new Set(
    (protests ?? [])
      .filter(p => {
        const ts = typeof p.time === 'number' ? p.time : (p.time ? new Date(p.time).getTime() : now);
        return (now - ts) <= windowMs;
      })
      .map(p => p.country)
      .filter(Boolean),
  );

  for (const o of outages) {
    const ts = typeof o.pubDate === 'number' ? o.pubDate : (o.pubDate ? new Date(o.pubDate).getTime() : now);
    if (now - ts > windowMs) continue;
    if (o.country && conflictCountries.has(o.country)) continue;
    if (o.lat == null || o.lon == null || (o.lat === 0 && o.lon === 0)) continue;
    const severityMap = { total: 90, major: 70, partial: 40 };
    signals.push({
      type: 'infra_outage',
      source: 'signal-aggregator',
      severity: severityMap[o.severity] ?? 30,
      lat: o.lat,
      lon: o.lon,
      country: o.country,
      timestamp: ts,
      label: `Infra outage: ${o.title || ''}`,
    });
  }

  return signals;
}

function generateDisasterTitle(cluster) {
  const types = new Set(cluster.map(s => s.type));
  const parts = [];
  if (types.has('earthquake')) {
    const maxMag = Math.max(...cluster.filter(s => s.type === 'earthquake').map(s => s.magnitude ?? 0));
    parts.push(`M${maxMag.toFixed(1)} seismic`);
  }
  if (types.has('infra_outage')) parts.push('infra disruption');
  const quakePlace = cluster.find(s => s.type === 'earthquake')?.label?.split('\u2014')[1]?.trim();
  return parts.length > 0
    ? `Disaster cascade: ${parts.join(' + ')}${quakePlace ? ` \u2014 ${quakePlace}` : ''}`
    : 'Disaster convergence detected';
}

// ── Clustering ──────────────────────────────────────────────
function clusterByProximity(signals, radiusKm) {
  if (signals.length === 0) return [];
  const DEG_PER_KM_LAT = 1 / 111;
  const cellSizeLat = radiusKm * DEG_PER_KM_LAT;
  const parent = signals.map((_, i) => i);
  const find = (i) => {
    while (parent[i] !== i) { parent[i] = parent[parent[i]]; i = parent[i]; }
    return i;
  };
  const union = (a, b) => {
    const ra = find(a), rb = find(b);
    if (ra !== rb) parent[ra] = rb;
  };
  const grid = new Map();
  const validIndices = [];
  for (let i = 0; i < signals.length; i++) {
    const s = signals[i];
    if (s.lat == null || s.lon == null) continue;
    validIndices.push(i);
    const cellRow = Math.floor(s.lat / cellSizeLat);
    const cosLat = Math.cos(s.lat * Math.PI / 180);
    const cellSizeLon = cosLat > 0.01 ? cellSizeLat / cosLat : cellSizeLat;
    const cellCol = Math.floor(s.lon / cellSizeLon);
    const key = `${cellRow}:${cellCol}`;
    const list = grid.get(key);
    if (list) list.push(i); else grid.set(key, [i]);
  }
  for (const [key, indices] of grid) {
    const sep = key.indexOf(':');
    const row = Number(key.slice(0, sep));
    const col = Number(key.slice(sep + 1));
    for (let dr = -1; dr <= 1; dr++) {
      for (let dc = -1; dc <= 1; dc++) {
        const neighbors = grid.get(`${row + dr}:${col + dc}`);
        if (!neighbors) continue;
        for (const i of indices) {
          const si = signals[i];
          for (const j of neighbors) {
            if (i >= j) continue;
            const sj = signals[j];
            if (haversineKm(si.lat, si.lon, sj.lat, sj.lon) <= radiusKm) union(i, j);
          }
        }
      }
    }
  }
  const clusterMap = new Map();
  for (const i of validIndices) {
    const root = find(i);
    const list = clusterMap.get(root);
    if (list) list.push(signals[i]); else clusterMap.set(root, [signals[i]]);
  }
  const clusters = [];
  for (const sigs of clusterMap.values()) {
    if (sigs.length >= 2) clusters.push({ signals: sigs });
  }
  return clusters;
}

function clusterByCountry(signals) {
  const byCountry = new Map();
  for (const s of signals) {
    if (!s.country) continue;
    const list = byCountry.get(s.country) ?? [];
    list.push(s);
    byCountry.set(s.country, list);
  }
  const clusters = [];
  for (const [country, sigs] of byCountry) {
    if (sigs.length < 2) continue;
    clusters.push({ signals: sigs, country });
  }
  return clusters;
}

function clusterByEntity(signals) {
  const COMPOUND_PATTERNS = [
    'supply chain', 'rare earth', 'central bank', 'interest rate',
    'trade war', 'oil price', 'gas price', 'federal reserve',
  ];
  const SINGLE_KEYS = new Set([
    'oil', 'gas', 'sanctions', 'trade', 'tariff', 'commodity', 'currency',
    'energy', 'wheat', 'crude', 'gold', 'silver', 'copper', 'bitcoin',
    'crypto', 'inflation', 'embargo', 'opec', 'semiconductor', 'dollar',
    'yuan', 'euro',
  ]);
  const tokenMap = new Map();
  for (const s of signals) {
    const lower = s.label.toLowerCase();
    let matchedKey = COMPOUND_PATTERNS.find(p => lower.includes(p));
    if (!matchedKey) {
      const words = lower.split(/\W+/);
      matchedKey = words.find(w => SINGLE_KEYS.has(w));
    }
    if (!matchedKey) continue;
    const list = tokenMap.get(matchedKey) ?? [];
    list.push(s);
    tokenMap.set(matchedKey, list);
  }
  const clusters = [];
  for (const [key, sigs] of tokenMap) {
    if (sigs.length < 2) continue;
    clusters.push({ signals: sigs, entityKey: key });
  }
  return clusters;
}

// ── Scoring ─────────────────────────────────────────────────
function scoreClusters(clusters, weights, threshold) {
  return clusters
    .map(cluster => {
      const perType = new Map();
      for (const s of cluster.signals) {
        const current = perType.get(s.type) ?? 0;
        perType.set(s.type, Math.max(current, s.severity));
      }
      let weightedSum = 0;
      for (const [type, severity] of perType) {
        weightedSum += severity * (weights[type] ?? 0);
      }
      const diversityBonus = Math.min(30, Math.max(0, (perType.size - 2)) * 12);
      const score = Math.min(100, weightedSum + diversityBonus);

      let centroidLat, centroidLon;
      const geoSignals = cluster.signals.filter(s => s.lat != null && s.lon != null);
      if (geoSignals.length > 0) {
        centroidLat = geoSignals.reduce((sum, s) => sum + s.lat, 0) / geoSignals.length;
        const toRad = Math.PI / 180;
        let sinSum = 0, cosSum = 0;
        for (const s of geoSignals) {
          sinSum += Math.sin(s.lon * toRad);
          cosSum += Math.cos(s.lon * toRad);
        }
        centroidLon = Math.atan2(sinSum, cosSum) * (180 / Math.PI);
      }

      const countries = [...new Set(cluster.signals.map(s => s.country).filter(Boolean))];
      const key = cluster.country ?? cluster.entityKey ?? `${centroidLat?.toFixed(1)},${centroidLon?.toFixed(1)}`;

      return { cluster, score, countries, centroidLat, centroidLon, key };
    })
    .filter(c => c.score >= threshold);
}

// ── Card Generation ─────────────────────────────────────────
function toCard(scored, domain, titleFn) {
  const title = titleFn(scored.cluster.signals, scored.cluster.entityKey);
  const location = scored.centroidLat != null && scored.centroidLon != null
    ? { lat: scored.centroidLat, lon: scored.centroidLon, label: scored.key }
    : undefined;

  const signals = scored.cluster.signals.map(s => ({
    type: s.type,
    source: s.source,
    severity: s.severity,
    lat: s.lat,
    lon: s.lon,
    country: s.country,
    timestamp: s.timestamp,
    label: s.label,
  }));

  return {
    id: `${domain}:${scored.key}`,
    domain,
    title,
    score: Math.round(scored.score),
    signals,
    location,
    countries: scored.countries,
    trend: 'stable',
    timestamp: Date.now(),
  };
}

// ── Domain configs ──────────────────────────────────────────
const DOMAINS = {
  military: {
    weights: { military_flight: 0.40, ais_gap: 0.30, military_vessel: 0.30 },
    clusterMode: 'geographic',
    spatialRadius: 500,
    threshold: 20,
    titleFn: generateMilitaryTitle,
  },
  escalation: {
    weights: { conflict_event: 0.45, escalation_outage: 0.25, news_severity: 0.30 },
    clusterMode: 'country',
    threshold: 20,
    titleFn: generateEscalationTitle,
  },
  economic: {
    weights: { market_move: 0.35, sanctions_news: 0.30, commodity_spike: 0.35 },
    clusterMode: 'entity',
    threshold: 20,
    titleFn: generateEconomicTitle,
  },
  disaster: {
    weights: { earthquake: 0.55, infra_outage: 0.45 },
    clusterMode: 'geographic',
    spatialRadius: 500,
    threshold: 20,
    titleFn: generateDisasterTitle,
  },
};

// ── Main ────────────────────────────────────────────────────
async function computeCorrelation() {
  const data = await fetchInputData();

  const hasAnyData = INPUT_KEYS.some(k => data[k] != null);
  if (!hasAnyData) throw new Error('No input data available in Redis');

  const flights = data['military:flights:v1']?.flights
    ?? data['military:flights:stale:v1']?.flights
    ?? data['military:flights:v1'] ?? data['military:flights:stale:v1'] ?? [];
  const rawFlights = Array.isArray(flights) ? flights : [];

  const protestData = data['unrest:events:v1'];
  const protests = protestData?.events ?? (Array.isArray(protestData) ? protestData : []);

  const outageData = data['infra:outages:v1'];
  const outages = outageData?.outages ?? (Array.isArray(outageData) ? outageData : []);

  const quakeData = data['seismology:earthquakes:v1'];
  const earthquakes = quakeData?.earthquakes ?? (Array.isArray(quakeData) ? quakeData : []);

  const stockQuotes = data['market:stocks-bootstrap:v1']?.quotes ?? [];
  const commodityQuotes = data['market:commodities-bootstrap:v1']?.quotes ?? [];
  const cryptoQuotes = data['market:crypto:v1']?.quotes ?? [];
  const allMarkets = [...stockQuotes, ...commodityQuotes, ...cryptoQuotes];

  const insights = data['news:insights:v1'];
  const newsClusters = (insights?.topStories ?? []).map(s => ({
    primaryTitle: s.primaryTitle,
    threat: { level: s.threatLevel ?? 'moderate' },
    lastUpdated: s.publishedAt ?? insights?.fetchedAt ?? Date.now(),
    lat: s.lat,
    lon: s.lon,
  }));

  const result = { military: [], escalation: [], economic: [], disaster: [], computedAt: Date.now() };

  // Military
  const milSignals = collectMilitarySignals(rawFlights);
  const milClusters = clusterByProximity(milSignals, 500);
  const milScored = scoreClusters(milClusters, DOMAINS.military.weights, DOMAINS.military.threshold);
  result.military = milScored.map(s => toCard(s, 'military', generateMilitaryTitle)).sort((a, b) => b.score - a.score);

  // Escalation
  const escSignals = collectEscalationSignals(protests, outages, newsClusters);
  const escClusters = clusterByCountry(escSignals);
  const escScored = scoreClusters(escClusters, DOMAINS.escalation.weights, DOMAINS.escalation.threshold);
  result.escalation = escScored.map(s => toCard(s, 'escalation', generateEscalationTitle)).sort((a, b) => b.score - a.score);

  // Economic
  const ecoSignals = collectEconomicSignals(allMarkets, newsClusters);
  const ecoClusters = clusterByEntity(ecoSignals);
  const ecoScored = scoreClusters(ecoClusters, DOMAINS.economic.weights, DOMAINS.economic.threshold);
  result.economic = ecoScored.map(s => toCard(s, 'economic', generateEconomicTitle)).sort((a, b) => b.score - a.score);

  // Disaster
  const disSignals = collectDisasterSignals(earthquakes, outages, protests);
  const disClusters = clusterByProximity(disSignals, 500);
  const disScored = scoreClusters(disClusters, DOMAINS.disaster.weights, DOMAINS.disaster.threshold);
  result.disaster = disScored.map(s => toCard(s, 'disaster', generateDisasterTitle)).sort((a, b) => b.score - a.score);

  return result;
}

runSeed('correlation', 'cards', CANONICAL_KEY, computeCorrelation, {
  ttlSeconds: CACHE_TTL,
  sourceVersion: 'correlation-engine-v1',
  recordCount: (data) => (data.military?.length ?? 0) + (data.escalation?.length ?? 0) + (data.economic?.length ?? 0) + (data.disaster?.length ?? 0),
  extraKeys: [
    { key: 'correlation:military:v1', ttl: CACHE_TTL },
    { key: 'correlation:escalation:v1', ttl: CACHE_TTL },
    { key: 'correlation:economic:v1', ttl: CACHE_TTL },
    { key: 'correlation:disaster:v1', ttl: CACHE_TTL },
  ].map(ek => ({
    key: ek.key,
    ttl: ek.ttl,
    transform: (data) => data[ek.key.split(':')[1]],
  })),
}).catch((err) => {
  console.error('FATAL:', err.message || err);
  process.exit(1);
});
