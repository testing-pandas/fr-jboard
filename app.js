import 'dotenv/config';
import express from 'express';
import Database from 'better-sqlite3';
import slugify from 'slugify';
import { OpenAI } from 'openai';
import { convert } from 'html-to-text';
import cron from 'node-cron';
import crypto from 'node:crypto';
import sax from 'sax';
import fetch from 'node-fetch';

// ========================================
// VARIABLES D’ENVIRONNEMENT
// ========================================
const PORT = Number(process.env.PORT || 3000);
const SITE_URL = (process.env.SITE_URL || `http://localhost:${PORT}`).replace(/\/+$/,'');
const SITE_NAME = process.env.SITE_NAME || 'Emplois Conducteur Routier';
const FAVICON_URL = process.env.FAVICON_URL || '';
const SITE_LOGO = process.env.SITE_LOGO || '';
const SITE_SAMEAS = process.env.SITE_SAMEAS || ''; // URLs sociales séparées par des virgules
const TARGET_LANG = process.env.TARGET_LANG || 'fr';
const FEED_URL = process.env.Feed_URL || process.env.FEED_URL || '';
const MAX_JOBS = Number(process.env.MAX_JOBS || 50000);
const CRON_SCHEDULE = process.env.CRON_SCHEDULE || '0 */6 * * *';
const HAS_OPENAI = !!process.env.OPENAI_API_KEY;
const CLICK_SECRET = process.env.CLICK_SECRET || crypto.randomBytes(16).toString('hex');
const TARGET_PROFESSION = process.env.TARGET_PROFESSION || 'conducteur routier';
const AI_PROCESS_LIMIT = Number(process.env.AI_PROCESS_LIMIT || 0); // 0 = illimité

// Mots-clés (minuscule)
const PROFESSION_KEYWORDS = (process.env.PROFESSION_KEYWORDS ||
  'conducteur routier,chauffeur poids lourd,chauffeur spl,poids lourds,super poids lourd,permIs ce,permis c,fimo,fco,adr,matières dangereuses,semi-remorque,distribution,livraison,regional,national,international,travail de nuit,grumier,citerne,plateau,frigo,bâché')
  .toLowerCase()
  .split(',')
  .map(s => s.trim())
  .filter(Boolean);

// ========================================
// BASE DE DONNÉES
// ========================================
const db = new Database('jobs.db');
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('cache_size = -64000');

db.exec(`
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  guid TEXT UNIQUE,
  source TEXT,
  title TEXT,
  company TEXT,
  description_html TEXT,
  description_short TEXT,
  url TEXT,
  published_at INTEGER,
  slug TEXT UNIQUE,
  tags_csv TEXT DEFAULT '',
  created_at INTEGER DEFAULT (strftime('%s','now'))
);
CREATE INDEX IF NOT EXISTS idx_jobs_published ON jobs(published_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_slug ON jobs(slug);
CREATE INDEX IF NOT EXISTS idx_jobs_guid ON jobs(guid);

CREATE TABLE IF NOT EXISTS tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT UNIQUE,
  slug TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_tags_slug ON tags(slug);
CREATE INDEX IF NOT EXISTS idx_tags_name ON tags(name);

CREATE TABLE IF NOT EXISTS job_tags (
  job_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  UNIQUE(job_id, tag_id) ON CONFLICT IGNORE
);
CREATE INDEX IF NOT EXISTS idx_job_tags_job_id ON job_tags(job_id);
CREATE INDEX IF NOT EXISTS idx_job_tags_tag_id ON job_tags(tag_id);

CREATE TABLE IF NOT EXISTS stats_cache (
  key TEXT PRIMARY KEY,
  value INTEGER,
  updated_at INTEGER DEFAULT (strftime('%s','now'))
);
`);

// ========================================
// REQUÊTES PRÉPARÉES
// ========================================
const stmtInsertJob = db.prepare(`
INSERT OR IGNORE INTO jobs
(guid, source, title, company, description_html, description_short, url, published_at, slug, tags_csv)
VALUES (@guid, @source, @title, @company, @description_html, @description_short, @url, @published_at, @slug, @tags_csv)
`);
const stmtHasGuid = db.prepare(`SELECT id FROM jobs WHERE guid=? LIMIT 1`);
const stmtBySlug = db.prepare(`SELECT * FROM jobs WHERE slug=? LIMIT 1`);
const stmtById = db.prepare(`SELECT * FROM jobs WHERE id=? LIMIT 1`);

const stmtPageCursor = db.prepare(`
SELECT id, title, company, description_short, slug, published_at
FROM jobs
WHERE published_at < ? OR (published_at = ? AND id < ?)
ORDER BY published_at DESC, id DESC
LIMIT ?
`);
const stmtPageFirst = db.prepare(`
SELECT id, title, company, description_short, slug, published_at
FROM jobs
ORDER BY published_at DESC, id DESC
LIMIT ?
`);

const stmtSearch = db.prepare(`
SELECT id, title, company, description_short, slug, published_at
FROM jobs
WHERE title LIKE ? OR company LIKE ?
ORDER BY published_at DESC, id DESC
LIMIT 100
`);

const stmtGetTagBySlug = db.prepare(`SELECT * FROM tags WHERE slug=? LIMIT 1`);
const stmtGetTagByName = db.prepare(`SELECT * FROM tags WHERE name=? LIMIT 1`);
const stmtInsertTag = db.prepare(`INSERT OR IGNORE INTO tags (name, slug) VALUES (?, ?)`);
const stmtInsertJobTag = db.prepare(`INSERT OR IGNORE INTO job_tags (job_id, tag_id) VALUES (?, ?)`);
const stmtCountJobsByTagId = db.prepare(`SELECT COUNT(*) AS c FROM job_tags WHERE tag_id=?`);

const stmtJobsByTagCursor = db.prepare(`
SELECT j.id, j.title, j.company, j.description_short, j.slug, j.published_at
FROM jobs j
JOIN job_tags jt ON jt.job_id = j.id
JOIN tags t ON t.id = jt.tag_id
WHERE t.slug = ?
  AND (j.published_at < ? OR (j.published_at = ? AND j.id < ?))
ORDER BY j.published_at DESC, j.id DESC
LIMIT ?
`);
const stmtJobsByTagFirst = db.prepare(`
SELECT j.id, j.title, j.company, j.description_short, j.slug, j.published_at
FROM jobs j
JOIN job_tags jt ON jt.job_id = j.id
JOIN tags t ON t.id = jt.tag_id
WHERE t.slug = ?
ORDER BY j.published_at DESC, j.id DESC
LIMIT ?
`);

const stmtPopularTags = db.prepare(`
SELECT t.name, t.slug, COUNT(*) AS cnt
FROM tags t
JOIN job_tags jt ON jt.tag_id = t.id
GROUP BY t.id
HAVING cnt >= ?
ORDER BY cnt DESC, t.name ASC
LIMIT ?
`);
const stmtRecent = db.prepare(`
SELECT title, slug, published_at
FROM jobs
ORDER BY published_at DESC, id DESC
LIMIT ?
`);

const stmtGetCache = db.prepare(`SELECT value FROM stats_cache WHERE key=? AND updated_at > ?`);
const stmtSetCache = db.prepare(`
INSERT OR REPLACE INTO stats_cache (key, value, updated_at)
VALUES (?, ?, strftime('%s','now'))
`);

function getCachedCount(ttlSeconds = 300) {
  const cutoff = Math.floor(Date.now() / 1000) - ttlSeconds;
  const cached = stmtGetCache.get('total_jobs', cutoff);
  if (cached) return cached.value;
  const count = db.prepare(`SELECT COUNT(*) as c FROM jobs`).get().c;
  stmtSetCache.run('total_jobs', count);
  return count;
}

const stmtDeleteOld = db.prepare(`
DELETE FROM jobs
WHERE id IN (
  SELECT id FROM jobs
  ORDER BY published_at DESC, id DESC
  LIMIT -1 OFFSET ?
)
`);

// ========================================
// OUTILS
// ========================================
const openai = HAS_OPENAI ? new OpenAI({ apiKey: process.env.OPENAI_API_KEY }) : null;

const mkSlug = (s) => slugify(String(s || 'emploi'), { lower: true, strict: true }).slice(0, 120);
const unixtime = (d) => Math.floor(new Date(d).getTime() / 1000);

function truncateWords(txt, n = 60) {
  const words = (txt || '').split(/\s+/);
  if (words.length <= n) return txt || '';
  return words.slice(0, n).join(' ') + '…';
}

function uniqNormTags(tags = []) {
  const seen = new Set();
  const out = [];
  for (let t of tags) {
    if (!t) continue;
    t = String(t).trim().toLowerCase();
    if (!t) continue;
    if (seen.has(t)) continue;
    seen.add(t);
    out.push(t);
  }
  return out.slice(0, 8);
}
function tagSlug(t) { return mkSlug(t); }

function sanitizeHtml(html = '') {
  if (!html) return '';
  return String(html)
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
    .replace(/<\/?(?:iframe|object|embed|link|style|noscript)\b[^>]*>/gi, '')
    .replace(/\son\w+\s*=\s*["'][^"']*["']/gi, '')
    .replace(/\son\w+\s*=\s*[^\s>]+/gi, '')
    .replace(/\s(href|src)\s*=\s*["']\s*javascript:[^"']*["']/gi, '')
    .replace(/\s(href|src)\s*=\s*javascript:[^\s>]+/gi, '');
}

function stripDocumentTags(html = '') {
  if (!html) return '';
  return String(html)
    .replace(/<!DOCTYPE[^>]*>/gi, '')
    .replace(/<\/?html[^>]*>/gi, '')
    .replace(/<\/?head[^>]*>/gi, '')
    .replace(/<\/?body[^>]*>/gi, '')
    .replace(/<meta[^>]*>/gi, '')
    .replace(/<title[^>]*>.*?<\/title>/gi, '')
    .trim();
}

function escapeHtml(s = '') {
  return String(s).replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[c]));
}

function canonical(path = '') {
  const p = String(path || '');
  if (/^https?:\/\//i.test(p)) return p;
  return `${SITE_URL}${p.startsWith('/') ? '' : '/'}${p}`;
}

function matchesProfession(title = '', company = '', description = '') {
  const text = `${title} ${company} ${description}`.toLowerCase();
  return PROFESSION_KEYWORDS.some(keyword => text.includes(keyword));
}

// Tags par profession (FR)
const PROFESSION_TAGS = {
  'conducteur routier': [
    'permis ce','permis c','spl','poids lourd','super poids lourd',
    'fimo','fco','adr','matières dangereuses','tachygraphe',
    'semi-remorque','remorque','solo','plateau','citerne','bâché','frigorifique',
    'distribution','navettes','régional','national','international',
    'travail de nuit','week-end','rotation','planification de tournée'
  ],
  'truck driver': ['class 1', 'class a', 'cdl', 'long haul', 'regional', 'local', 'tanker', 'flatbed', 'otr', 'hazmat'],
  'chauffeur poids lourd': ['pl','spl','permIs c','permis ce','fimo','fco'],
  'warehouse': ['chariot','cariste','préparateur','expédition','réception','inventaire']
};

function extractTags({ title = '', company = '', html = '' }) {
  const text = `${title} ${company} ${convert(html || '', { wordwrap: 120 }).slice(0, 1000)}`.toLowerCase();
  const profKey = TARGET_PROFESSION.toLowerCase();
  const profTags = PROFESSION_TAGS[profKey] || PROFESSION_TAGS['conducteur routier'] || [];
  const found = profTags.filter(tag => text.includes(tag));

  if (/(télétravail|remote|travail à distance)/i.test(text)) found.push('télétravail');
  if (/(temps plein|full[-\s]?time|plein temps)/i.test(text)) found.push('temps plein');
  if (/(temps partiel|part[-\s]?time|mi-temps)/i.test(text)) found.push('temps partiel');
  if (/(cdi|permanent)/i.test(text)) found.push('cdi');
  if (/(cdd|intérim|interim|contrat|temporaire)/i.test(text)) found.push('contrat');

  found.push(TARGET_PROFESSION.toLowerCase());

  return uniqNormTags(found);
}

// Analyse méta (FR + EN)
function parseMeta(textHTML = '', title = '') {
  const text = (convert(textHTML || '', { wordwrap: 1000 }) + ' ' + (title || '')).toLowerCase();

  let employmentType = 'FULL_TIME';
  if (/(temps partiel|part[-\s]?time|mi-temps)\b/i.test(text)) employmentType = 'PART_TIME';
  else if (/(cdd|intérim|interim|contrat|contractor|zeitarbeit|befristet)\b/i.test(text)) employmentType = 'CONTRACTOR';
  else if (/(stage|stagiaire|internship|praktikum)\b/i.test(text)) employmentType = 'INTERN';
  else if (/(temporaire|seasonal|saison)/i.test(text)) employmentType = 'TEMPORARY';

  const isRemote = /(télétravail|remote|travail à distance|home office|homeoffice|telecommute)/i.test(text);

  // Unité de salaire
  let unit = 'HOUR';
  if (/\b(an|année|annuel|par an|year|annually|per year)\b/i.test(text)) unit = 'YEAR';
  else if (/\b(mois|mensuel|par mois|month|monthly|per month)\b/i.test(text)) unit = 'MONTH';
  else if (/\b(semaine|hebdo|par semaine|week|weekly|per week)\b/i.test(text)) unit = 'WEEK';
  else if (/\b(jour|journalier|par jour|day|daily|per day)\b/i.test(text)) unit = 'DAY';
  else if (/\b(heure|horaire|par heure|hour|hourly|per hour)\b/i.test(text)) unit = 'HOUR';

  // Devise & plages
  let currency = null, min = null, max = null;
  const cMatch = text.match(/\b(eur|euro|usd|chf|gbp)\b|[€$£]/i);
  if (cMatch) {
    const c = cMatch[0].toUpperCase();
    currency = (c === '€' || c === 'EUR' || c === 'EURO') ? 'EUR'
      : (c === '$' || c === 'USD') ? 'USD'
      : (c === '£' || c === 'GBP') ? 'GBP'
      : (c === 'CHF') ? 'CHF' : null;
  }
  const range = text.match(/(\d{1,2}[.,]?\d{3,6})\s*[-–—à]\s*(\d{1,2}[.,]?\d{3,6})/i);
  if (range) {
    min = Number(range[1].replace(/[.,]/g, ''));
    max = Number(range[2].replace(/[.,]/g, ''));
  } else {
    const one = text.match(/(?:à partir de|dès|from|de|von)\s*(\d{1,2}[.,]?\d{3,6})|(\d{1,2}[.,]?\d{3,6})\s*(?:\+|jusqu'?à|bis)/i);
    if (one) {
      const val = one[1] || one[2];
      min = Number(String(val || '').replace(/[.,]/g, ''));
    }
  }

  // Expérience
  let experienceRequirements = null;
  const yearsMatch =
    text.match(/(\d+)\s*\+?\s*(ans|an|years|yrs)\s*(d['’]expérience|experience)/i);
  if (yearsMatch) {
    const yrs = yearsMatch[1];
    experienceRequirements = `${yrs} ans d'expérience pertinente`;
  } else if (/(expérience exigée|expérience requise|experience required)/i.test(text)) {
    experienceRequirements = `Expérience pertinente requise`;
  }
  const experienceInPlaceOfEducation =
    /(ou expérience équivalente|equivalent experience|gleichwertige erfahrung)/i.test(text) ? true : false;

  return {
    employmentType,
    isRemote,
    salary: (currency && (min || max)) ? { currency, min, max, unit } : null,
    experienceRequirements,
    experienceInPlaceOfEducation
  };
}

// Inférence pays depuis le domaine
function getCountryFromHost(siteUrl) {
  try {
    const host = new URL(siteUrl).hostname.toLowerCase();
    const tld = host.split('.').pop();
    const map = {
      fr: 'FR', be: 'BE', ch: 'CH', lu: 'LU', ca: 'CA',
      de: 'DE', at: 'AT', li: 'LI',
      uk: 'GB', gb: 'GB', ie: 'IE',
      us: 'US', au: 'AU', nz: 'NZ',
      nl: 'NL', es: 'ES', pt: 'PT', it: 'IT'
    };
    return map[tld] || 'FR';
  } catch {
    return 'FR';
  }
}

/**
 * Toujours renvoyer un jobLocation valide.
 */
function inferJobLocations(html = '', title = '', siteUrl = SITE_URL) {
  const country = getCountryFromHost(siteUrl);
  const text = (convert(html || '', { wordwrap: 1000 }) + ' ' + (title || '')).toLowerCase();

  const citiesFR = ['paris','lyon','marseille','toulouse','lille','bordeaux','nantes','strasbourg','rennes','montpellier','nice','grenoble','dijon','angers','tours','reims','saint-étienne','toulon','le havre','clermont-ferrand'];
  let city = null;
  for (const c of citiesFR) {
    if (text.includes(c)) { city = c; break; }
  }

  const address = city
    ? { "@type": "PostalAddress", "addressLocality": city[0].toUpperCase() + city.slice(1), "addressCountry": country }
    : { "@type": "PostalAddress", "addressCountry": country };

  return [{
    "@type": "Place",
    "address": address
  }];
}

const UNIT_LABELS = { YEAR: 'an', MONTH: 'mois', WEEK: 'semaine', DAY: 'jour', HOUR: 'heure' };

// Contenu AI
async function rewriteJobRich({ title, company, html }, useAI = false) {
  const plain = convert(html || '', {
    wordwrap: 120,
    selectors: [{ selector: 'a', options: { ignoreHref: true } }]
  }).slice(0, 9000);

  const fallback = () => {
    const paragraphs = plain.split(/\n+/).filter(Boolean).slice(0, 6).map(p => `<p>${escapeHtml(p)}</p>`).join('\n');
    const fallbackHTML = `
<section><h2>À propos du poste</h2>${paragraphs || '<p>Détails fournis par l’employeur.</p>'}</section>
<section><h2>Responsabilités</h2><ul><li>Réaliser les missions décrites.</li></ul></section>
<section><h2>Profil recherché</h2><ul><li>Expérience pertinente ou motivation à apprendre.</li></ul></section>
<section><h2>Avantages</h2><ul><li>Avantages selon la description.</li></ul></section>
<section><h2>Rémunération</h2><p>À discuter.</p></section>
<section><h2>Lieu & Horaires</h2><p>Selon la description.</p></section>
<section><h2>Candidater</h2><p>Utilisez le bouton “Postuler”.</p></section>
`.trim();

    return {
      short: truncateWords(plain, 45),
      html: sanitizeHtml(fallbackHTML),
      tags: extractTags({ title, company, html }),
      usedAI: false
    };
  };

  if (!HAS_OPENAI || !useAI || !openai) return fallback();

  const system = `
Tu es un éditeur senior de contenus d'offres pour ${TARGET_PROFESSION}. Rédige naturellement en ${TARGET_LANG}.
CONTRAT DE SORTIE — renvoie EXACTEMENT ces trois blocs dans cet ordre :
===DESCRIPTION=== [35–60 mots en texte brut. Pas de HTML, guillemets, emojis.]
===HTML=== [Fragments HTML propres ; JAMAIS <!DOCTYPE>, <html>, <head> ou <body>.]
===TAGS=== [Tableau JSON valide (3–8 éléments), tout en minuscules, en ${TARGET_LANG}, pertinent pour ${TARGET_PROFESSION}.]

SECTIONS HTML (titres localisés en ${TARGET_LANG}, ordre strict) :
1) À propos du poste
2) Responsabilités
3) Profil recherché
4) Avantages
5) Rémunération
6) Lieu & Horaires
7) Candidater

RÈGLES HTML :
- Balises sémantiques uniquement : <section>, <h2>, <p>, <ul>, <li>, <strong>, <em>, <time>, <address>.
- 7 sections exactement avec ces <h2>, aucune section vide.
- Listes scannables : 5–8 puces, 4–12 mots/puce.
- Pas de styles inline, scripts, images, tableaux.
- Pas de lien externe sauf si un lien explicite de candidature est fourni.
- Unités métriques et formats locaux.

CONTENU :
- DESCRIPTION : 35–60 mots, voix active, bénéfices concrets, zéro fluff.
- Responsabilités & Profil : résultats concrets, outils, indispensables.
- Avantages : réalistes.
- Rémunération : fourchette si connue ; sinon “À discuter”.
- Lieu & Horaires : refléter les infos connues ; sinon générique.
- Candidater : une phrase simple, sans lien sauf s’il est fourni.

VALIDATION STRICTE :
- DESCRIPTION 35–60 mots, sans HTML.
- HTML = 7 sections exactes.
- TAGS JSON valide (3–8), tous pertinents.
- Pas d’invention de faits ou de liens.
`;
  const user = `Offre: ${title || 'N/A'}
Entreprise: ${company || 'N/A'}
Texte:
${plain}`;

  try {
    const resp = await openai.chat.completions.create({
      model: "gpt-4o-mini",
      temperature: 0.2,
      messages: [
        { role: "system", content: system },
        { role: "user", content: user }
      ]
    });

    const out = resp.choices?.[0]?.message?.content || '';
    const descMatch = out.match(/===DESCRIPTION===\s*([\s\S]*?)\s*===HTML===/i);
    const htmlMatch = out.match(/===HTML===\s*([\s\S]*?)\s*===TAGS===/i);
    const tagsMatch = out.match(/===TAGS===\s*([\s\S]*)$/i);

    let short = (descMatch?.[1] || '').trim();
    if (!short) short = convert(out, { wordwrap: 120 }).slice(0, 300);
    short = convert(short, { wordwrap: 120 }).trim().slice(0, 600);

    let htmlOut = (htmlMatch?.[1] || '').trim();
    if (!htmlOut) {
      htmlOut = `<section><h2>À propos du poste</h2><p>${escapeHtml(short)}</p></section>`;
    }
    htmlOut = stripDocumentTags(htmlOut);
    if (htmlOut.length < 50) {
      htmlOut = `<section><h2>À propos du poste</h2><p>${escapeHtml(short)}</p></section>`;
    }

    let tagsParsed = null;
    try {
      const m = (tagsMatch?.[1] || '').match(/\[[\s\S]*\]/);
      if (m) tagsParsed = JSON.parse(m[0]);
    } catch { /* noop */ }

    const tags = uniqNormTags(tagsParsed || extractTags({ title, company, html }));

    return { short, html: sanitizeHtml(htmlOut), tags, usedAI: true };
  } catch (e) {
    console.error('OpenAI error:', e.message);
    return fallback();
  }
}

function upsertTagsForJob(jobId, tags = []) {
  const insertTag = db.transaction((names) => {
    for (const name of names) {
      const slug = tagSlug(name);
      stmtInsertTag.run(name, slug);
      const t = stmtGetTagByName.get(name);
      if (t) stmtInsertJobTag.run(jobId, t.id);
    }
  });
  insertTag(tags);
}

// ========================================
// TRAITEMENT DU FLUX (avec limite IA)
// ========================================
let FEED_RUNNING = false;

export async function processFeed() {
  if (FEED_RUNNING) {
    console.log('Traitement du flux déjà en cours, on ignore…');
    return;
  }
  if (!FEED_URL) {
    console.log('Aucune FEED_URL configurée');
    return;
  }

  FEED_RUNNING = true;
  try {
    console.log(`\nRécupération du flux XML : ${FEED_URL}`);
    console.log(`Filtrage pour la profession : ${TARGET_PROFESSION}`);
    console.log(`Mots-clés : ${PROFESSION_KEYWORDS.join(', ')}`);
    console.log(`Traitement IA : ${AI_PROCESS_LIMIT === 0 ? 'Illimité' : `Premières ${AI_PROCESS_LIMIT} offres`}`);
    console.log('Démarrage du parseur XML en streaming…\n');

    const response = await fetch(FEED_URL);
    const stream = response.body;

    let matched = 0;
    let processed = 0;
    let skipped = 0;
    let aiEnhanced = 0;
    let fallbackUsed = 0;

    const batchSize = 100;
    const insertBatch = db.transaction((jobs) => {
      for (const job of jobs) {
        stmtInsertJob.run(job);
        const inserted = stmtHasGuid.get(job.guid);
        if (inserted) {
          upsertTagsForJob(inserted.id, job.tags_csv.split(', ').filter(Boolean));
        }
      }
    });

    let batch = [];
    let currentItem = null;
    let currentTag = '';
    let currentText = '';

    const parser = sax.createStream(true, { trim: true, normalize: true });

    parser.on('opentag', (node) => {
      currentTag = node.name.toLowerCase();
      currentText = '';
      if (currentTag === 'job' || currentTag === 'item') {
        currentItem = { title: '', description: '', company: '', link: '', guid: '', pubDate: new Date().toISOString() };
      }
    });

    parser.on('text', (text) => { currentText += text; });
    parser.on('cdata', (text) => { currentText += text; });

    parser.on('closetag', (tagName) => {
      tagName = tagName.toLowerCase();
      if (!currentItem) return;

      switch (tagName) {
        case 'title': currentItem.title = currentText.trim(); break;
        case 'description': currentItem.description = currentText.trim(); break;
        case 'company': currentItem.company = currentText.trim(); break;
        case 'url':
        case 'link': currentItem.link = currentText.trim(); break;
        case 'guid':
        case 'referencenumber':
          if (!currentItem.guid) currentItem.guid = currentText.trim();
          break;
        case 'pubdate':
        case 'date_updated': currentItem.pubDate = currentText.trim(); break;
      }

      if (tagName === 'job' || tagName === 'item') {
        processed++;
        if (processed % 10000 === 0) {
          console.log(`Traité ${processed.toLocaleString()} éléments (retenus : ${matched.toLocaleString()}, ignorés : ${skipped.toLocaleString()})`);
        }

        const guid = currentItem.guid || currentItem.link || `job-${processed}`;
        if (stmtHasGuid.get(guid)) {
          skipped++;
          currentItem = null;
          return;
        }
        if (!matchesProfession(currentItem.title, currentItem.company, currentItem.description)) {
          skipped++;
          currentItem = null;
          return;
        }

        matched++;
        batch.push({
          rawTitle: currentItem.title,
          rawCompany: currentItem.company,
          rawDescription: currentItem.description,
          guid,
          source: new URL(FEED_URL).hostname,
          url: currentItem.link,
          published_at: unixtime(currentItem.pubDate)
        });
        currentItem = null;
      }
    });

    parser.on('error', (err) => {
      console.error('Erreur SAX :', err.message);
    });

    await new Promise((resolve, reject) => {
      stream.pipe(parser);

      parser.on('end', async () => {
        if (batch.length > 0) {
          console.log(`\nTraitement de ${batch.length} offres retenues…`);
          const processedBatch = [];
          for (let i = 0; i < batch.length; i++) {
            const rawJob = batch[i];
            const shouldUseAI = (AI_PROCESS_LIMIT === 0) || (aiEnhanced < AI_PROCESS_LIMIT);
            const { short, html, tags, usedAI } = await rewriteJobRich(
              { title: rawJob.rawTitle, company: rawJob.rawCompany, html: rawJob.rawDescription },
              shouldUseAI
            );

            if (usedAI) {
              aiEnhanced++;
              if (aiEnhanced % 10 === 0) console.log(`IA appliquée : ${aiEnhanced} offres…`);
            } else {
              fallbackUsed++;
            }

            const slug = mkSlug(`${rawJob.rawTitle}-${rawJob.rawCompany}`) || mkSlug(rawJob.rawTitle) || mkSlug(rawJob.guid);

            processedBatch.push({
              guid: rawJob.guid,
              source: rawJob.source,
              title: rawJob.rawTitle || 'Sans titre',
              company: rawJob.rawCompany || '',
              description_html: html,
              description_short: truncateWords(short, 60),
              url: rawJob.url || '',
              published_at: rawJob.published_at,
              slug,
              tags_csv: tags.join(', ')
            });

            if (processedBatch.length >= batchSize) {
              insertBatch(processedBatch);
              processedBatch.length = 0;
            }
          }
          if (processedBatch.length > 0) {
            insertBatch(processedBatch);
          }
        }
        resolve();
      });

      parser.on('error', reject);
      stream.on('error', reject);
    });

    console.log(`\nFlux traité !`);
    console.log(`Total éléments : ${processed.toLocaleString()}`);
    console.log(`Offres retenues : ${matched.toLocaleString()}`);
    console.log(`IA appliquée : ${aiEnhanced.toLocaleString()}`);
    console.log(`Fallback rapide : ${fallbackUsed.toLocaleString()}`);
    console.log(`Ignorés : ${skipped.toLocaleString()} (doublons/non pertinents)\n`);

    const total = getCachedCount(0);
    if (total > MAX_JOBS) {
      console.log(`Nettoyage : on garde ${MAX_JOBS.toLocaleString()} offres les plus récentes`);
      stmtDeleteOld.run(MAX_JOBS);
      stmtSetCache.run('total_jobs', MAX_JOBS);
    }
  } catch (error) {
    console.error('Erreur de traitement du flux :', error.message);
    throw error;
  } finally {
    FEED_RUNNING = false;
  }
}

// ========================================
// CSS (thème bleu moderne) + bandeau cookies
// ========================================
const baseCss = `
:root {
  --primary: #2563eb;
  --primary-dark: #1e40af;
  --bg: #f8fafc;
  --card: #ffffff;
  --text: #1e293b;
  --text-muted: #64748b;
  --border: #e2e8f0;
  --shadow: 0 1px 3px rgba(0,0,0,0.1);
  --shadow-lg: 0 4px 6px -1px rgba(0,0,0,0.1), 0 2px 4px -1px rgba(0,0,0,0.06);
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font-family: system-ui, -apple-system, 'Segoe UI', Roboto, Arial, sans-serif; line-height: 1.6; }
.wrap { max-width: 1000px; margin: 0 auto; padding: 20px; }
header.wrap { display: flex; justify-content: space-between; align-items: center; padding-top: 24px; padding-bottom: 24px; border-bottom: 1px solid var(--border); background: var(--card); flex-wrap: wrap; gap: 16px; }
header h1 { margin: 0; font-size: 24px; }
header h1 a { color: var(--text); text-decoration: none; font-weight: 700; }
nav { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
nav a { color: var(--text-muted); text-decoration: none; padding: 8px 12px; border-radius: 6px; transition: all 0.2s; }
nav a:hover { color: var(--primary); background: var(--bg); }
.btn { display: inline-block; padding: 10px 18px; background: var(--text); color: white; border-radius: 8px; border: none; cursor: pointer; font-size: 14px; font-weight: 500; text-decoration: none; transition: all 0.2s; }
.btn:hover { background: var(--text-muted); transform: translateY(-1px); }
.btn-primary { background: var(--primary); color: white; font-weight: 600; }
.btn-primary:hover { background: var(--primary-dark); }
.card { background: var(--card); border-radius: 12px; padding: 24px; margin: 16px 0; box-shadow: var(--shadow); border: 1px solid var(--border); transition: all 0.2s; }
.card:hover { box-shadow: var(--shadow-lg); border-color: var(--primary); }
.list { list-style: none; padding: 0; margin: 0; }
.muted { color: var(--text-muted); }
.small { font-size: 14px; }
.search-form { margin: 24px 0; }
.search-form input[type="search"] { width: 100%; max-width: 500px; padding: 12px 16px; border: 2px solid var(--border); border-radius: 8px; font-size: 16px; transition: all 0.2s; }
.search-form input[type="search"]:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }
.pager { display: flex; gap: 12px; margin: 24px 0; flex-wrap: wrap; }
.pager a, .pager .current { padding: 8px 16px; background: var(--card); border-radius: 8px; color: var(--text); text-decoration: none; box-shadow: var(--shadow); border: 1px solid var(--border); transition: all 0.2s; }
.pager a:hover { background: var(--primary); color: white; border-color: var(--primary); }
.pager .disabled { opacity: 0.5; pointer-events: none; }
.tags { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }
.tag { background: #eff6ff; color: var(--primary); border-radius: 999px; padding: 6px 14px; font-size: 13px; text-decoration: none; transition: all 0.2s; border: 1px solid #dbeafe; }
.tag:hover { background: var(--primary); color: white; border-color: var(--primary); }
.content h2 { color: var(--text); margin-top: 24px; font-size: 20px; }
.content p, .content ul, .content ol { line-height: 1.7; margin: 12px 0; }
.content ul, .content ol { padding-left: 24px; }
form label { display: block; margin-top: 16px; margin-bottom: 6px; font-weight: 500; color: var(--text); }
form input[type="text"], form input[type="url"], form input[type="number"], form select, form textarea { width: 100%; padding: 10px 14px; border: 2px solid var(--border); border-radius: 8px; font-size: 15px; font-family: inherit; transition: all 0.2s; }
form input:focus, form select:focus, form textarea:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1); }
form textarea { min-height: 150px; resize: vertical; }
form button[type="submit"] { margin-top: 20px; }
.form-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; }
.help-text { font-size: 13px; color: var(--text-muted); margin-top: 4px; }
footer { margin-top: 60px; padding-top: 24px; border-top: 1px solid var(--border); }
/* Bandeau cookies */
.cookie-banner { position: fixed; left: 16px; right: 16px; bottom: 16px; z-index: 9999; background: var(--card); color: var(--text); border: 1px solid var(--border); box-shadow: var(--shadow-lg); border-radius: 12px; padding: 16px; display: none; }
.cookie-actions { display: flex; gap: 8px; margin-top: 12px; flex-wrap: wrap; }
.cookie-link { color: var(--primary); text-decoration: none; }
.cookie-link:hover { text-decoration: underline; }
@media (max-width: 768px) {
  header.wrap { flex-direction: column; align-items: flex-start; }
  nav { width: 100%; justify-content: flex-start; }
  .form-row { grid-template-columns: 1fr; }
}
`;

// ========================================
// GABARIT HTML (avec bandeau cookies)
// ========================================
function layout({ title, body, metaExtra = '', breadcrumbs = null }) {
  const faviconHtml = FAVICON_URL ? `<link rel="icon" href="${escapeHtml(FAVICON_URL)}"/>` : '';
  const canonicalUrl = canonical(breadcrumbs ? breadcrumbs[breadcrumbs.length - 1].url : '/');

  let breadcrumbSchema = '';
  if (breadcrumbs && breadcrumbs.length > 1) {
    breadcrumbSchema = `<script type="application/ld+json">${JSON.stringify({
      "@context": "https://schema.org",
      "@type": "BreadcrumbList",
      "itemListElement": breadcrumbs.map((crumb, idx) => ({
        "@type": "ListItem",
        "position": idx + 1,
        "name": crumb.name,
        "item": canonical(crumb.url)
      }))
    })}</script>`;
  }

  const cookieBanner = `
<div id="cookie-banner" class="cookie-banner" role="dialog" aria-live="polite" aria-label="Consentement aux cookies">
  <div>
    <strong>Nous utilisons des cookies</strong>
    <p class="small muted" style="margin:6px 0 0 0;">
      Nous utilisons des cookies essentiels pour faire fonctionner ${escapeHtml(SITE_NAME)} et améliorer votre expérience.
      Consultez notre <a class="cookie-link" href="/cookies">Politique Cookies</a> et notre <a class="cookie-link" href="/privacy">Politique de confidentialité</a>.
    </p>
    <div class="cookie-actions">
      <button id="cookie-accept" class="btn btn-primary">Tout accepter</button>
      <a class="cookie-link" href="/cookies">Gérer les paramètres</a>
    </div>
  </div>
</div>
<script>
(function(){
  function getCookie(name){
    return document.cookie.split('; ').find(row => row.startsWith(name + '='))?.split('=')[1];
  }
  function showBanner(){
    var el = document.getElementById('cookie-banner');
    if (el) el.style.display = 'block';
  }
  function hideBanner(){
    var el = document.getElementById('cookie-banner');
    if (el) el.style.display = 'none';
  }
  if (!getCookie('cookie_consent')){
    window.addEventListener('load', showBanner);
  }
  var btn = document.getElementById('cookie-accept');
  if (btn){
    btn.addEventListener('click', function(){
      var oneYear = 365*24*60*60;
      document.cookie = 'cookie_consent=1; Max-Age=' + oneYear + '; Path=/; SameSite=Lax';
      hideBanner();
    });
  }
})();
</script>
`;

  return `
<!doctype html>
<html lang="${TARGET_LANG}">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>${title ? `${escapeHtml(title)} · ` : ''}${escapeHtml(SITE_NAME)}</title>
<meta name="description" content="Trouvez des emplois de ${escapeHtml(TARGET_PROFESSION)} sur ${escapeHtml(SITE_NAME)}"/>
<link rel="canonical" href="${canonicalUrl}"/>
${faviconHtml}
<link rel="alternate" type="application/rss+xml" title="Flux RSS" href="${canonical('/feed.xml')}"/>
<!-- Open Graph -->
<meta property="og:title" content="${escapeHtml(title || SITE_NAME)}"/>
<meta property="og:description" content="Offres récentes pour ${escapeHtml(TARGET_PROFESSION)}"/>
<meta property="og:url" content="${canonicalUrl}"/>
<meta property="og:type" content="website"/>
${SITE_LOGO ? `<meta property="og:image" content="${escapeHtml(SITE_LOGO)}"/>` : ''}
<!-- Twitter Card -->
<meta name="twitter:card" content="summary"/>
<meta name="twitter:title" content="${escapeHtml(title || SITE_NAME)}"/>
<meta name="twitter:description" content="Trouver des offres pour ${escapeHtml(TARGET_PROFESSION)}"/>
<style>${baseCss}</style>
${breadcrumbSchema}
${metaExtra}
</head>
<body>
<header class="wrap">
  <h1><a href="/">${escapeHtml(SITE_NAME)}</a></h1>
  <nav>
    <a href="/post-job" class="btn btn-primary">Publier une offre</a>
    <a href="/tags">Tags</a>
    <a href="/feed.xml">RSS</a>
    <a href="/rules">Règles</a>
    <a href="/privacy">Confidentialité</a>
    <a href="/terms">Conditions</a>
  </nav>
</header>
<main class="wrap">
${body}
</main>
<footer class="wrap">
  <p class="muted small">© ${new Date().getFullYear()} ${escapeHtml(SITE_NAME)} · Offres ${escapeHtml(TARGET_PROFESSION)} · <a href="/privacy">Confidentialité</a> · <a href="/terms">Conditions</a> · <a href="/cookies">Cookies</a></p>
</footer>
${cookieBanner}
</body>
</html>
`;
}

// ========================================
// SERVEUR HTTP
// ========================================
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());
app.use(express.static('public'));

// Santé
app.get('/healthz', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString(), jobs: getCachedCount(), feedRunning: FEED_RUNNING, aiEnabled: HAS_OPENAI });
});

// ACCUEIL
app.get('/', (req, res) => {
  const pageSize = 50;
  const cursor = req.query.cursor || '';
  let rows;
  if (!cursor) {
    rows = stmtPageFirst.all(pageSize);
  } else {
    const [pub, id] = cursor.split('-').map(Number);
    if (!pub || !id) return res.status(400).send('Curseur invalide');
    rows = stmtPageCursor.all(pub, pub, id, pageSize);
  }
  const total = getCachedCount();
  const hasMore = rows.length === pageSize;
  const nextCursor = hasMore ? `${rows[rows.length - 1].published_at}-${rows[rows.length - 1].id}` : null;

  const items = rows.map(r => `
<li class="card">
  <h2><a href="/job/${r.slug}">${escapeHtml(r.title)}</a></h2>
  ${r.company ? `<div class="muted">${escapeHtml(r.company)}</div>` : ''}
  <p>${escapeHtml(r.description_short)}</p>
  <div class="muted small">${new Date(r.published_at * 1000).toLocaleDateString('fr-FR')}</div>
</li>`).join('');

  const popular = stmtPopularTags.all(5, 50);
  const tagsBlock = popular.length ? `
<section>
  <h3>Tags populaires</h3>
  <div class="tags">
    ${popular.map(t => `<a class="tag" href="/tag/${t.slug}">${escapeHtml(t.name)} (${t.cnt})</a>`).join('')}
  </div>
</section>` : '';

  const pagerLinks = [];
  if (nextCursor) {
    res.setHeader('Link', `<${canonical('/?cursor=' + nextCursor)}>; rel="next"`);
    pagerLinks.push(`<a href="/?cursor=${nextCursor}" rel="next">Suivant →</a>`);
  }
  if (cursor) {
    pagerLinks.unshift(`<a href="/" rel="prev">← Début</a>`);
  }
  const pager = pagerLinks.length ? `<div class="pager">${pagerLinks.join('')}</div>` : '';

  const orgSchema = `<script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Organization",
    "name": SITE_NAME,
    "url": SITE_URL,
    ...(SITE_LOGO ? { "logo": SITE_LOGO } : {}),
    ...(SITE_SAMEAS ? { "sameAs": SITE_SAMEAS.split(',').map(s => s.trim()).filter(Boolean) } : {})
  })}</script>`;

  const websiteSchema = `<script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "WebSite",
    "name": SITE_NAME,
    "url": SITE_URL,
    "potentialAction": {
      "@type": "SearchAction",
      "target": { "@type": "EntryPoint", "urlTemplate": `${SITE_URL}/search?q={search_term_string}` },
      "query-input": "required name=search_term_string"
    }
  })}</script>`;

  res.send(layout({
    title: 'Dernières offres',
    body: `
<section class="card search-form">
  <form method="GET" action="/search">
    <label for="q">Rechercher une offre</label>
    <input type="search" id="q" name="q" placeholder="Titre ou entreprise…" required/>
    <button type="submit" class="btn" style="margin-top:12px">Rechercher</button>
  </form>
</section>
<p class="muted">Affichage des postes ${escapeHtml(TARGET_PROFESSION)} · ${total.toLocaleString('fr-FR')} offres au total</p>
${tagsBlock}
<ul class="list">${items || '<li class="card">Aucune offre pour le moment. Rendez-vous sur /fetch pour importer.</li>'}</ul>
${pager}
`,
    metaExtra: orgSchema + websiteSchema
  }));
});

// RECHERCHE — noindex
app.get('/search', (req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  const q = String(req.query.q || '').trim();
  if (!q) return res.redirect('/');
  const searchPattern = `%${q}%`;
  const rows = stmtSearch.all(searchPattern, searchPattern);

  const items = rows.map(r => `
<li class="card">
  <h2><a href="/job/${r.slug}">${escapeHtml(r.title)}</a></h2>
  ${r.company ? `<div class="muted">${escapeHtml(r.company)}</div>` : ''}
  <p>${escapeHtml(r.description_short)}</p>
  <div class="muted small">${new Date(r.published_at * 1000).toLocaleDateString('fr-FR')}</div>
</li>`).join('');

  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Recherche', url: `/search?q=${encodeURIComponent(q)}` }
  ];

  res.send(layout({
    title: `Recherche : ${q}`,
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Recherche</nav>
<h1>Recherche : "${escapeHtml(q)}"</h1>
<p class="muted">${rows.length} résultats</p>
<ul class="list">${items || '<li class="card">Aucun résultat.</li>'}</ul>
<p><a href="/">← Retour aux offres</a></p>
`,
    breadcrumbs,
    metaExtra: `<meta name="robots" content="noindex, nofollow"/>`
  }));
});

// PUBLIER UNE OFFRE (GET)
app.get('/post-job', (req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Publier une offre', url: '/post-job' }
  ];
  res.send(layout({
    title: 'Publier une offre',
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Publier une offre</nav>
<article class="card">
  <h1>Publier une offre</h1>
  <p>Publiez votre offre ${escapeHtml(TARGET_PROFESSION)}. Les champs marqués d’un * sont obligatoires.</p>
  <form method="POST" action="/post-job">
    <label for="title">Intitulé du poste *</label>
    <input type="text" id="title" name="title" required placeholder="ex. Conducteur SPL"/>

    <label for="company">Entreprise *</label>
    <input type="text" id="company" name="company" required placeholder="ex. ABC Logistique"/>

    <label for="url">URL de candidature *</label>
    <input type="url" id="url" name="url" required placeholder="https://..."/>
    <div class="help-text">Lien où les candidat·e·s postulent</div>

    <label for="description">Description (optionnel)</label>
    <textarea id="description" name="description" placeholder="Si vide, l’IA générera une description structurée…"></textarea>
    <div class="help-text">Laissez vide pour contenu IA, ou fournissez votre HTML/texte</div>

    <label for="tags">Tags (optionnel)</label>
    <input type="text" id="tags" name="tags" placeholder="ex. télétravail, temps plein, adr"/>
    <div class="help-text">Séparés par des virgules</div>

    <div class="form-row">
      <div>
        <label for="employmentType">Type de contrat</label>
        <select id="employmentType" name="employmentType">
          <option value="FULL_TIME">Temps plein</option>
          <option value="PART_TIME">Temps partiel</option>
          <option value="CONTRACTOR">Contrat</option>
          <option value="TEMPORARY">Temporaire</option>
          <option value="INTERN">Stage</option>
        </select>
      </div>
      <div>
        <label for="isRemote">Télétravail</label>
        <select id="isRemote" name="isRemote">
          <option value="no">Non</option>
          <option value="yes">Oui</option>
        </select>
      </div>
    </div>

    <h3 style="margin-top:24px">Salaire (optionnel)</h3>
    <div class="form-row">
      <div>
        <label for="currency">Devise</label>
        <select id="currency" name="currency">
          <option value="">Aucune</option>
          <option value="EUR">EUR</option>
          <option value="CHF">CHF</option>
          <option value="USD">USD</option>
          <option value="GBP">GBP</option>
        </select>
      </div>
      <div>
        <label for="salaryMin">Minimum</label>
        <input type="number" id="salaryMin" name="salaryMin" placeholder="ex. 28000"/>
      </div>
      <div>
        <label for="salaryMax">Maximum</label>
        <input type="number" id="salaryMax" name="salaryMax" placeholder="ex. 35000"/>
      </div>
      <div>
        <label for="salaryUnit">Par</label>
        <select id="salaryUnit" name="salaryUnit">
          <option value="YEAR">An</option>
          <option value="MONTH">Mois</option>
          <option value="WEEK">Semaine</option>
          <option value="DAY">Jour</option>
          <option value="HOUR">Heure</option>
        </select>
      </div>
    </div>

    <button type="submit" class="btn btn-primary">Soumettre l’offre</button>
  </form>
</article>
`,
    breadcrumbs
  }));
});

// PUBLIER UNE OFFRE (POST)
app.post('/post-job', async (req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  try {
    const {
      title, company, url,
      description = '', tags = '',
      employmentType = 'FULL_TIME',
      isRemote = 'no',
      currency = '',
      salaryMin = '',
      salaryMax = '',
      salaryUnit = 'YEAR'
    } = req.body;

    if (!title || !company || !url) {
      return res.status(400).send('Champs obligatoires manquants');
    }

    const guid = `manual-${Date.now()}-${crypto.randomBytes(8).toString('hex')}`;
    const published_at = Math.floor(Date.now() / 1000);

    const userTags = tags.split(',').map(t => t.trim().toLowerCase()).filter(Boolean);

    let finalHtml, finalShort, finalTags;

    if (!String(description || '').trim()) {
      console.log('Génération IA pour publication manuelle :', title);
      const result = await rewriteJobRich({ title, company, html: `<p>Poste chez ${company}</p>` }, true);
      finalHtml = result.html;
      finalShort = result.short;
      finalTags = [...new Set([...result.tags, ...userTags])];
    } else {
      finalHtml = sanitizeHtml(stripDocumentTags(description));
      finalShort = truncateWords(convert(description, { wordwrap: 120 }), 45);
      finalTags = [...new Set([...extractTags({ title, company, html: description }), ...userTags])];
    }

    let salaryInfo = '';
    if (currency && (salaryMin || salaryMax)) {
      const unitLabel = UNIT_LABELS[String(salaryUnit).toUpperCase()] || 'période';
      salaryInfo = `\n<p><strong>Salaire :</strong> ${currency} ${salaryMin ? salaryMin : ''}${salaryMin && salaryMax ? '-' : ''}${salaryMax ? salaryMax : ''} par ${unitLabel}</p>`;
    }

    const enrichedHtml = finalHtml + salaryInfo;
    const slug = mkSlug(`${title}-${company}-${Date.now()}`) || mkSlug(guid);

    stmtInsertJob.run({
      guid,
      source: 'manuel',
      title,
      company,
      description_html: enrichedHtml,
      description_short: finalShort,
      url,
      published_at,
      slug,
      tags_csv: uniqNormTags(finalTags).join(', ')
    });

    const inserted = stmtHasGuid.get(guid);
    if (inserted) {
      upsertTagsForJob(inserted.id, finalTags);
    }
    stmtSetCache.run('total_jobs', getCachedCount(0));

    console.log(`Offre publiée : ${title} chez ${company}`);
    res.redirect(`/job/${slug}`);
  } catch (error) {
    console.error('Erreur lors de la publication :', error);
    res.status(500).send('Erreur serveur. Veuillez réessayer.');
  }
});

// RÈGLES (FAQ + schéma)
app.get('/rules', (req, res) => {
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Règles', url: '/rules' }
  ];
  const faqData = {
    "@context": "https://schema.org",
    "@type": "FAQPage",
    "mainEntity": [
      { "@type": "Question", "name": "Comment publier une offre ?", "acceptedAnswer": { "@type": "Answer", "text": `Cliquez sur “Publier une offre” dans l’en-tête et remplissez le formulaire. Toutes les offres ${TARGET_PROFESSION} sont les bienvenues.` } },
      { "@type": "Question", "name": "La publication est-elle gratuite ?", "acceptedAnswer": { "@type": "Answer", "text": "Oui, la publication d’offres est entièrement gratuite sur notre plateforme." } },
      { "@type": "Question", "name": "Combien de temps une offre reste-t-elle en ligne ?", "acceptedAnswer": { "@type": "Answer", "text": "Les offres restent actives 30 jours et sont incluses dans le sitemap et le flux RSS." } }
    ]
  };
  const faqSchema = `<script type="application/ld+json">${JSON.stringify(faqData)}</script>`;
  const orgSchema = `<script type="application/ld+json">${JSON.stringify({
    "@context": "https://schema.org",
    "@type": "Organization",
    "name": SITE_NAME,
    "url": SITE_URL,
    ...(SITE_LOGO ? { "logo": SITE_LOGO } : {}),
    ...(SITE_SAMEAS ? { "sameAs": SITE_SAMEAS.split(',').map(s => s.trim()).filter(Boolean) } : {})
  })}</script>`;

  res.send(layout({
    title: 'Règles & FAQ',
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Règles</nav>
<article class="card">
  <h1>Règles & FAQ</h1>
  <h2>Directives de publication</h2>
  <ul>
    <li>Uniquement des postes de ${escapeHtml(TARGET_PROFESSION)}</li>
    <li>Offres légitimes uniquement</li>
    <li>Informations entreprise et lien de candidature exacts</li>
    <li>Aucune discrimination</li>
  </ul>
  <h2>FAQ</h2>
  <h3>Comment publier une offre ?</h3>
  <p>Cliquez sur “Publier une offre” et remplissez le formulaire.</p>
  <h3>La publication est-elle gratuite ?</h3>
  <p>Oui, c’est gratuit.</p>
  <h3>Combien de temps une offre reste-t-elle en ligne ?</h3>
  <p>30 jours, incluse dans le sitemap et le RSS.</p>
  <h3>Modifier ou supprimer une offre ?</h3>
  <p>Contactez-nous si nécessaire.</p>
  <h3>Comment les offres sont-elles traitées ?</h3>
  <p>Nous utilisons l’IA pour structurer et améliorer la lisibilité. Si vous fournissez votre description, nous l’utilisons telle quelle.</p>
</article>
`,
    breadcrumbs,
    metaExtra: faqSchema + orgSchema
  }));
});

// PAGE TAG (pagination)
app.get('/tag/:slug', (req, res) => {
  const slug = req.params.slug;
  const tag = stmtGetTagBySlug.get(slug);
  if (!tag) return res.status(404).send('Introuvable');

  const pageSize = 50;
  const cursor = req.query.cursor || '';
  let rows;
  if (!cursor) {
    rows = stmtJobsByTagFirst.all(slug, pageSize);
  } else {
    const [pub, id] = cursor.split('-').map(Number);
    if (!pub || !id) return res.status(400).send('Curseur invalide');
    rows = stmtJobsByTagCursor.all(slug, pub, pub, id, pageSize);
  }
  const cnt = stmtCountJobsByTagId.get(tag.id).c;
  const hasMore = rows.length === pageSize;
  const nextCursor = hasMore ? `${rows[rows.length - 1].published_at}-${rows[rows.length - 1].id}` : null;

  const items = rows.map(r => `
<li class="card">
  <h2><a href="/job/${r.slug}">${escapeHtml(r.title)}</a></h2>
  ${r.company ? `<div class="muted">${escapeHtml(r.company)}</div>` : ''}
  <p>${escapeHtml(r.description_short)}</p>
  <div class="muted small">${new Date(r.published_at * 1000).toLocaleDateString('fr-FR')}</div>
</li>`).join('');

  const pagerLinks = [];
  if (nextCursor) {
    res.setHeader('Link', `<${canonical(`/tag/${slug}?cursor=${nextCursor}`)}>; rel="next"`);
    pagerLinks.push(`<a href="/tag/${slug}?cursor=${nextCursor}" rel="next">Suivant →</a>`);
  }
  if (cursor) {
    pagerLinks.unshift(`<a href="/tag/${slug}" rel="prev">← Début</a>`);
  }
  const pager = pagerLinks.length ? `<div class="pager">${pagerLinks.join('')}</div>` : '';

  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Tags', url: '/tags' },
    { name: tag.name, url: `/tag/${slug}` }
  ];

  res.send(layout({
    title: `Tag : ${tag.name}`,
    body: `
<nav class="muted small"><a href="/">Accueil</a> › <a href="/tags">Tags</a> › ${escapeHtml(tag.name)}</nav>
<h1>Tag : ${escapeHtml(tag.name)}</h1>
<p class="muted">${cnt} offres</p>
<ul class="list">${items || '<li class="card">Aucune offre.</li>'}</ul>
${pager}
`,
    breadcrumbs
  }));
});

// LISTE DE TOUS LES TAGS
app.get('/tags', (req, res) => {
  const popular = stmtPopularTags.all(1, 500);
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Tags', url: '/tags' }
  ];

  const body = popular.length ? `
<nav class="muted small"><a href="/">Accueil</a> › Tags</nav>
<h1>Tous les tags</h1>
<div class="tags">
  ${popular.map(t => `<a class="tag" href="/tag/${t.slug}">${escapeHtml(t.name)} (${t.cnt})</a>`).join('')}
</div>
` : `
<nav class="muted small"><a href="/">Accueil</a> › Tags</nav>
<h1>Tags</h1>
<p class="muted">Aucun tag pour le moment.</p>
`;

  res.send(layout({ title: 'Tags', body, breadcrumbs }));
});

// PAGE OFFRE (avec JSON-LD corrigé)
app.get('/job/:slug', (req, res) => {
  const job = stmtBySlug.get(req.params.slug);
  if (!job) return res.status(404).send('Introuvable');

  const token = crypto.createHmac('sha256', CLICK_SECRET).update(String(job.id)).digest('hex').slice(0, 16);
  const tags = (job.tags_csv || '').split(',').map(s => s.trim()).filter(Boolean);
  const tagsHtml = tags.length ? `<div class="tags">
    ${tags.map(name => `<a class="tag" href="/tag/${tagSlug(name)}">${escapeHtml(name)}</a>`).join('')}
  </div>` : '';

  const meta = parseMeta(job.description_html || '', job.title || '');
  const datePostedISO = new Date(job.published_at * 1000).toISOString();
  const validThrough = new Date(Date.now() + 30 * 24 * 3600 * 1000).toISOString();

  const jobLocations = inferJobLocations(job.description_html || '', job.title || '', SITE_URL);

  const identifier = {
    "@type": "PropertyValue",
    "name": SITE_NAME,
    "value": String(job.guid || job.id)
  };
  const directApply = false; // redirection vers la source

  const jobPostingJson = {
    "@context": "https://schema.org",
    "@type": "JobPosting",
    "title": job.title,
    "description": job.description_html,
    "datePosted": datePostedISO,
    "validThrough": validThrough,
    "employmentType": meta.employmentType,
    "hiringOrganization": {
      "@type": "Organization",
      "name": job.company || "Inconnu"
    },
    "jobLocation": jobLocations,                     // REQUIS
    ...(meta.isRemote ? { "jobLocationType": "TELECOMMUTE" } : {}),
    "identifier": identifier,                        // recommandé
    "directApply": directApply,                      // recommandé
    ...(meta.experienceRequirements ? { "experienceRequirements": meta.experienceRequirements } : {}),
    "experienceInPlaceOfEducation": Boolean(meta.experienceInPlaceOfEducation),
    ...(meta.salary ? {
      "baseSalary": {
        "@type": "MonetaryAmount",
        "currency": meta.salary.currency,
        "value": {
          "@type": "QuantitativeValue",
          ...(meta.salary.min ? { "minValue": meta.salary.min } : {}),
          ...(meta.salary.max ? { "maxValue": meta.salary.max } : {}),
          ...(meta.salary.unit ? { "unitText": meta.salary.unit } : {})
        }
      }
    } : {})
  };

  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: job.title, url: `/job/${job.slug}` }
  ];

  const metaExtra = `
<script type="application/ld+json">${JSON.stringify(jobPostingJson)}</script>
<meta name="robots" content="index, follow"/>
`;

  const body = `
<nav class="muted small"><a href="/">Accueil</a> › ${escapeHtml(job.title)}</nav>
<article class="card">
  <h1>${escapeHtml(job.title)}</h1>
  ${job.company ? `<div class="muted">${escapeHtml(job.company)}</div>` : ''}
  <div class="muted small">${new Date(job.published_at * 1000).toLocaleDateString('fr-FR')}</div>
  ${tagsHtml}
  <div class="content">${job.description_html || ''}</div>
  <form method="POST" action="/go" style="margin-top:24px">
    <input type="hidden" name="id" value="${job.id}"/>
    <input type="hidden" name="t" value="${token}"/>
    <button class="btn btn-primary" type="submit">Postuler / Voir la source</button>
  </form>
</article>
`;

  res.send(layout({ title: job.title, body, metaExtra, breadcrumbs }));
});

// /go - redirection sécurisée
app.post('/go', (req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  const id = Number(req.body?.id || 0);
  const t = String(req.body?.t || '');
  if (!id || !t) return res.status(400).send('Requête invalide');
  const expect = crypto.createHmac('sha256', CLICK_SECRET).update(String(id)).digest('hex').slice(0, 16);
  if (t !== expect) return res.status(403).send('Interdit');
  const job = stmtById.get(id);
  if (!job || !job.url) return res.status(404).send('Introuvable');
  return res.redirect(302, job.url);
});
app.get('/go', (_req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  return res.status(405).send('Méthode non autorisée');
});

// robots.txt
app.get('/robots.txt', (_req, res) => {
  res.type('text/plain').send(`User-agent: *
Disallow: /go
Disallow: /post-job
Disallow: /fetch
Sitemap: ${SITE_URL}/sitemap.xml
`);
});

// sitemap.xml
app.get('/sitemap.xml', (req, res) => {
  const recent = stmtRecent.all(10000);
  const urls = recent.map(r => `
  <url>
    <loc>${canonical(`/job/${r.slug}`)}</loc>
    <lastmod>${new Date(r.published_at * 1000).toISOString()}</lastmod>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>`).join('');
  res.set('Content-Type', 'application/xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>${SITE_URL}/</loc>
    <changefreq>hourly</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>${SITE_URL}/tags</loc>
    <changefreq>daily</changefreq>
    <priority>0.7</priority>
  </url>
  ${urls}
</urlset>`);
});

// RSS
app.get('/feed.xml', (req, res) => {
  const recent = stmtRecent.all(100);
  const items = recent.map(r => `
  <item>
    <title><![CDATA[${r.title}]]></title>
    <link>${canonical(`/job/${r.slug}`)}</link>
    <guid>${canonical(`/job/${r.slug}`)}</guid>
    <pubDate>${new Date(r.published_at * 1000).toUTCString()}</pubDate>
  </item>`).join('');
  res.set('Content-Type', 'application/rss+xml').send(`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>${escapeHtml(SITE_NAME)}</title>
    <link>${SITE_URL}</link>
    <description>Dernières offres ${escapeHtml(TARGET_PROFESSION.toLowerCase())}</description>
    <language>${TARGET_LANG}</language>
    <atom:link href="${canonical('/feed.xml')}" rel="self" type="application/rss+xml"/>
    ${items}
  </channel>
</rss>`);
});

// ========================================
// PAGES LÉGALES
// ========================================
const LAST_UPDATED = new Date().toISOString().slice(0,10); // AAAA-MM-JJ

app.get('/privacy', (req, res) => {
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Confidentialité', url: '/privacy' }
  ];
  res.send(layout({
    title: 'Politique de confidentialité',
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Confidentialité</nav>
<article class="card content">
  <h1>Politique de confidentialité</h1>
  <p class="small muted">Dernière mise à jour : ${LAST_UPDATED}</p>
  <p>Nous respectons votre vie privée. Ce site stocke le minimum de données nécessaires à son fonctionnement.</p>
  <h2>Données traitées</h2>
  <ul>
    <li>Logs serveur (IP, user agent) pour sécurité et fiabilité</li>
    <li>Contenu des offres que vous soumettez</li>
    <li>Cookies essentiels pour mémoriser votre consentement</li>
  </ul>
  <h2>Finalités & Base légale</h2>
  <p>Exploiter le site, prévenir l’abus, fournir les offres. Intérêt légitime et consentement le cas échéant.</p>
  <h2>Conservation</h2>
  <p>Logs pour une durée limitée, offres tant qu’elles sont pertinentes, cookies de consentement jusqu’à un an.</p>
  <h2>Vos droits</h2>
  <p>Demander l’accès ou la suppression de vos données présentes dans les offres soumises.</p>
  <h2>Contact</h2>
  <p>Pour toute demande, contactez-nous via les informations du site.</p>
</article>
`,
    breadcrumbs
  }));
});

app.get('/terms', (req, res) => {
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Conditions', url: '/terms' }
  ];
  res.send(layout({
    title: 'Conditions d’utilisation',
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Conditions</nav>
<article class="card content">
  <h1>Conditions d’utilisation</h1>
  <p class="small muted">Dernière mise à jour : ${LAST_UPDATED}</p>
  <h2>Acceptation</h2>
  <p>En utilisant ${escapeHtml(SITE_NAME)}, vous acceptez ces Conditions. Si vous n’êtes pas d’accord, n’utilisez pas le site.</p>
  <h2>Utilisation du service</h2>
  <ul>
    <li>Publiez uniquement des offres ${escapeHtml(TARGET_PROFESSION)} légitimes</li>
    <li>Pas de contenu illégal ou discriminatoire</li>
    <li>Pas d’usage abusif ou de perturbation du service</li>
  </ul>
  <h2>Contenu</h2>
  <p>Vous êtes responsable des contenus soumis. Nous pouvons retirer les contenus non conformes.</p>
  <h2>Avertissement</h2>
  <p>Le site est fourni “tel quel”, sans garantie.</p>
  <h2>Responsabilité</h2>
  <p>Dans la limite autorisée par la loi, nous déclinons toute responsabilité pour dommages indirects.</p>
  <h2>Modifications</h2>
  <p>Nous pouvons mettre à jour ces Conditions en publiant une nouvelle version ici.</p>
</article>
`,
    breadcrumbs
  }));
});

app.get('/cookies', (req, res) => {
  const breadcrumbs = [
    { name: 'Accueil', url: '/' },
    { name: 'Cookies', url: '/cookies' }
  ];
  res.send(layout({
    title: 'Politique Cookies',
    body: `
<nav class="muted small"><a href="/">Accueil</a> › Cookies</nav>
<article class="card content">
  <h1>Politique Cookies</h1>
  <p class="small muted">Dernière mise à jour : ${LAST_UPDATED}</p>
  <h2>Qu’est-ce qu’un cookie ?</h2>
  <p>Un petit fichier texte stocké sur votre appareil pour aider les sites à fonctionner.</p>
  <h2>Cookies utilisés</h2>
  <ul>
    <li><strong>cookie_consent</strong> — mémorise votre choix (expire sous 12 mois).</li>
  </ul>
  <h2>Gestion</h2>
  <p>Supprimez les cookies dans les réglages du navigateur. Pour changer votre consentement ici, effacez les cookies ou utilisez le bouton ci-dessous.</p>
  <button class="btn" onclick="document.cookie='cookie_consent=; Max-Age=0; Path=/; SameSite=Lax'; alert('Consentement effacé. Rechargez la page pour voir le bandeau.');">Effacer le consentement</button>
</article>
`,
    breadcrumbs
  }));
});

// Déclenchement manuel du flux
app.get('/fetch', async (_req, res) => {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow');
  res.write('Traitement du flux…\n\n');
  try {
    await processFeed();
    res.end('Terminé ! Voir la console pour le détail.\n');
  } catch (e) {
    res.end(`Erreur : ${e.message}\n`);
  }
});

// ========================================
// DÉMARRAGE
// ========================================
if (FEED_URL) {
  processFeed().catch(console.error);
}
if (FEED_URL && CRON_SCHEDULE) {
  cron.schedule(CRON_SCHEDULE, () => {
    console.log(`\nCRON : démarrage du traitement planifié du flux…`);
    processFeed().catch(console.error);
  });
}

app.listen(PORT, () => {
  console.log('\n' + '='.repeat(60));
  console.log(`${SITE_NAME}`);
  console.log('='.repeat(60));
  console.log(`Serveur :     ${SITE_URL}`);
  console.log(`Profession :  ${TARGET_PROFESSION}`);
  console.log(`Mots-clés :   ${PROFESSION_KEYWORDS.join(', ')}`);
  console.log(`IA activée :  ${HAS_OPENAI ? 'Oui' : 'Non'}`);
  console.log(`Limite IA :   ${AI_PROCESS_LIMIT === 0 ? 'Illimitée' : `${AI_PROCESS_LIMIT} offres/flux`}`);
  console.log(`Flux :        ${FEED_URL || 'Non configuré'}`);
  console.log(`Cron :        ${CRON_SCHEDULE}`);
  console.log(`Favicon :     ${FAVICON_URL || 'Aucun'}`);
  console.log(`Offres tot. : ${getCachedCount().toLocaleString('fr-FR')}`);
  console.log('='.repeat(60) + '\n');
});
