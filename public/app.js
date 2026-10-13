'use strict';
const $ = (id) => document.getElementById(id);
const STATES = ['queued', 'running', 'succeeded', 'dead', 'cancelled'];
let timer = null;

const getKey = () => { try { return sessionStorage.getItem('qf_key') || ''; } catch { return ''; } };
const setStatus = (msg, isErr = false) => { const s = $('status'); s.textContent = msg; s.className = isErr ? 'err' : ''; };

async function api(path, opts = {}) {
  const headers = { Accept: 'application/json' };
  const key = getKey();
  if (key) headers.Authorization = `Bearer ${key}`;
  if (opts.body) headers['Content-Type'] = 'application/json';
  const res = await fetch(`/api/v1${path}`, { ...opts, headers });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data?.error?.message || `HTTP ${res.status}`);
  return data;
}

// All dynamic text goes through textContent / createElement: never innerHTML.
function el(tag, props = {}, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) { if (k === 'className') e.className = v; else if (k === 'onclick') e.addEventListener('click', v); else e.setAttribute(k, v); }
  for (const k of kids) e.append(k);
  return e;
}
const fmt = (t) => (t ? new Date(t).toLocaleString() : '—');

function renderCards(totals) {
  const root = $('cards');
  root.replaceChildren(...STATES.map((s) => el('div', { className: 'card' }, el('b', {}, String(totals[s] ?? 0)), el('small', {}, s))));
}

function renderRows(jobs) {
  const rows = $('rows');
  rows.replaceChildren(...jobs.map((j) => el('tr', { className: 'row', tabindex: '0' },
    el('td', {}, el('code', {}, j.id.slice(0, 8))),
    el('td', {}, j.queue), el('td', {}, j.type),
    el('td', {}, el('span', { className: `badge ${j.state}` }, j.state)),
    el('td', {}, `${j.attempts}/${j.maxAttempts}`),
    el('td', {}, fmt(j.createdAt)),
    el('td', {}, el('button', { onclick: (e) => { e.stopPropagation(); openDetail(j.id); } }, 'Details')),
  )));
  for (const [i, tr] of [...rows.children].entries()) {
    tr.addEventListener('click', () => openDetail(jobs[i].id));
    tr.addEventListener('keydown', (e) => { if (e.key === 'Enter') openDetail(jobs[i].id); });
  }
  $('empty').hidden = jobs.length > 0;
}

async function openDetail(id) {
  try {
    const [{ job }, { events }] = await Promise.all([api(`/jobs/${id}`), api(`/jobs/${id}/events`)]);
    $('dTitle').textContent = `${job.type} · ${job.id}`;
    $('dBody').textContent = JSON.stringify(job, null, 2);
    $('dEvents').replaceChildren(...events.map((e) => el('li', {}, `${fmt(e.at)} — ${e.event}${e.detail ? `: ${e.detail}` : ''}`)));
    const old = document.querySelector('#detail .actions'); old?.remove();
    const actions = el('div', { className: 'actions' });
    if (job.state === 'queued') actions.append(el('button', { onclick: () => act(job.id, 'cancel') }, 'Cancel'));
    if (job.state === 'dead' || job.state === 'cancelled') actions.append(el('button', { onclick: () => act(job.id, 'retry') }, 'Retry'));
    $('dTitle').after(actions);
    $('detail').hidden = false;
  } catch (err) { setStatus(err.message, true); }
}

async function act(id, action) {
  try { await api(`/jobs/${id}/${action}`, { method: 'POST' }); await load(); await openDetail(id); }
  catch (err) { setStatus(err.message, true); }
}

async function load() {
  const p = new URLSearchParams();
  if ($('fState').value) p.set('state', $('fState').value);
  if ($('fQueue').value.trim()) p.set('queue', $('fQueue').value.trim());
  if ($('fCorr').value.trim()) p.set('correlationId', $('fCorr').value.trim());
  try {
    const [stats, list] = await Promise.all([api('/stats'), api(`/jobs?${p}`)]);
    renderCards(stats.totals);
    renderRows(list.jobs);
    setStatus(`Updated ${new Date().toLocaleTimeString()} · ${list.total} matching`);
  } catch (err) { setStatus(err.message, true); }
}

function schedule() { clearInterval(timer); timer = setInterval(() => { if (!document.hidden) load(); }, 5000); }

$('saveKey').addEventListener('click', () => { try { sessionStorage.setItem('qf_key', $('apiKey').value.trim()); } catch { /* ignore */ } $('apiKey').value = ''; load(); });
$('refresh').addEventListener('click', load);
$('closeDetail').addEventListener('click', () => { $('detail').hidden = true; });
$('fState').addEventListener('change', load);
document.addEventListener('keydown', (e) => { if (e.key === 'Escape') $('detail').hidden = true; });
load(); schedule();
