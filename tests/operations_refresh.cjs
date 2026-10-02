const assert = require('node:assert/strict');
const vm = require('node:vm');
const calls = [], timers = new Map(), renders = [], messages = [];
let now = 0, timerId = 0, resolveFetch;
const button = {};
const context = vm.createContext({
  project: 'roy', maintenanceLocked: false, latestData: null,
  Date: {now: () => now},
  setTimeout(fn, delay) { const id = ++timerId; timers.set(id, {fn, delay}); return id; },
  clearTimeout(id) { timers.delete(id); },
  el: () => button,
  fetchApi(path) { calls.push(path); return new Promise(resolve => { resolveFetch = resolve; }); },
  readJsonApi: async response => response.data,
  clearMessage() {}, showMessage: message => messages.push(message),
  render(data) { context.latestData = data; renders.push(data); },
});
vm.runInContext(__REFRESH_CODE__, context);
const run = code => vm.runInContext(code, context);
const reply = data => resolveFetch({ok: true, data});
const delay = () => { assert.equal(timers.size, 1); return [...timers.values()][0].delay; };
const flush = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };

(async () => {
  const first = run('loadDashboard(false, true)');
  const duplicate = run('loadDashboard(false)');
  assert.equal(calls.length, 1);
  assert.match(calls[0], /revalidate=1$/);
  reply({generated_at: 'old', auto_refresh_seconds: 30, cache: {refresh_in_progress: true}});
  await Promise.all([first, duplicate]);
  assert.equal(delay(), 5000);
  const next = [...timers.values()][0].fn();
  reply({generated_at: 'new-orders', auto_refresh_seconds: 30, cache: {}, inventory_refresh: {in_progress: true}});
  await next;
  assert.equal(renders.at(-1).generated_at, 'new-orders');
  assert.equal(delay(), 5000);
  now = 120001;
  run('scheduleDashboardRefresh(latestData)');
  assert.equal(delay(), 30000); // bounded quick polling while a backend is stuck
  const finished = run('loadDashboard(false)');
  reply({generated_at: 'new-orders', auto_refresh_seconds: 30, cache: {}});
  await finished;
  assert.equal(run('quickRefreshStartedAt'), null);
  assert.equal(delay(), 30000);

  const inFlight = run('loadDashboard(false)');
  const count = calls.length;
  run('loadDashboard(true)');
  assert.equal(calls.length, count);
  reply({generated_at: 'before-action', auto_refresh_seconds: 30, cache: {}});
  await flush();
  assert.equal(calls.length, count + 1);
  assert.match(calls.at(-1), /refresh=1$/);
  reply({generated_at: 'after-action', auto_refresh_seconds: 30, cache: {}});
  await inFlight;
  assert.equal(renders.at(-1).generated_at, 'after-action');
  const failed = run('loadDashboard(false)');
  resolveFetch({ok: false, status: 503, data: {error: 'temporary'}});
  await failed;
  assert.equal(renders.at(-1).generated_at, 'after-action');
  assert.equal(messages.length, 1);
  assert.equal(delay(), 30000);
  context.maintenanceLocked = true;
  run('scheduleDashboardRefresh(latestData)');
  assert.equal(timers.size, 0);
  console.log('OPERATIONS_REFRESH_BROWSER_OK');
})().catch(error => { console.error(error); process.exitCode = 1; });
