const assert = require('node:assert/strict');
const vm = require('node:vm');

function tab(storage = new Map()) {
  const ids = new Map(), calls = [], downloads = [];
  const context = vm.createContext({
    BOOTSTRAP: {project: 'roy'}, URL, Intl, Blob, setTimeout: (fn) => fn(),
    window: {location: {origin: 'https://dashboard.example'}, confirm: () => true},
    sessionStorage: {getItem: k => storage.get(k), setItem: (k, v) => storage.set(k, v), removeItem: k => storage.delete(k)},
    document: {
      getElementById(id) {
        if (!ids.has(id)) ids.set(id, {classList: {toggle() {}}, setAttribute() {}});
        return ids.get(id);
      },
      body: {appendChild() {}},
      createElement: () => ({click() { downloads.push(this.download); }, remove() {}}),
    },
  });
  vm.runInContext(__DASHBOARD_CODE__, context);
  const run = code => vm.runInContext(code, context);
  run('showMessage = () => {}; loadDashboard = async () => {};');
  context.fetch = async (path, options) => {
    calls.push({path, body: JSON.parse(options.body)});
    return context.reply;
  };
  function pdf(batchId, count) {
    context.reply = {ok: true, headers: new Map([['X-Picking-Batch-Id', batchId], ['X-Picking-Order-Count', String(count)], ['X-Picking-Filename', `${batchId}.pdf`]]),
      blob: async () => new Blob(['%PDF-test'])};
  }
  const refresh = orders => run(`latestData = {orders: {orders: ${JSON.stringify(orders.map(order_num => ({order_num})))}}}; updatePickingControls();`);
  return {run, pdf, refresh, context, storage, calls, ids, downloads};
}

(async () => {
  const a = tab(), b = tab();
  a.refresh(['A', 'B']);
  assert.equal(a.ids.get('markPickingPrintedBtn').disabled, true);
  a.pdf('a'.repeat(32), 2);
  await a.run("downloadPickingPdf(['A', 'B'])");
  assert.equal(a.downloads.length, 1);
  a.refresh(['A', 'B', 'NEW']);
  assert.match(a.ids.get('markPickingPrintedBtn').textContent, /\(2\)/);
  b.pdf('b'.repeat(32), 1);
  await b.run("downloadPickingPdf(['NEW'])");
  assert.equal(a.run('downloadedPickingBatch.batchId'), 'a'.repeat(32));
  const reloaded = tab(a.storage);
  assert.equal(reloaded.run('downloadedPickingBatch.batchId'), 'a'.repeat(32));
  a.context.reply = {ok: false, json: async () => ({error: 'failed'})};
  await a.run("downloadPickingPdf(['NEW'])");
  assert.equal(a.run('downloadedPickingBatch.batchId'), 'a'.repeat(32));
  a.pdf('c'.repeat(32), 3);
  a.context.reply.blob = async () => { throw new Error('connection lost'); };
  await a.run("downloadPickingPdf(['A', 'B', 'NEW'])");
  assert.equal(a.run('downloadedPickingBatch.batchId'), 'a'.repeat(32));
  a.pdf('d'.repeat(32), 1);
  await a.run("downloadPickingPdf(['NEW'], true)");
  a.refresh(['SOMETHING-ELSE']);
  a.context.reply = {ok: true, json: async () => ({batch: {order_count: 1}})};
  await a.run('markPickingPrinted()');
  assert.deepEqual(a.calls.at(-1).body, {batch_id: 'd'.repeat(32)});
  assert.equal(a.run('downloadedPickingBatch'), null);
  assert.equal(a.ids.get('markPickingPrintedBtn').disabled, true);
  assert.equal(b.run('downloadedPickingBatch.batchId'), 'b'.repeat(32));
  console.log('PICKING_BATCH_BROWSER_OK');
})().catch(error => { console.error(error); process.exitCode = 1; });
