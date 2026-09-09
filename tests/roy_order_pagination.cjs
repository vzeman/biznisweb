const assert = require('node:assert/strict');
const vm = require('node:vm');

function element() {
  return {
    textContent: '', innerHTML: '', value: '', disabled: false,
    dataset: {}, events: {}, classList: {toggle() {}},
    setAttribute() {}, scrollIntoView() {},
    addEventListener(type, handler) { this.events[type] = handler; },
  };
}
const ids = new Map();
const navs = Array.from({length: 4}, () => {
  const nodes = new Map();
  for (const selector of ['[data-orders-range]', '[data-orders-page-label]', '[data-orders-page="previous"]', '[data-orders-page="next"]', '[data-orders-page-size]']) {
    nodes.set(selector, element());
  }
  const previous = nodes.get('[data-orders-page="previous"]');
  const next = nodes.get('[data-orders-page="next"]');
  previous.dataset.ordersPage = 'previous';
  next.dataset.ordersPage = 'next';
  return {
    ...element(),
    querySelector(selector) { return nodes.get(selector); },
    querySelectorAll() { return [previous, next]; },
    closest() { return {querySelector: () => navs[0]}; },
  };
});
const context = vm.createContext({
  BOOTSTRAP: {project: 'roy'}, URL, Intl,
  window: {location: {origin: 'https://dashboard.example'}},
  document: {
    getElementById(id) {
      if (!ids.has(id)) ids.set(id, element());
      return ids.get(id);
    },
    querySelectorAll() { return navs; },
  },
});
vm.runInContext(__DASHBOARD_CODE__, context);
const run = (code) => vm.runInContext(code, context);
const control = (selector, index = 0) => navs[index].querySelector(selector);
const next = (index = 0) => control('[data-orders-page="next"]', index).events.click();
const previous = () => control('[data-orders-page="previous"]').events.click();
const setSize = (size) => control('[data-orders-page-size]').events.change({target: {value: String(size)}});
const rowIds = (id = 'ordersBody') => [...ids.get(id).innerHTML.matchAll(/<strong class="mono">(\d+)<\/strong>/g)].map(match => Number(match[1]));
const refresh = (count) => run(`latestData = {orders: {orders: Array.from({length:${count}}, (_, i) => ({order_num: String(i + 1), items: []})), summary: {picking_unprinted_orders:${count}}}}; renderOrders(latestData);`);

run('initializeOrdersPagination()');
refresh(57);
assert.equal(control('[data-orders-page="previous"]').disabled, true);
const visited = [];
for (let page = 1; page <= 6; page++) {
  assert.deepEqual(rowIds(), rowIds('ordersFullBody'));
  visited.push(...rowIds());
  if (page < 6) next(1); // Bottom controls must work too.
}
assert.deepEqual(visited, Array.from({length:57}, (_, i) => i + 1));
assert.equal(control('[data-orders-page="next"]').disabled, true);
assert.equal(control('[data-orders-range]').textContent, '51–57 z 57 objednávok');
for (const nav of navs) assert.equal(nav.querySelector('[data-orders-page-label]').textContent, 'Strana 6 z 6');
assert.equal(new URL(ids.get('pickingPdfLink').href, 'https://dashboard.example').searchParams.getAll('order_num').length, 57);
assert.match(ids.get('ordersBody').innerHTML, /data-print-order="57"/);
assert.match(ids.get('ordersMeta').textContent, /^57 objednávok/);

refresh(58); // Auto-refresh keeps the current page.
assert.deepEqual(rowIds(), [51,52,53,54,55,56,57,58]);
previous();
assert.deepEqual(rowIds(), [41,42,43,44,45,46,47,48,49,50]);
refresh(12); // Removed orders clamp to the last available page.
assert.deepEqual(rowIds(), [11,12]);
refresh(0);
assert.deepEqual(rowIds(), []);
assert.equal(control('[data-orders-range]').textContent, '0 objednávok');
assert.equal(control('[data-orders-page="previous"]').disabled, true);
assert.equal(control('[data-orders-page="next"]').disabled, true);
assert.equal(ids.get('markPickingPrintedBtn').disabled, true);

refresh(60);
setSize(25);
assert.equal(rowIds().length, 25);
next(2); // The orders tab shares the same page.
assert.deepEqual(rowIds(), Array.from({length:25}, (_, i) => i + 26));
next(3);
assert.deepEqual(rowIds(), [51,52,53,54,55,56,57,58,59,60]);
setSize(50);
assert.equal(rowIds()[0], 1);
assert.equal(rowIds().length, 50);
setSize(0); // Invalid page sizes cannot break navigation.
assert.equal(rowIds().length, 50);
refresh(50); // Exact page boundary has no extra empty page.
assert.equal(control('[data-orders-page="next"]').disabled, true);
refresh(1);
assert.deepEqual(rowIds(), [1]);
console.log('ROY_ORDER_PAGINATION_OK:57-orders:all-pages:refresh:shrink:empty:page-sizes:picking');
