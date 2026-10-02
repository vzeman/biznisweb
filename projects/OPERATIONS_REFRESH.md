# Independent operations refresh

The order list must not wait for inventory search and calculation. Ordinary reads
publish a complete, unchanged eligibility scan with current print-state annotation;
a separate single-flight worker refreshes inventory. The shared dashboard response
composes the latest compatible components. Inventory has a five-minute freshness
window and a separate timestamp. Its completion cannot overwrite newer orders or
make their cache look newer. A cold cache can show orders while inventory is loading.

ROY uses a 30-second order cache TTL and browser cadence. Manual Refresh starts
asynchronous order revalidation within that TTL. While either component is being
refreshed, the browser reads the result every five seconds for at most two minutes,
then returns to normal polling. Only one dashboard request can be active per tab;
a requested state-changing readback during it is queued. Errors preserve the last
rendered data and background failures have a 30-second retry backoff. The page shows
order/inventory timestamps and refresh/failure indicators. These intervals are not
a guarantee of source-order arrival latency: the upstream scan still takes time.

Inventory uses the existing conditional state writes. Its result is discarded
after a local invalidation or remote state-fingerprint change. Order publication
also checks its generation token and current remote fingerprint. Memory/shared
publication is serialized against local invalidation. Existing state-changing
actions retain synchronous complete readback. Payment/shipping eligibility,
seven-day immutable PDF batches and COD invoice rules are unchanged.

The regressions in `tests/test_operations_refresh.py` and
`tests/operations_refresh.cjs` use synthetic data, blocked workers and a virtual
browser clock. They verify independent progress, bounded polling, coalesced reads,
failure retention/backoff and rejection of stale results. All temporary threads
are joined. No persistent dev server is required for these tests.

Release through `scripts/deploy_picking_batch_dashboard.py` from a clean exact
merged checkout. Its finite host gate now requires `independent-orders-v1` in the
HTML and a newly published API result, alongside the existing PDF proof, localhost
marker and nine isolated backend refresh tests. The private receipt records task,
IP, path, digest and measured order-scan duration. Promote only the intended shop;
all non-image settings and reporting schedules must remain identical. Then verify
actual UI refresh and read back the remaining scheduled runtimes independently.

Current rollout scope: ROY only. VEVO shares the tested implementation but retains
its existing production image/configuration until separately released.
