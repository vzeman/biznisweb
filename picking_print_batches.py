"""Bind print acknowledgements to the exact orders rendered in a downloaded PDF."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import uuid

from roy_operations_dashboard import mark_picking_orders_printed

BATCH_TTL = timedelta(days=7)
MAX_BATCHES = 200


def _now():
    return datetime.now(timezone.utc)


def _timestamp(value):
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=timezone.utc)


def register_picking_pdf_batch(state, project, orders, pdf, filename, *, now=None):
    """Called only after successful rendering; saving must succeed before delivery."""
    now = now or _now()
    if project not in {"roy", "vevo"} or not orders or not pdf.startswith(b"%PDF-"):
        raise ValueError("PDF dávku sa nepodarilo pripraviť. Stiahnite PDF znova.")
    keys = [str(order.get("order_num") or "").strip() for order in orders]
    if not all(keys) or len(keys) != len(set(keys)):
        raise ValueError("PDF obsahuje nejednoznačné objednávky.")
    batches = state.setdefault("picking_pdf_batches", {})
    retained = sorted(
        ((key, value) for key, value in batches.items()
         if isinstance(value, dict) and _timestamp(value.get("created_at")) > now - BATCH_TTL),
        key=lambda item: item[1]["created_at"],
    )[-(MAX_BATCHES - 1):]
    batch = {
        "batch_id": uuid.uuid4().hex,
        "project": project,
        "created_at": now.isoformat(),
        "filename": filename,
        "pdf_sha256": hashlib.sha256(pdf).hexdigest(),
        "order_count": len(keys),
        "orders": [{field: deepcopy(order[field]) for field in
                    ("order_num", "id", "status", "purchase_at", "sum") if field in order}
                   for order in orders],
    }
    state["picking_pdf_batches"] = {**dict(retained), batch["batch_id"]: batch}
    return batch


def acknowledge_picking_pdf_batch(state, project, batch_id, *, now=None):
    """Never consult current fulfillment membership or client-supplied order IDs."""
    now = now or _now()
    if not isinstance(batch_id, str) or len(batch_id) != 32:
        raise ValueError("Najprv stiahnite PDF v tomto okne dashboardu.")
    batch = (state.get("picking_pdf_batches") or {}).get(batch_id)
    if (not isinstance(batch, dict) or batch.get("project") != project
            or _timestamp(batch.get("created_at")) <= now - BATCH_TTL):
        raise ValueError("Táto PDF dávka už nie je dostupná. Stiahnite PDF znova.")
    if batch.get("acknowledgement"):
        return deepcopy(batch["acknowledgement"])
    result = mark_picking_orders_printed(
        state, batch["orders"], printed_at=now.isoformat(), batch_id=batch_id,
    )
    result.update({"pdf_sha256": batch["pdf_sha256"], "download_order_count": batch["order_count"]})
    batch["acknowledgement"] = deepcopy(result)
    return result
