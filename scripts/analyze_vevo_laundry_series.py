"""Offline, PII-free aggregate analysis of a realized VEVO item export.

Never calls shop, advertising, email, or deployment APIs. Inputs remain private.
Amounts are the export's EUR values after discounts. Nonzero unallocated credits
block product profitability rather than being silently spread across products.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import html
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from projects.vevo.laundry_series_catalog import classify, classify_retention_components  # noqa: E402

SERIES = ("Natural", "Premium")
STATES = (*SERIES, "Both")


def money(value: str) -> Decimal:
    result = Decimal(value)
    if not result.is_finite():
        raise ValueError("Nonfinite monetary input")
    return result


def percent(n: int, d: int) -> float | None:
    return round(100 * n / d, 4) if d else None


def wilson(n: int, d: int) -> list[float] | None:
    if not d:
        return None
    z = 1.959963984540054
    p = n / d
    center = (p + z * z / (2 * d)) / (1 + z * z / d)
    half = z * ((p * (1 - p) / d + z * z / (4 * d * d)) ** .5) / (1 + z * z / d)
    return [round(100 * (center - half), 4), round(100 * (center + half), 4)]


def state(event: dict) -> str:
    return next(iter(event["series"])) if len(event["series"]) == 1 else "Both"


def cohort_metrics(sequences: list[list[dict]], end: datetime, days: int | None = None,
                   start: datetime | None = None, size: int | None = None) -> dict:
    """Each customer belongs to exactly one first-purchase cohort.

    A common launch date filters original first purchases, never resets a repeat
    customer's history. Maturity and follow-up use elapsed time, not row count.
    """
    result = {s: {"customers": 0, "repeat_any": 0, "same_any": 0,
                  "next": dict.fromkeys(STATES, 0),
                  "last": dict.fromkeys(STATES, 0)} for s in STATES}
    for seq in sequences:
        first = seq[0]
        if start and first["date"] < start:
            continue
        if size and first["sizes"] != {size}:
            continue
        if days is not None and first["date"] + timedelta(days=days) > end:
            continue
        follow = seq[1:] if days is None else [e for e in seq[1:]
                  if e["date"] <= first["date"] + timedelta(days=days)]
        origin = state(first)
        row = result[origin]
        row["customers"] += 1
        if follow:
            row["repeat_any"] += 1
            row["next"][state(follow[0])] += 1
            row["last"][state(follow[-1])] += 1
            if origin in SERIES and any(origin in event["series"] for event in follow):
                row["same_any"] += 1
    for s, row in result.items():
        n = row["customers"]
        row["repeat_any_pct"] = percent(row["repeat_any"], n)
        row["repeat_any_ci95"] = wilson(row["repeat_any"], n)
        row["same_any_pct"] = percent(row["same_any"], n) if s in SERIES else None
        row["same_any_ci95"] = wilson(row["same_any"], n) if s in SERIES else None
        row["next_same_exclusive_pct"] = percent(row["next"].get(s, 0), n)
        assert sum(row["next"].values()) == sum(row["last"].values()) == row["repeat_any"]
    return result


def series_entry_metrics(sequences: list[list[dict]], end: datetime, days: int,
                         start: datetime) -> dict:
    """Observe each series from its first purchase, including acquired switchers.

    A customer's Natural and Premium denominators can overlap. Never reset a
    series entry date to the start of the common comparison period.
    """
    result = {}
    for s in SERIES:
        n = same = any_return = 0
        for seq in sequences:
            index = next((i for i, event in enumerate(seq) if s in event["series"]), None)
            if index is None:
                continue
            first = seq[index]["date"]
            if first < start or first + timedelta(days=days) > end:
                continue
            n += 1
            follow = [e for e in seq[index + 1:] if e["date"] <= first + timedelta(days=days)]
            any_return += bool(follow)
            same += any(s in e["series"] for e in follow)
        result[s] = {"customers": n, "same_any": same, "repeat_any": any_return,
            "same_any_pct": percent(same, n), "repeat_any_pct": percent(any_return, n),
            "same_any_ci95": wilson(same, n)}
    return result


def analyze(rows: list[dict], end: date, include_pure_sets: bool = True) -> dict:
    seen = set()
    orders = {}
    selected_orders = {}
    financial_order_series = defaultdict(set)
    mixed_set_rows = Counter()
    labels = {}
    totals = {s: {"net": Decimal(0), "gross": Decimal(0), "cogs": Decimal(0),
                  "bottles": Decimal(0), "free_bottles": Decimal(0), "item_rows": 0, "pure_set_rows": 0,
                  "modeled_cost_rows": 0, "cost_sources": Counter()} for s in SERIES}
    excluded = Counter()
    notes = Counter()
    for r in rows:
        key = (r["order_num"], r["item_number"])
        if key in seen:
            raise ValueError("Duplicate order/item identity")
        seen.add(key)
        if r["realized_revenue"].lower() != "true":
            raise ValueError("Input must be the realized financial export")
        dt = datetime.fromisoformat(r["purchase_date"])
        if dt.date() > end:
            raise ValueError("Source exceeds observation end")
        email = r["customer_email"].strip().lower()
        if email in {"", "nan", "none"}:
            raise ValueError("Missing customer identity")
        order_key = r["order_num"]
        identity = (r["order_id"], email, dt)
        if order_key in orders and orders[order_key] != identity:
            raise ValueError("Conflicting order identity")
        orders[order_key] = identity
        if money(r["order_credit_net_adjustment"]) != 0:
            raise ValueError("Unallocated order credits require line-level attribution")
        product = classify(r, include_pure_sets=include_pure_sets)
        components = classify_retention_components(r) if include_pure_sets else ([product] if product else [])
        if not components:
            excluded[r["item_label"]] += 1
            continue
        quantity, net, gross, cost = (money(r[c]) for c in (
            "item_quantity", "item_total_without_tax", "item_total_with_tax", "total_expense"))
        if quantity <= 0 or net < 0 or cost < 0 or gross < net:
            raise ValueError("Nonpositive or inconsistent selected merchandise")
        if abs(net - cost - money(r["profit_before_ads"])) > Decimal("0.011"):
            raise ValueError("Export line profit does not reconcile")
        if net > 0:
            event = selected_orders.setdefault(order_key, {
                "date": dt, "email": email, "series": set(), "sizes": set(),
                "order_num": order_key, "country": r["delivery_country"] or r["invoice_country"]})
            for component in components:
                event["series"].add(component["series"])
                event["sizes"].add(component["size_ml"])
        if product is None:
            # Known fullsize perfume in a paid mixed set is a purchase. Its joint
            # price and total cost cannot be assigned entirely to perfume.
            if net > 0:
                mixed_set_rows[r["item_label"]] += 1
            excluded[r["item_label"]] += 1
            continue
        series = product["series"]
        t = totals[series]
        t["net"] += net
        t["gross"] += gross
        t["cogs"] += cost
        t["bottles" if net > 0 else "free_bottles"] += quantity * product["bottles_per_item"]
        t["item_rows"] += 1
        t["pure_set_rows"] += product["bottles_per_item"] > 1
        t["cost_sources"][r["expense_source"]] += 1
        if any(x in r["expense_source"] for x in ("margin", "fallback", "estimate")):
            t["modeled_cost_rows"] += 1
        if product.get("identity_note"):
            notes[product["identity_note"]] += 1
        if net > 0:
            financial_order_series[order_key].add(series)
        label_key = (r["item_label"], r["item_warehouse_number"], series)
        label = labels.setdefault(label_key, {"label": r["item_label"],
            "warehouse": r["item_warehouse_number"], **product, "rows": 0,
            "net": Decimal(0), "cogs": Decimal(0), "bottles": Decimal(0)})
        label["rows"] += 1
        label["net"] += net
        label["cogs"] += cost
        label["bottles"] += quantity * product["bottles_per_item"]
    by_customer = defaultdict(list)
    for event in selected_orders.values():
        by_customer[event["email"]].append(event)
    sequences = []
    for events in by_customer.values():
        seq = sorted(events, key=lambda e: (e["date"], e["order_num"]))
        if len({e["date"] for e in seq}) != len(seq):
            raise ValueError("Tied customer purchase timestamps require manual ordering")
        sequences.append(seq)
    cutoff = datetime.combine(end, time.max)
    if not all(any(s in e["series"] for e in selected_orders.values()) for s in SERIES):
        raise ValueError("Comparison requires paid purchases of both series")
    firsts = {s: min(e["date"] for e in selected_orders.values() if s in e["series"])
              for s in SERIES}
    common = max(firsts.values()).replace(hour=0, minute=0, second=0, microsecond=0)
    all_metrics = cohort_metrics(sequences, cutoff)
    financial = {}
    retention = {}
    for s, t in totals.items():
        buyers = [seq for seq in sequences if any(s in e["series"] for e in seq)]
        repeats = sum(sum(s in e["series"] for e in seq) >= 2 for seq in buyers)
        retention[s] = {"buyers": len(buyers), "buyers_repeat_same": repeats,
            "buyers_repeat_same_pct": percent(repeats, len(buyers)),
            "orders": sum(s in e["series"] for e in selected_orders.values()),
            "first_sale": firsts[s].isoformat()}
        financial_counts = Counter(orders[key][1] for key, series_set in financial_order_series.items() if s in series_set)
        financial_repeats = sum(count >= 2 for count in financial_counts.values())
        financial_dates = [orders[key][2] for key, series_set in financial_order_series.items() if s in series_set]
        financial[s] = {**t, "profit_before_other_costs": t["net"] - t["cogs"],
            "gross_margin_pct": float(100 * (t["net"] - t["cogs"]) / t["net"]) if t["net"] else None,
            "buyers": len(financial_counts), "buyers_repeat_same": financial_repeats,
            "buyers_repeat_same_pct": percent(financial_repeats, len(financial_counts)),
            "orders": sum(financial_counts.values()),
            "first_sale": min(financial_dates).isoformat() if financial_dates else None}
    transitions = Counter()
    unique_transitions = Counter()
    for seq in sequences:
        pairs = [(state(a), state(b)) for a, b in zip(seq, seq[1:])]
        transitions.update(f"{a}->{b}" for a, b in pairs)
        unique_transitions.update({f"{a}->{b}" for a, b in pairs})
    return {
        "scope": "fullsize_bottles_including_verified_mixed_sets" if include_pure_sets else "single_bottles_only",
        "financial_scope": "single_bottles_and_perfume_only_sets" if include_pure_sets else "single_bottles_only",
        "retention_mixed_set_rows": dict(sorted(mixed_set_rows.items())),
        "source_rows": len(rows), "source_orders": len(orders),
        "selected_orders": len(selected_orders), "selected_customers": len(sequences),
        "financial": financial, "retention": retention, "all_time": all_metrics,
        "windows": {str(d): cohort_metrics(sequences, cutoff, d) for d in (60, 90, 180)},
        "common_start": common.date().isoformat(),
        "common_launch_windows": {str(d): cohort_metrics(sequences, cutoff, d, common) for d in (60, 90, 180)},
        "all_buyer_series_windows": {str(d): series_entry_metrics(sequences, cutoff, d, common) for d in (60, 90, 180)},
        "entry_size_90d": {str(v): cohort_metrics(sequences, cutoff, 90, common, v) for v in (200, 500)},
        "transitions": dict(transitions), "unique_transition_customers": dict(unique_transitions),
        "identity_notes": dict(notes), "products": list(labels.values()),
        "excluded_labels": dict(sorted(excluded.items())),
    }


def serial(value):
    if isinstance(value, Decimal):
        return str(value.quantize(Decimal("0.01")))
    if isinstance(value, Counter):
        return dict(value)
    raise TypeError(type(value).__name__)


def number(value, suffix="") -> str:
    if value is None:
        return "nedostupné"
    return f"{float(value):,.2f}".replace(",", " ").replace(".", ",") + suffix


def table(headers, rows):
    return "<table><thead><tr>" + "".join(f"<th>{html.escape(str(x))}</th>" for x in headers) + \
        "</tr></thead><tbody>" + "".join("<tr>" + "".join(f"<td>{html.escape(str(x))}</td>" for x in r)
        + "</tr>" for r in rows) + "</tbody></table>"


def write_html(result: dict, output: Path, series: str | None = None):
    a = result["primary"]
    columns = [series] if series else list(SERIES)
    title = f"VEVO {series}" if series else "VEVO Natural a Premium"
    body = f"<h1>{title}: návraty zákazníkov a tržby</h1><p>{result['from']} – {result['to']}</p>"
    body += "<p>Parfumy do prania 200 a 500 ml. Zahrnuté sú samostatné fľaše a čisté balíky týchto fliaš. " \
        "Filtrujeme položky, nie celé objednávky: Natural + samostatne pridaný Shot sa ráta ako nákup Natural, " \
        "Premium + iný produkt ako nákup Premium. Ostatné položky nepridávame do parfumových tržieb ani nákladov. " \
        "Vzorky a ostatné produkty sa ignorujú. Do návratov rátame aj preukázané fľaše 200/500 ml " \
        "v hotových zmiešaných setoch. Ich spoločnú cenu však nevieme celú pripísať parfumu.</p>"
    fair = a["all_buyer_series_windows"]["90"]
    n_rate, p_rate = fair["Natural"]["same_any_pct"], fair["Premium"]["same_any_pct"]
    finding = "Porovnanie potrebuje dozreté kohorty oboch radov."
    if n_rate is not None and p_rate is not None:
        finding = ("Natural má vyššiu pozorovanú mieru opakovaného nákupu tej istej rady." if n_rate > p_rate
                   else "Premium má vyššiu pozorovanú mieru opakovaného nákupu tej istej rady." if p_rate > n_rate
                   else "Pozorované miery opakovaného nákupu tej istej rady sú rovnaké.")
    body += "<p><strong>Návrat k tej istej rade do 90 dní od jej prvého nákupu v spoločnom období: Natural " \
        + number(fair["Natural"]["same_any_pct"], " %") + ", Premium " \
        + number(fair["Premium"]["same_any_pct"], " %") + ".</strong> " \
        + finding + " Zahrnutí sú aj zákazníci získaní z druhej rady. Dáta o nákupoch samy osebe nemerajú spokojnosť.</p>"
    money_rows = []
    for s in columns:
        f = a["financial"][s]
        money_rows.append([s, number(f["net"], " €"), number(f["gross"], " €"), number(f["cogs"], " €"),
                           number(f["profit_before_other_costs"], " €"), int(f["bottles"]), f["buyers"]])
    body += "<h2>Financie: samostatné fľaše a čisté parfumové balíky</h2><p>" \
        "Táto tabuľka zahŕňa iba položky s oddeliteľnou parfumovou cenou. Parfumy v hotových zmiešaných setoch " \
        "sú zahrnuté do návratov nižšie, ale bez rozpisu ceny nie sú zahrnuté do týchto finančných súčtov.</p>" + table(
        ["Rada", "Tržby bez DPH", "Tržby s DPH", "Náklady na tovar", "Hrubá marža v €", "Platené fľaše", "Kupujúci"], money_rows)
    body += "<p>Hrubá marža = tržby bez DPH − vykázané náklady na tovar. Nie je to čistý zisk: " \
        "reklama, doprava, balenie, prevádzka a dane tu nie sú odpočítané. Meny sú prepočítané na EUR podľa reportingu. " \
        "Zľavy sú už zohľadnené. V exporte nie sú žiadne dodatočné nealokované dobropisové odpočty. " \
        "Bezplatné položky nepridávajú nákup ani návrat, ich vykázané náklady zostávajú v marži.</p>"
    body += "<h2>Prvý nákup sledovaných fliaš a ďalší návrat</h2>"
    cohort_rows = []
    for s in columns:
        c = a["all_time"][s]
        cohort_rows.append([s, c["customers"], f"{c['repeat_any']} / {number(c['repeat_any_pct'], ' %')}",
                           f"{c['same_any']} / {number(c['same_any_pct'], ' %')}", c["next"][s],
                           c["next"]["Premium" if s == "Natural" else "Natural"], c["next"]["Both"]])
    body += table(["Začal radou", "Zákazníci", "Ďalší nákup ktorejkoľvek rady", "Znovu pôvodná rada niekedy",
                   "Najbližšie len tá istá", "Najbližšie len druhá", "Najbližšie obe"], cohort_rows)
    body += "<p>Percentá majú v menovateli všetkých zákazníkov danej počiatočnej kohorty. " \
        "Viac fliaš v jednej objednávke nie je návrat. Zmena vône alebo objemu v rámci rady je návrat. " \
        "Zákazníci s oboma radmi v prvom nákupe sú samostatná kohorta: " + str(a["all_time"]["Both"]["customers"]) + ".</p>"
    body += "<h2>Všetci kupujúci rady vrátane príchodov z druhej rady</h2>" + table(
        ["Rada", "Kúpili aspoň raz", "Kúpili túto radu v aspoň 2 objednávkach", "% opakovaných"],
        [[s, a["retention"][s]["buyers"], a["retention"][s]["buyers_repeat_same"],
          number(a["retention"][s]["buyers_repeat_same_pct"], " %")] for s in columns])
    body += "<p>Tento menovateľ zahŕňa aj zákazníkov, ktorí najprv kúpili druhú radu. " \
        "Kupujúci oboch radov sa môžu objaviť v oboch riadkoch. Čas od prvého nákupu tu nie je vyrovnaný.</p>"
    body += "<h2>Rovnaké okná pre všetkých kupujúcich každej rady</h2><p>Počítané od prvého nákupu konkrétnej rady, " \
        "vrátane zákazníkov, ktorí na ňu prešli z druhej rady. Prvý nákup tejto rady musí byť od " \
        + a["common_start"] + ". Dozreté okná, históriu neposúvame; menovatele oboch radov sa môžu prekrývať.</p>"
    body += table(["Dní", "Rada", "Dozretí kupujúci", "Znova kúpili radu", "% rovnaká rada", "% akýkoľvek návrat"],
        [[d, s, c["customers"], c["same_any"], number(c["same_any_pct"], " %"), number(c["repeat_any_pct"], " %")]
         for d in (60, 90, 180) for s in columns for c in [a["all_buyer_series_windows"][str(d)][s]]])
    body += "<h2>Doplnok: okná podľa úplne prvej zvolenej rady</h2><p>Táto užšia kohorta zaraďuje zákazníka iba podľa úplne prvého nákupu sledovaných fliaš. Vstupy od spoločného obdobia predaja " + a["common_start"] + \
        ". Len zákazníci s celým uplynutým oknom; pôvodný prvý nákup sa neposúva.</p>"
    mature = []
    for days in (60, 90, 180):
        for s in columns:
            c = a["common_launch_windows"][str(days)][s]
            mature.append([days, s, c["customers"], c["same_any"], number(c["same_any_pct"], " %"),
                           number(c["repeat_any_pct"], " %"),
                           " – ".join(number(x, " %") for x in c["same_any_ci95"]) if c["same_any_ci95"] else "nedostupné"])
    body += table(["Dní", "Rada", "Dozretí zákazníci", "Návrat k pôvodnej rade", "% pôvodná rada", "% akýkoľvek návrat", "95 % interval pôvodnej rady"], mature)
    body += "<h2>Presedlanie: prvá a posledná pozorovaná voľba</h2><p>Len zákazníci s aspoň dvoma objednávkami. " \
        "Každý je v jednej bunke. Posledná objednávka obsahujúca Natural aj Premium neprideľuje výlučný bod " \
        "žiadnej rade. Shot či iný produkt sám osebe nemení zaradenie objednávky.</p>"
    body += table(["Prvá rada", "Posledná Natural", "Posledná Premium", "Posledná obe"],
                  [[s, *[a["all_time"][s]["last"][x] for x in STATES]] for s in STATES])
    body += "<p>Posledný nákup je posledná pozorovaná voľba, nie dôkaz trvalého odchodu. " \
        "Opakovaný nákup je signál preferencie, sám o sebe nedokazuje spokojnosť ani príčinu rozdielu.</p>"
    body += "<h2>Priame prechody medzi susednými nákupmi</h2>" + table(
        ["Prechod", "Počet prechodov", "Unikátni zákazníci"],
        [[f"{source} → {target}", a["transitions"].get(f"{source}->{target}", 0),
          a["unique_transition_customers"].get(f"{source}->{target}", 0)]
         for source, target in (("Natural", "Premium"), ("Premium", "Natural"))])
    body += "<p>Jeden zákazník sa môže neskôr vrátiť opačným smerom a objaviť v oboch riadkoch. " \
        "Preto tieto prechody nesčítavajte s bodovaním prvej a poslednej voľby vyššie.</p>"
    body += "<h2>Kontrola bez balíkov</h2>" + table(["Rada", "Tržby bez DPH", "Hrubá marža", "Návrat k rade do 90 dní / spoločné obdobie"],
        [[s, number(result["singles"]["financial"][s]["net"], " €"), number(result["singles"]["financial"][s]["profit_before_other_costs"], " €"),
          number(result["singles"]["all_buyer_series_windows"]["90"][s]["same_any_pct"], " %")] for s in columns])
    body += "<h2>Zmiešané sety zahrnuté do návratov</h2>" + table(
        ["Položka s overenou fľašou 200/500 ml", "Riadky"], a["retention_mixed_set_rows"].items())
    body += "<h2>Položky zahrnuté aj do finančnej tabuľky</h2>" + table(["Názov v objednávke", "Rada", "Riadky", "Fľaše vrátane bezplatných", "Tržby bez DPH"],
        [[p["label"], p["series"], p["rows"], int(p["bottles"]), number(p["net"], " €")]
         for p in sorted(a["products"], key=lambda x: (x["series"], x["label"])) if p["series"] in columns])
    body += "<h2>Zdroj a hranice výpočtu</h2><p>" + html.escape(result["source_uri"]) + "</p>"
    body += f"<p>SHA-256 exportu: <code>{result['source_sha256']}</code>. {a['source_orders']} realizovaných objednávok, " \
        f"{a['source_rows']} položiek pred filtrom. E-mail sa používa len na spojenie zákazníka a nie je súčasťou tohto reportu. " \
        "Dobropisované a stornované objednávky sú mimo finančného exportu. Nejde o samostatnú analýzu reklamácií. " \
        "Súčasná dostupnosť, reklama, cena a sezóna môžu ovplyvniť návraty. Historické náklady z reportingu nie sú účtovný čistý zisk.</p>"
    css = "body{font:16px/1.55 Arial,sans-serif;color:#18302c;max-width:1200px;margin:40px auto;padding:0 24px;background:#fcfcf9}h1{font-size:32px}h2{margin-top:40px;font-size:22px}table{border-collapse:collapse;width:100%;font-size:14px;margin:18px 0}th{background:#174e42;color:white;text-align:left}th,td{padding:10px;border-bottom:1px solid #d5ded9}tbody tr:nth-child(even){background:#edf3ef}p{max-width:1050px}code{overflow-wrap:anywhere;font-size:12px}@media(max-width:750px){table{display:block;overflow-x:auto}body{margin:20px auto;padding:0 12px}}@media print{body{margin:0}h2{break-after:avoid}tr{break-inside:avoid}}"
    output.write_text(f'<!doctype html><html lang="sk"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>{title}</title><style>{css}</style><body>{body}</body></html>', encoding="utf-8")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True, type=Path)
    p.add_argument("--output", required=True, type=Path)
    p.add_argument("--from-date", required=True, type=date.fromisoformat)
    p.add_argument("--to-date", required=True, type=date.fromisoformat)
    p.add_argument("--source-uri", required=True)
    args = p.parse_args()
    raw = args.input.read_bytes()
    with args.input.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if any(datetime.fromisoformat(r["purchase_date"]).date() < args.from_date for r in rows):
        raise ValueError("Source starts before requested interval")
    result = {"from": args.from_date.isoformat(), "to": args.to_date.isoformat(),
              "source_uri": args.source_uri, "source_sha256": hashlib.sha256(raw).hexdigest(),
              "primary": analyze(rows, args.to_date), "singles": analyze(rows, args.to_date, False)}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "analysis.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True, default=serial), encoding="utf-8")
    write_html(result, args.output / "vevo-porovnanie.html")
    for s in SERIES:
        write_html(result, args.output / f"vevo-{s.lower()}.html", s)
    compact = {k: v for k, v in result["primary"].items() if k not in {"products", "excluded_labels"}}
    print(json.dumps(compact, ensure_ascii=True, indent=2, default=serial))


if __name__ == "__main__":
    main()
