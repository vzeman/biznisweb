"""Combine frozen, aggregate VEVO evidence; no network or customer-level data."""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docs-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'data/country-analysis-20261007/aggregates')
    args = parser.parse_args()
    def read(name):
        return json.loads((args.docs_dir / name).read_text(encoding='utf-8'))
    orders = read('vevo_country_orders_20261007.json')
    coverage = read('vevo_country_order_coverage_20261007.json')
    meta = read('vevo_meta_performance_20261007.json')
    google = read('vevo_google_ads_20261007.json')
    if not coverage['complete_boundary']:
        raise ValueError('Incomplete source reconciliation')
    rows = []
    for month in ['2026-08', '2026-09', '2026-10']:
        period = orders['periods'][month if month != '2026-10' else '2026-10-01_06']
        google_spend = sum(float(r['metrics'].get('cost_micros', 0)) / 1e6
                           for r in google['data']['customer_month'] if r['segments']['month'][:7] == month)
        for country in ['SK', 'CZ', 'HU']:
            archive = period['currency_shops'][country]
            included = [r for r in coverage['rows'] if r['month'] == month and r['country'] == country and r['included']]
            if sum(r['orders'] for r in included) != archive['orders']:
                raise ValueError('Live included-order count differs from archive')
            if abs(sum(r['item_net_eur'] for r in included) - archive['net_item_revenue_eur']) > .02:
                raise ValueError('Live included revenue differs beyond rounding tolerance')
            missing = [r for r in coverage['rows'] if r['month'] == month and r['country'] == country
                       and country == 'HU' and r['status_id'] == '4' and r['payment_id'] == '16'
                       and not r['included'] and r['reason'] == 'cod_status_without_cod_payment']
            missing_count = sum(r['orders'] for r in missing)
            missing_revenue = round(sum(r['item_net_eur'] for r in missing), 2)
            count = archive['orders'] + missing_count
            revenue = round(archive['net_item_revenue_eur'] + missing_revenue, 2)
            meta_spend = sum(r['spend'] for r in meta['country_monthly']
                             if r['date_start'][:7] == month and r['country'] == country)
            spend = meta_spend + (google_spend if country == 'SK' else 0)
            contribution = None if missing_count else round(archive['modeled_cm1_before_ads_fixed_eur'] - spend, 2)
            rows.append({'month': month, 'country': country, 'reported_orders': archive['orders'],
                         'reported_revenue': archive['net_item_revenue_eur'], 'omitted_cod_orders': missing_count,
                         'omitted_cod_revenue': missing_revenue, 'corrected_eligible_orders': count,
                         'corrected_net_merchandise_revenue': revenue, 'aov': revenue / count if count else None,
                         'meta_spend': round(meta_spend, 6), 'google_sk_campaign_spend': google_spend if country == 'SK' else 0,
                         'ads_spend': round(spend, 6), 'blended_mer': revenue / spend if spend else None,
                         'estimated_contribution_after_ads_before_fixed': contribution,
                         'revenue_less_ads_before_all_costs': round(revenue - spend, 2)})
    result = {'rows': rows, 'methodology': [
        'Corrected revenue adds only audited shipped HU COD16, not unpaid or cancelled orders; not collected-cash proof.',
        'Archived EUR net item values retain original reporting rounding; HU added-source rounding may differ by EUR0.01.',
        'Meta spend uses physical country; campaign targeting agrees with country names to less than EUR0.11 monthly.',
        'All configured Google spend assigned to sole spending SK Brand campaign; current destination is vevo.sk; historical URLs not proven.',
        'MER is all-shop net merchandise revenue / paid spend, not platform ROAS or incremental return.',
        'Contribution is product margin less modeled EUR0.50/order and ads, before shared overhead; per-country creditnote fulfillment not allocated.',
        'HU complete product costs missing from archived report; contribution intentionally null, not zero.',
        'Fixed overhead EUR70/day remains a management estimate and is not arbitrarily allocated to countries.',
        'October covers only October1–6; acquisition attribution is not fully mature.'
    ]}
    (args.docs_dir / 'vevo_country_comparison_20261007.json').write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
