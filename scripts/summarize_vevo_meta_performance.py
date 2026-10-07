"""Summarize a saved Meta audit without requerying or summing overlapping actions."""
import argparse
import hashlib
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path


def action(row, name, field='actions', window='value'):
    matches = [float(a.get(window, 0)) for a in row.get(field, []) if a['action_type'] == name]
    if len(matches) > 1:
        raise ValueError('Duplicate exact action type')
    return matches[0] if matches else 0.0


def ratio(a, b):
    return round(a / b, 6) if b else None


def normalize(row):
    result = {k: row[k] for k in ['date_start', 'date_stop', 'country', 'campaign_id', 'campaign_name'] if k in row}
    result.update({k: float(row.get(k) or 0) for k in ['spend', 'impressions', 'clicks', 'inline_link_clicks']})
    result['purchases'] = action(row, 'offsite_conversion.fb_pixel_purchase', window='7d_click')
    result['purchase_value'] = action(row, 'offsite_conversion.fb_pixel_purchase', 'action_values', '7d_click')
    result['api_value_purchases'] = action(row, 'offsite_conversion.fb_pixel_purchase')
    result['api_value_purchase_value'] = action(row, 'offsite_conversion.fb_pixel_purchase', 'action_values')
    result['view_1d_purchases_separate'] = action(row, 'offsite_conversion.fb_pixel_purchase', window='1d_view')
    result['landing_page_views'] = action(row, 'landing_page_view')
    result['cpm'] = ratio(result['spend'] * 1000, result['impressions'])
    result['link_ctr_pct'] = ratio(result['inline_link_clicks'] * 100, result['impressions'])
    result['link_cpc'] = ratio(result['spend'], result['inline_link_clicks'])
    result['lpv_per_link_click_pct'] = ratio(result['landing_page_views'] * 100, result['inline_link_clicks'])
    result['purchases_per_lpv_pct'] = ratio(result['purchases'] * 100, result['landing_page_views'])
    result['purchase_cpa'] = ratio(result['spend'], result['purchases'])
    result['platform_purchase_roas'] = ratio(result['purchase_value'], result['spend'])
    result['frequency'] = float(row['frequency']) if 'frequency' in row else None
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding='utf-8'))
    output = {k: payload[k] for k in ['generated_at', 'metadata', 'definition', 'campaign_configuration']}
    output['definition'] = ('Primary purchases and purchase value use exact offsite_conversion.fb_pixel_purchase '
                            'with explicit 7d_click and impression dates. API value and 1d_view are preserved '
                            'separately and are never added. An absent action/window has zero returned actions. '
                            'Purchase values retain the platform event basis, not assumed VAT or shipping treatment.')
    output['source_sha256'] = hashlib.sha256(args.input.read_bytes()).hexdigest()
    for key in ['country_monthly', 'account_monthly', 'campaign_monthly']:
        output[key] = [normalize(row) for row in payload[key]]
    output['spend_reconciliation'] = []
    for account in output['account_monthly']:
        month = account['date_start']
        sums = {key: sum(r['spend'] for r in output[key] if r['date_start'] == month)
                for key in ['country_monthly', 'campaign_monthly']}
        if any(abs(value - account['spend']) > .02 for value in sums.values()):
            raise ValueError('Country/campaign spend does not reconcile')
        output['spend_reconciliation'].append({'date_start': month, 'account': account['spend'], **sums})
    daily = [normalize(r) for r in payload['campaign_daily']]
    country_map = {}
    for campaign in payload['campaign_configuration']:
        country = campaign['name'].split('-')[0]
        adsets = campaign.get('adsets', [])
        if country not in ('SK', 'CZ', 'HU') or campaign.get('adsets_truncated') or not adsets:
            raise ValueError('Incomplete campaign targeting evidence')
        if any(set(a.get('countries') or []) != {country} for a in adsets):
            raise ValueError('Campaign name and actual current country targeting disagree')
        country_map[campaign['id']] = country
    output['country_daily'] = []
    output['country_weekly'] = []
    for grain in ['daily', 'weekly']:
        groups = defaultdict(list)
        for row in daily:
            day = date.fromisoformat(row['date_start'])
            period = day if grain == 'daily' else day - timedelta(days=day.weekday())
            country = country_map[row['campaign_id']]
            groups[(str(period), country)].append(row)
        for (period, country), rows in sorted(groups.items()):
            output['country_' + grain].append({
                'date_start': period, 'country': country,
                **{k: round(sum(r[k] for r in rows), 6) for k in
                   ['spend', 'impressions', 'inline_link_clicks', 'landing_page_views', 'purchases', 'purchase_value']}})
    output['daily_weekly_definition'] = 'Campaign target-country grouping verified against current adsets; '
    output['daily_weekly_definition'] += 'daily purchase actions summed; weekly unique reach/frequency not inferred.'
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('Country/campaign monthly spend reconciled to account; summary saved.')


if __name__ == '__main__':
    main()
