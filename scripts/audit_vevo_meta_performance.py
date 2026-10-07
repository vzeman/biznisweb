"""Read-only VEVO Meta Insights audit; credentials remain only in memory."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secret-id', required=True)
    parser.add_argument('--profile', default='codex')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    secret = json.loads(boto3.Session(profile_name=args.profile, region_name='eu-central-1')
                        .client('secretsmanager').get_secret_value(SecretId=args.secret_id)['SecretString'])
    account = secret['FACEBOOK_AD_ACCOUNT_ID']
    if not account.startswith('act_'):
        account = 'act_' + account
    session = requests.Session()
    session.headers['Authorization'] = 'Bearer ' + secret['FACEBOOK_ACCESS_TOKEN']
    base = 'https://graph.facebook.com/v21.0/'

    def get(path, params):
        response = session.get(base + path, params=params, timeout=90)
        body = response.json()
        if not response.ok or 'error' in body:
            error = body.get('error', {})
            raise RuntimeError(f'Meta GET failed: HTTP {response.status_code}, '
                               f'code {error.get("code")}, subcode {error.get("error_subcode")}')
        return body

    def pages(params):
        result = []
        params = dict(params, limit=500)
        for _ in range(40):
            body = get(account + '/insights', params)
            result.extend(body.get('data', []))
            paging = body.get('paging', {})
            if not paging.get('next'):
                return result
            after = paging.get('cursors', {}).get('after')
            if not after or after == params.get('after'):
                raise RuntimeError('Meta pagination did not advance')
            params['after'] = after
        raise RuntimeError('Meta pagination exceeded 40 pages')

    metadata = get(account, {'fields': 'id,name,currency,timezone_name'})
    if metadata.get('currency') != 'EUR':
        raise RuntimeError('Expected EUR Meta account; review currency before combining')
    fields = ('date_start,date_stop,spend,impressions,reach,frequency,clicks,inline_link_clicks,'
              'outbound_clicks,actions,action_values')
    common = {'time_range': json.dumps({'since': '2026-07-01', 'until': '2026-10-06'}),
              'time_increment': 'monthly', 'fields': fields,
              'action_attribution_windows': json.dumps(['7d_click', '1d_view']),
              'action_report_time': 'impression'}
    output = {'generated_at': datetime.now(timezone.utc).isoformat(), 'metadata': metadata,
              'definition': 'Meta reported actions, 7-day click + 1-day view; impression date. '
                            'Use exact offsite_conversion.fb_pixel_purchase, never sum overlapping action types.'}
    for key, override in [('country_monthly', {'level': 'account', 'breakdowns': 'country'}),
                          ('account_monthly', {'level': 'account'}),
                          ('campaign_monthly', {'level': 'campaign', 'fields': fields + ',campaign_id,campaign_name'}),
                          ('campaign_daily', {'level': 'campaign', 'time_increment': 1,
                                              'fields': fields + ',campaign_id,campaign_name'})]:
        output[key] = pages(dict(common, **override))
        print(key, len(output[key]), flush=True)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    output['campaign_configuration'] = []
    for campaign_id in sorted({r['campaign_id'] for r in output['campaign_monthly']}):
        campaign = get(campaign_id, {'fields': 'id,name,status,effective_status,objective,start_time,stop_time'})
        adsets = get(campaign_id + '/adsets', {
            'fields': 'id,name,status,effective_status,optimization_goal,billing_event,attribution_spec,promoted_object,targeting',
            'limit': 100})
        campaign['adsets_truncated'] = bool(adsets.get('paging', {}).get('next'))
        campaign['adsets'] = [
            {**{k: a.get(k) for k in ['id', 'name', 'status', 'effective_status', 'optimization_goal',
                                     'billing_event', 'attribution_spec', 'promoted_object']},
             'countries': a.get('targeting', {}).get('geo_locations', {}).get('countries')}
            for a in adsets.get('data', [])]
        if 'UGC-Contest' in campaign['name']:
            ads = get(campaign_id + '/ads', {
                'fields': 'name,effective_status,creative{object_story_spec,asset_feed_spec,object_url}',
                'limit': 100})
            def public_urls(value):
                result = set()
                if isinstance(value, dict):
                    for key, child in value.items():
                        if key in ('link', 'website_url', 'object_url') and isinstance(child, str):
                            parsed = urlsplit(child)
                            if parsed.scheme in ('http', 'https'):
                                result.add(f'{parsed.scheme}://{parsed.netloc}{parsed.path}')
                        elif isinstance(child, (dict, list)):
                            result.update(public_urls(child))
                elif isinstance(value, list):
                    for child in value:
                        result.update(public_urls(child))
                return result
            campaign['ads_truncated'] = bool(ads.get('paging', {}).get('next'))
            campaign['ads'] = [{'name': a.get('name'), 'effective_status': a.get('effective_status'),
                                'destination_urls': sorted(public_urls(a.get('creative', {})))}
                               for a in ads.get('data', [])]
        output['campaign_configuration'].append(campaign)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    session.close()
    print('Saved aggregate Meta evidence', args.output)


if __name__ == '__main__':
    main()
