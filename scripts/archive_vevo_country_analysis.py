"""Archive this audit's explicit evidence files privately; never change runtime aliases."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import boto3


def main():
    source = Path(__file__).resolve().parents[1] / 'data/country-analysis-20261007'
    bucket = 'biznisweb-reporting-artifacts-919341186960-eu-central-1'
    prefix = 'data/vevo/analyses/2026-10-07-country-performance/'
    session = boto3.Session(profile_name='codex', region_name='eu-central-1')
    if session.client('sts').get_caller_identity()['Account'] != '919341186960':
        raise ValueError('Unexpected AWS account')
    s3 = session.client('s3')
    block = s3.get_public_access_block(Bucket=bucket)['PublicAccessBlockConfiguration']
    if not all(block.get(k) is True for k in ['BlockPublicAcls', 'IgnorePublicAcls', 'BlockPublicPolicy', 'RestrictPublicBuckets']):
        raise ValueError('All four bucket public access blocks are required')
    names = ['VEVO_analyza_SK_CZ_HU_2026-08_09.md', 'vevo_country_comparison_20261007.json',
             'vevo_country_order_coverage_20261007.json', 'vevo_country_orders_20261007.json',
             'vevo_google_ads_20261007.json', 'vevo_meta_performance_20261007.json']
    files = [(source / 'aggregates' / name, name) for name in names]
    files += [(source / 'meta.json', 'source_meta.json'),
              (source / 'biznisweb_order_facts.json', 'source_order_facts_private.json')]
    manifest = {'archived_at': datetime.now(timezone.utc).isoformat(), 'bucket': bucket, 'prefix': prefix,
                'source_reporting_generation': 'daily-reports/vevo/20261006T231826Z/',
                'public_access_block': block, 'files': []}
    for path, name in files:
        content = path.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        key = prefix + name
        s3.put_object(Bucket=bucket, Key=key, Body=content, ServerSideEncryption='AES256',
                      ContentType='text/markdown; charset=utf-8' if name.endswith('.md') else 'application/json',
                      Metadata={'sha256': digest})
        readback = s3.get_object(Bucket=bucket, Key=key)
        if readback.get('ServerSideEncryption') != 'AES256' or hashlib.sha256(readback['Body'].read()).hexdigest() != digest:
            raise ValueError('Private archive readback verification failed')
        manifest['files'].append({'key': key, 'bytes': len(content), 'sha256': digest})
    content = (json.dumps(manifest, indent=2) + '\n').encode()
    s3.put_object(Bucket=bucket, Key=prefix + 'manifest.json', Body=content,
                  ServerSideEncryption='AES256', ContentType='application/json')
    readback = s3.get_object(Bucket=bucket, Key=prefix + 'manifest.json')['Body'].read()
    if readback != content:
        raise ValueError('Manifest readback mismatch')
    (source / 'manifest.json').write_bytes(content)
    print(json.dumps({'verified_files': len(files), 'prefix': prefix,
                      'manifest_sha256': hashlib.sha256(content).hexdigest()}))


if __name__ == '__main__':
    main()
