"""Private same-response Meta source receipts for an authorized VEVO probe.

This observes responses returned by Session.send, including any retries hidden
inside its unchanged HTTP adapter. It does not claim physical request counts,
fetch data, change queries, or calculate report economics.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import parse_qsl, quote, quote_plus, unquote, unquote_plus, urlencode, urlparse

ACCOUNT = 'act_1131531703632101'
ORIGIN = 'https://graph.facebook.com'
VERSION = 'v21.0'
# Meta may advertise a newer version in native paging metadata. Requests still
# use VERSION and the provider follows only cursors against its original URL.
PAGING_VERSIONS = frozenset({VERSION, 'v24.0'})
PUBLIC_PARAMS = frozenset({'fields', 'level', 'time_range', 'time_increment', 'breakdowns', 'limit', 'after'})
AUTH_PARAMS = frozenset({'access_token', 'appsecret_proof'})
DAILY_FIELDS = frozenset({'spend', 'impressions', 'clicks', 'cpc', 'cpm', 'ctr', 'reach',
                         'frequency', 'unique_clicks', 'cost_per_unique_click'})
PRIMARY_DAILY_FIELDS = frozenset({'spend', 'impressions', 'clicks', 'cpc', 'cpm', 'ctr'})
IDENTITY_FIELDS = frozenset({'id', 'currency', 'timezone_name'})
MAX_BODY = 8 * 1024 * 1024


def require(condition, reason):
    if not condition:
        raise RuntimeError('probe-meta-source-' + reason)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def decode(raw):
    def unique(pairs):
        require(len(pairs) == len(dict(pairs)), 'duplicate-json-key')
        return dict(pairs)
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: require(False, 'nonfinite-json'))


def credential_variants(values):
    result = set()
    for value in values:
        if isinstance(value, str) and value:
            result.update((value, json.dumps(value, ensure_ascii=True)[1:-1],
                           quote(value, safe=''), quote_plus(value, safe='')))
    return sorted(result, key=len, reverse=True)


def credential_reflected(value, variants):
    return any(secret in candidate for candidate in (value, unquote(value), unquote_plus(value))
               for secret in variants)


def _endpoint(value, paths):
    try:
        parsed = urlparse(value)
        port = parsed.port
    except (TypeError, ValueError):
        raise RuntimeError('probe-meta-source-endpoint') from None
    require(parsed.scheme == 'https' and parsed.hostname == 'graph.facebook.com'
            and port is None and not parsed.username and not parsed.password
            and not parsed.fragment and not parsed.params, 'endpoint')
    require(parsed.path in paths, 'account-version-path')
    return parsed


def endpoint(value):
    return _endpoint(value, {f'/{VERSION}/{ACCOUNT}', f'/{VERSION}/{ACCOUNT}/insights'})


def paging_endpoint(value):
    return _endpoint(value, {f'/{version}/{ACCOUNT}/insights' for version in PAGING_VERSIONS})


def paging_query(parsed, direction, request_query):
    """Admit only native paging metadata for the unchanged request's scope."""
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    require(len(pairs) == len(dict(pairs)), 'paging-query-duplicates')
    cursor_key = 'after' if direction == 'next' else 'before'
    allowed = PUBLIC_PARAMS | ({'before'} if direction == 'previous' else set())
    require(set(dict(pairs)) <= allowed | AUTH_PARAMS, 'paging-query-key')
    public = {key: value for key, value in pairs if key in allowed}
    expected = {key: value for key, value in request_query.items() if key != 'after'}
    require(set(public) == set(expected) | {cursor_key}, 'paging-query-scope')

    def normalized(query):
        value = dict(query)
        if 'time_range' in value:
            value['time_range'] = decode(value['time_range'].encode())
        if 'fields' in value:
            fields = value['fields'].split(',')
            require(len(fields) == len(set(fields)), 'paging-query-fields')
            value['fields'] = sorted(fields)
        return value

    require(normalized({key: value for key, value in public.items() if key != cursor_key})
            == normalized(expected), 'paging-query-scope')
    return public, any(key in AUTH_PARAMS for key, _ in pairs)


def public_query(parsed):
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    require(len(pairs) == len(dict(pairs)), 'duplicate-query-key')
    query = dict(pairs)
    require(set(query) <= PUBLIC_PARAMS | AUTH_PARAMS, 'unexpected-query-key')
    return {key: value for key, value in query.items() if key in PUBLIC_PARAMS}, [
        value for key, value in query.items() if key in AUTH_PARAMS]


class MetaSourceSnapshot:
    """Record an exact authorized host's native input pages, then bind a manifest."""

    def __init__(self, identity, ready_sha, from_date, to_date, prefix, put, *, credentials=(), source_paths=None,
                 capture_mode='probe'):
        require(capture_mode in ('probe', 'live'), 'capture-mode')
        identity_fields = {'marker', 'release_id', 'source_commit', 'task_arn', 'private_ip', 'image_digest',
                           'task_definition', 'instance_id', 'service', 'path', 'project', 'gate_sha256'}
        if capture_mode == 'live':
            identity_fields |= {'report_to_date', 'skip_email', 'skip_invoices', 'skip_inline_guard'}
            require(identity.get('report_to_date') == to_date and all(identity.get(key) is True for key in
                    ('skip_email', 'skip_invoices', 'skip_inline_guard')), 'live-report-boundary')
        require(set(identity) == identity_fields
                and identity.get('marker') == ('VEVO_REPORT_PROBE_HOST_OK' if capture_mode == 'probe'
                                               else 'VEVO_REPORT_IMAGE_LIVE_HOST_OK')
                and identity.get('project') == 'vevo' and identity.get('path') == '/app'
                and identity.get('instance_id') == 'N/A:Fargate'
                and identity.get('service') == 'vevo-daily-report-email'
                and re.fullmatch('[a-f0-9]{32}', identity.get('release_id', ''))
                and re.fullmatch('[a-f0-9]{40}', identity.get('source_commit', ''))
                and re.fullmatch(r'sha256:[a-f0-9]{64}', identity.get('image_digest', ''))
                and re.fullmatch('[a-f0-9]{64}', identity.get('gate_sha256', ''))
                and re.fullmatch(r'arn:aws:ecs:eu-central-1:919341186960:task/vevo-reporting-cluster/[a-f0-9]{32}', identity.get('task_arn', ''))
                and re.fullmatch(r'arn:aws:ecs:eu-central-1:919341186960:task-definition/vevo-reporting-daily:[0-9]+', identity.get('task_definition', ''))
                and re.fullmatch(r'(?:[0-9]{1,3}\.){3}[0-9]{1,3}', identity.get('private_ip', '')), 'host-binding')
        require(ready_sha == sha(canonical(identity)), 'ready-binding')
        expected_prefix = ('data/vevo/reporting/runtime/probes/' if capture_mode == 'probe'
                           else 'daily-reports/vevo/source-inputs/') + identity['release_id'] + '/'
        require(prefix == expected_prefix, 'prefix')
        first, last = date.fromisoformat(from_date), date.fromisoformat(to_date)
        require(first <= last and first.isoformat() == from_date and last.isoformat() == to_date, 'dates')
        self.identity, self.ready_sha = decode(canonical(identity)), ready_sha
        self.from_date, self.to_date = from_date, to_date
        self.capture_mode = capture_mode
        self.prefix = prefix + ('artifacts/source-inputs/meta/' if capture_mode == 'probe' else 'meta/')
        self.put = put
        self.credentials, self.pages, self.groups, self.errors = tuple(credentials), [], {}, []
        self.currency_reads, self.last_currency_page, self.finished = 0, None, False
        paths = source_paths if source_paths is not None else (
            Path(__file__), Path(__file__).with_name('reporting_migration_host_gate.py'),
            Path(__file__).parents[1] / 'facebook_ads.py', *(
                (Path(__file__).with_name('vevo_report_image_host.py'),) if capture_mode == 'live' else ()))
        self.sources = {Path(path): sha(Path(path).read_bytes()) for path in paths}
        require(self.sources and len({path.name for path in self.sources}) == len(self.sources), 'source-paths')
        self.periods = {'latest': (from_date, to_date)}
        for days in (7, 30, 90):
            self.periods[f'{days}d'] = (max(first, last - timedelta(days=days - 1)).isoformat(), to_date)

    def _source_gate(self):
        require(not self.finished and self.ready_sha == sha(canonical(self.identity))
                and all(sha(path.read_bytes()) == digest for path, digest in self.sources.items()), 'code-changed-or-finished')

    def _sanitize(self, raw, secrets, request_query):
        variants = credential_variants(secrets)
        original = decode(raw)
        def inspect(value, path=()):
            if path in (('paging', 'next'), ('paging', 'previous')):
                return
            if isinstance(value, dict):
                for key, child in value.items():
                    require(not credential_reflected(key, variants), 'credential-reflection')
                    inspect(child, path + (key,))
            elif isinstance(value, list):
                for child in value:
                    inspect(child, path)
            elif isinstance(value, str):
                require(not credential_reflected(value, variants), 'credential-reflection')
        inspect(original)
        safe = raw
        for value in variants:
            safe = safe.replace(value.encode(), b'[REDACTED_CREDENTIAL]')
        require(not any(value.encode() in safe for value in variants), 'credential-reflection')
        payload = decode(safe)
        redacted = safe != raw
        paging_replacements = {}
        # Paging URLs are never followed by this recorder. Preserve their public
        # native query/cursor projection without token or other credential keys.
        paging = payload.get('paging') if isinstance(payload, dict) else None
        if paging is not None:
            require(isinstance(paging, dict) and set(paging) <= {'cursors', 'next', 'previous'}, 'paging-shape')
            for name in ('next', 'previous'):
                if name in paging:
                    parsed = paging_endpoint(paging[name])
                    public, has_auth = paging_query(parsed, name, request_query)
                    redacted = redacted or has_auth
                    cursor_key = 'after' if name == 'next' else 'before'
                    cursors = paging.get('cursors')
                    require(isinstance(cursors, dict) and isinstance(cursors.get(cursor_key), str)
                            and cursors[cursor_key] and public[cursor_key] == cursors[cursor_key],
                            'server-cursor-mismatch')
                    original_url = paging[name]
                    paging[name] = ORIGIN + parsed.path + ('?' + urlencode(public) if public else '')
                    paging_replacements[original_url] = paging[name]
        # Replace only paging URL string tokens, preserving the exact native
        # monetary number/string bytes and all noncredential response content.
        def replace_paging(match):
            value = decode(match.group())
            return json.dumps(paging_replacements[value]).encode() if value in paging_replacements else match.group()
        stored = re.sub(rb'"(?:\\.|[^"\\])*"', replace_paging, safe)
        decode(stored)
        require(not any(value.encode() in stored for value in variants), 'stored-credential-reflection')
        return payload, stored, redacted

    @staticmethod
    def selected(request):
        """Ignore existing campaign/ad/hourly queries before strict receipt checks."""
        parsed = urlparse(request.url)
        if parsed.hostname != 'graph.facebook.com' or not re.search(r'/act_[^/]+(?:/insights)?$', parsed.path):
            return False
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if parsed.path.endswith('/insights'):
            # The unchanged client fetches optional detailed metrics again for
            # weekday statistics. They are not the daily-spend input to verify.
            if set(query.get('fields', '').split(',')) == DAILY_FIELDS:
                return False
            return query.get('level') == 'account' and (query.get('breakdowns') == 'country'
                or ('breakdowns' not in query and query.get('time_increment') == '1'))
        return 'currency' in query.get('fields', '').split(',')

    def observe_failure(self, request):
        if self.selected(request):
            self.errors.append('selected-transport-failure')

    def observe(self, request, response):
        """Called before returning the original response to the unchanged client."""
        if not self.selected(request):
            return False
        try:
            self._source_gate()
            parsed = endpoint(request.url)
            require(request.method == 'GET' and endpoint(response.url).path == parsed.path, 'method-response-origin')
            query, query_secrets = public_query(parsed)
            fields = set(query.get('fields', '').split(','))
            identity = parsed.path == f'/{VERSION}/{ACCOUNT}'
            if identity:
                if not fields <= IDENTITY_FIELDS or 'currency' not in fields:
                    return False
                require(set(query) == {'fields'}, 'identity-query')
                kind, limits = 'identity', None
            else:
                if query.get('level') != 'account' or query.get('breakdowns') not in (None, 'country'):
                    return False
                require(fields <= DAILY_FIELDS and 'spend' in fields and query.get('limit') == '500', 'insights-fields-limit')
                limits = decode(query.get('time_range', '').encode())
                require(isinstance(limits, dict) and set(limits) == {'since', 'until'}
                        and (limits['since'], limits['until']) in self.periods.values(), 'insights-period')
                if query.get('breakdowns') == 'country':
                    require(fields == {'spend'} and query.get('time_increment') in (None, '1'), 'country-query')
                    kind = 'country_daily' if query.get('time_increment') == '1' else 'country'
                else:
                    if query.get('time_increment') != '1':
                        return False
                    kind = 'daily'
            require(response.status_code == 200 and isinstance(response.content, bytes)
                    and 0 < len(response.content) <= MAX_BODY, 'response-status-size')
            authorization = request.headers.get('Authorization', '')
            bearer = authorization[7:] if authorization.startswith('Bearer ') else ''
            secrets = (*self.credentials, *query_secrets, bearer)
            require(not credential_reflected(canonical(query).decode(), credential_variants(secrets)), 'query-credential-reflection')
            payload, stored, redacted = self._sanitize(response.content, secrets, query)
            require(isinstance(payload, dict), 'response-object')
            group = None
            if kind == 'identity':
                require(set(payload) <= IDENTITY_FIELDS and payload.get('currency') == 'EUR'
                        and payload.get('id') in (ACCOUNT, ACCOUNT[4:]), 'native-account-currency')
                self.currency_reads += 1
            else:
                require(set(payload) <= {'data', 'paging'} and isinstance(payload.get('data'), list), 'insights-response')
                base_query = {key: value for key, value in query.items() if key != 'after'}
                base_query['time_range'] = limits
                group_id = sha(canonical({'path': parsed.path, 'query': base_query}))
                group = self.groups.get(group_id)
                if group is None:
                    require('after' not in query, 'first-page-cursor')
                    group = {'kind': kind, 'query': base_query, 'pages': [], 'expected_after': None,
                             'complete': False, 'seen_cursors': set(), 'row_keys': set(),
                             'currency_metadata_page': self.last_currency_page}
                    require(kind not in {'country', 'country_daily'} or self.last_currency_page is not None, 'country-currency-missing')
                    self.groups[group_id] = group
                else:
                    require(not group['complete'] and query.get('after') == group['expected_after'], 'repeated-or-wrong-page')
                rows = payload['data']
                for row in rows:
                    allowed = fields | {'date_start', 'date_stop'} | ({'country'} if kind in {'country', 'country_daily'} else set())
                    require(isinstance(row, dict) and {'spend', 'date_start', 'date_stop'} <= set(row)
                            and set(row) <= allowed, 'native-row-shape')
                    try:
                        amount = Decimal(str(row['spend']))
                    except (InvalidOperation, ValueError):
                        require(False, 'native-spend')
                    require(not isinstance(row['spend'], bool) and amount.is_finite() and amount >= 0, 'native-spend')
                    if kind == 'daily':
                        require(isinstance(row['date_start'], str) and len(row['date_start']) == 10
                                and date.fromisoformat(row['date_start']).isoformat() == row['date_start']
                                and row['date_start'] == row['date_stop']
                                and limits['since'] <= row['date_start'] <= limits['until'], 'daily-date')
                        row_key = row['date_start']
                    elif kind == 'country_daily':
                        try:
                            native_date = date.fromisoformat(row['date_start']).isoformat()
                        except (TypeError, ValueError):
                            native_date = None
                        require(isinstance(row['date_start'], str) and native_date == row['date_start']
                                and row['date_start'] == row['date_stop']
                                and limits['since'] <= row['date_start'] <= limits['until']
                                and isinstance(row.get('country'), str)
                                and (row['country'] == 'unknown' or re.fullmatch('[A-Za-z]{2}', row['country'])),
                                'country-daily-row')
                        row_key = (row['date_start'], row['country'].lower())
                    else:
                        require(row['date_start'] == limits['since'] and row['date_stop'] == limits['until']
                                and isinstance(row.get('country'), str)
                                and (row['country'] == 'unknown' or re.fullmatch('[A-Za-z]{2}', row['country'])), 'country-row')
                        row_key = row['country'].lower()
                    require(row_key not in group['row_keys'], 'duplicate-native-row')
                    group['row_keys'].add(row_key)
                paging = payload.get('paging') or {}
                next_url = paging.get('next')
                if next_url:
                    cursor = (paging.get('cursors') or {}).get('after')
                    require(rows and isinstance(cursor, str) and cursor and cursor not in group['seen_cursors'], 'nonadvancing-cursor')
                    next_query = dict(parse_qsl(paging_endpoint(next_url).query))
                    require(next_query.get('after') == cursor, 'server-cursor-mismatch')
                    group['seen_cursors'].add(cursor)
                    group['expected_after'] = cursor
                else:
                    group['complete'] = True
                    group['expected_after'] = None
            number = len(self.pages) + 1
            require(number <= 300, 'response-budget')
            key = self.prefix + f'{number:04d}-response.json'
            self.put(key, stored)
            record = {'key': key, 'size': len(stored), 'sha256': sha(stored),
                      'original_response_sha256': sha(response.content), 'credential_redaction': redacted,
                      'at': datetime.now(timezone.utc).isoformat(), 'method': 'GET', 'origin': ORIGIN,
                      'path': parsed.path, 'public_params': query, 'status': 200, 'kind': kind,
                      'headers_or_credentials_persisted': False}
            self.put(self.prefix + f'{number:04d}-receipt.json', canonical(record))
            self.pages.append(record)
            if kind == 'identity':
                self.last_currency_page = number
            if group is not None:
                group['pages'].append(number)
            return True
        except BaseException:
            self.errors.append('selected-response-rejected')
            raise

    def finalize(self):
        self._source_gate()
        require(not self.errors and self.currency_reads and self.groups, 'incomplete-or-rejected')
        require(all(group['complete'] for group in self.groups.values()), 'unfinished-pagination')
        country_ranges = {(group['query']['time_range']['since'], group['query']['time_range']['until'])
                          for group in self.groups.values() if group['kind'] == 'country'}
        require(country_ranges == set(self.periods.values()), 'country-periods-incomplete')
        country_daily_ranges = {(group['query']['time_range']['since'], group['query']['time_range']['until'])
                                for group in self.groups.values() if group['kind'] == 'country_daily'}
        if country_daily_ranges:
            require(country_daily_ranges == set(self.periods.values()), 'country-daily-periods-incomplete')
        require(any(group['kind'] == 'daily' and group['query']['time_range'] == {
            'since': self.from_date, 'until': self.to_date}
            and set(group['query']['fields'].split(',')) == PRIMARY_DAILY_FIELDS
            for group in self.groups.values()), 'daily-full-range-missing')
        groups = {key: {name: value for name, value in group.items()
                        if name not in {'expected_after', 'seen_cursors', 'row_keys'}} for key, group in self.groups.items()}
        value = {'schema_version': 1, 'project': 'vevo', 'capture_mode': self.capture_mode,
                 'source_commit': self.identity['source_commit'],
                 'release_id': self.identity['release_id'], 'image_digest': self.identity['image_digest'],
                 'host_identity': self.identity, 'ready_sha256': self.ready_sha,
                 'report_from_date': self.from_date, 'report_to_date': self.to_date,
                 'meta_account_id': ACCOUNT, 'currency': 'EUR', 'complete': True,
                 'native_currency_metadata_responses': self.currency_reads,
                 'observed_session_send_responses': len(self.pages), 'physical_http_attempt_count': None,
                 'physical_http_attempt_count_status': 'unknown-unchanged-provider-adapter',
                 'source_files': {path.name: digest for path, digest in self.sources.items()},
                 'native_responses': self.pages, 'query_groups': groups,
                 'provider_queries_changed': False, 'financial_calculation_changed': False,
                 'credential_values_or_headers_persisted': False}
        raw = canonical(value)
        key = self.prefix + 'source-manifest.json'
        try:
            self.put(key, raw)
        except BaseException:
            self.errors.append('manifest-write-rejected')
            raise
        self.finished = True
        return {'key': key, 'sha256': sha(raw), 'size': len(raw)}
