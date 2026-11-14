"""Strict no-object Reference execution for the evidenced three-column FULL SELECT.

This adapter is deliberately bounded to BIGINT/INT/STRING with small exact JSON
integers. Typed schema evidence comes from complete_result's independent API
read, never from show JSON. It does not establish incremental/snapshot semantics.
"""
from collections import Counter
from contextlib import contextmanager
import copy
import json
import re
import subprocess
import time

from capture_integrity import verify_capture
from databricks_survey import Campaign, Refused

SCHEMA = [('id', 'BIGINT', 'LONG', 0), ('amount', 'INT', 'INT', 1),
          ('tag', 'STRING', 'STRING', 2)]


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise Refused('reference:duplicate-json-key')
        result[key] = value
    return result


def assert_reference(stdout, stderr, direct, expected, sql, query_log, compiled):
    if stderr.strip():
        raise Refused('reference:stderr')
    try:
        objects = json.loads(stdout, object_pairs_hook=unique_object)
    except (ValueError, TypeError) as error:
        raise Refused('reference:ambiguous-json') from error
    if not isinstance(objects, list):
        raise Refused('reference:json-array')
    rows = []
    for obj in objects:
        if not isinstance(obj, dict) or list(obj) != ['id', 'amount', 'tag']:
            raise Refused('reference:json-columns')
        row = list(obj.values())
        if (type(row[0]) is not int or abs(row[0]) > 2**53 - 1
                or (row[1] is not None and (type(row[1]) is not int or abs(row[1]) > 2**31 - 1))
                or (row[2] is not None and type(row[2]) is not str)):
            raise Refused('reference:json-types')
        rows.append(row)
    if Counter(map(tuple, rows)) != Counter(map(tuple, expected)):
        raise Refused('reference:json-rows')
    schema = [tuple(c.get(key) for key in ('name','type_text','type_name','position'))
              for c in direct['columns']]
    if schema != SCHEMA or any(type(c.get('position')) is not int for c in direct['columns']):
        raise Refused('reference:typed-schema')
    expected_cells = [[None if cell is None else str(cell) for cell in row] for row in expected]
    if Counter(map(tuple, direct['rows'])) != Counter(map(tuple, expected_cells)):
        raise Refused('reference:typed-rows')
    # The isolated empty project has no headers/hooks/user macros. Accept precisely
    # its one SELECT, with only generated full-line metadata comments/whitespace.
    query = '\n'.join(line for line in query_log.splitlines() if not line.startswith('--'))
    normalize = lambda text: re.sub(r'\s+', ' ', text).strip()
    if (not re.match(r'(?i)^select\s', sql) or ';' in sql
            or normalize(compiled) != normalize(sql)
            or not query.rstrip().endswith(';') or query.count(';') != 1
            or normalize(query.rstrip()[:-1]) != normalize(sql)):
        raise Refused('reference:query-sql')
    return {'complete_show_rows':len(rows), 'typed_schema':True,
            'duplicate_null_multiset':True, 'exact_select_only':True}


@contextmanager
def reference_canary(campaign, label):
    # An exclusive recorder avoids attributing concurrent managed telemetry or
    # a held managed Confirm to the State-disabled reference process.
    canary = copy.copy(campaign)
    canary.a = copy.copy(campaign.a)
    canary.a.rust_url = 'http://' + campaign.env['RUN_CACHE_API_URL']
    canary.work = campaign.work / 'readonly-canaries' / label
    if canary.work.exists():
        raise Refused('reference:canary-path-reused')
    canary.work.mkdir(parents=True)
    canary.env = dict(campaign.env)
    canary.events, canary.results, canary.proxy = [], [], None
    if hasattr(canary, 'proxy_log'):
        del canary.proxy_log
    try:
        Campaign.start(canary)
        yield canary.env['RUN_CACHE_API_URL']
    finally:
        Campaign.stop(canary)
        captures = list((canary.work / 'golden').glob('*.jsonl'))
        if len(captures) != 1:
            raise Refused('reference:canary-capture')
        rows = verify_capture(captures[0])
        canary.save('certification.json', {'complete':True, 'rpcs':len(rows)})
        if rows:
            raise Refused('reference:state-rpc')


def show_reference(campaign, label, sql, expected, direct, validator=assert_reference):
    """Execute only the isolated, State-disabled show candidate proved by pilot."""
    c = campaign
    project = c.work / 'readonly-project'
    if not project.exists():
        c.save('readonly-project/dbt_project.yml', {'name':'readonly_reference', 'version':'1.0',
               'config-version':2, 'profile':'survey', 'model-paths':['models'], 'macro-paths':['macros']})
        (project/'models').mkdir(); (project/'macros').mkdir()
        (c.work/'readonly-home').mkdir()
    target = c.work/'readonly-targets'/label
    logs = c.work/'readonly-logs'/label
    if target.exists() or logs.exists():
        raise Refused('reference:artifact-path-reused')
    env = {key:c.env[key] for key in ('PATH','LANG','DBT_ENV_SECRET_DATABRICKS_TOKEN',
           'DATABRICKS_TOKEN','DATABRICKS_HOST','RUN_CACHE_API_URL','RUN_CACHE_API_SECURE') if key in c.env}
    env.update(HOME=str(c.work/'readonly-home'), DBT_ENGINE_MANAGE_STATE='false',
               DBT_SEND_ANONYMOUS_USAGE_STATS='false')
    args = [c.a.dbt,'show','--inline',sql,'--output','json','--limit','-1','--no-manage-state',
            '--no-version-check','--no-send-anonymous-usage-stats','--quiet','--project-dir',str(project),
            '--profiles-dir',str(c.profiles),'--profile','survey','--target','prod',
            '--target-path',str(target),'--log-path',str(logs)]
    c.event('readonly-reference', label=label, argv=args, managed=False)
    started = time.monotonic()
    with reference_canary(c, label) as endpoint:
        env['RUN_CACHE_API_URL'] = endpoint
        try:
            result = subprocess.run(args, cwd=project, env=env, capture_output=True, text=True, timeout=120)
            c.save('readonly-results/'+label+'.stdout',result.stdout)
            c.save('readonly-results/'+label+'.stderr',result.stderr)
        except subprocess.TimeoutExpired as error:
            for suffix, output in [('stdout',error.stdout),('stderr',error.stderr)]:
                c.save('readonly-results/'+label+'.'+suffix,
                       output.decode(errors='replace') if isinstance(output,bytes) else output or '')
            raise Refused('reference:show-timeout') from error
        finally:
            for path in logs.rglob('*'):
                if path.is_file(): path.write_text(c.clean(path.read_text()))
    c.event('readonly-result',label=label,returncode=result.returncode,elapsed=time.monotonic()-started)
    if result.returncode:
        raise Refused('reference:show-failed')
    compiled = list((target/'compiled').glob('inline_*.sql'))
    if len(compiled) != 1:
        raise Refused('reference:compiled-output')
    checks = validator(result.stdout,result.stderr,direct,expected,sql,
                              (logs/'query_log.sql').read_text(),compiled[0].read_text())
    checks['zero_reference_rpcs'] = True
    c.save('readonly-results/'+label+'.json',checks)
    return checks
