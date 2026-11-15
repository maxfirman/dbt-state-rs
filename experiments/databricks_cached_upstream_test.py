"""Fault controls for source-backed cached outcomes; no warehouse access."""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from databricks_cached_upstream import UpstreamScope, UpstreamTests
from databricks_survey import Refused


class UpstreamCachedControls(unittest.TestCase):
    def runner(self):
        runner = object.__new__(UpstreamTests)
        runner.source = 'cdo_demo_db.ds_20261112a134ef_prod.t143_' + 'a' * 32 + '_inputs'
        runner.quoted_source = '.'.join('`' + part + '`' for part in runner.source.split('.'))
        runner.scope = SimpleNamespace(schema=lambda role: 'ds_20261112a134ef_prod')
        runner.audit_schema = 'ds_20261112a134ef_prod_dbt_test__audit'
        return runner

    def test_changed_failure_cannot_pass_with_cached_success_or_lost_duplicate(self):
        runner = self.runner()
        result = {'columns': [{'name': n, 'type_text': 'INT', 'type_name': 'INT', 'position': i}
                              for i, n in enumerate(('id', 'v'))],
                  'rows': [['1', '-2'], ['1', '-2']]}
        runner.assert_rows(result, 2)
        for rows in ([], [['1', '-2']], [['1', '2'], ['1', '2']]):
            with self.assertRaises(Refused):
                runner.assert_rows({**result, 'rows': rows}, 2)
        with self.assertRaises(Refused):
            runner.assert_rows(result, 0)
        changed = copy.deepcopy(result)
        changed['columns'][0]['type_text'] = 'BIGINT'
        with self.assertRaises(Refused):
            runner.assert_rows(changed, 2)

    def test_ordinary_cache_needs_the_exact_source_and_known_epoch(self):
        runner = self.runner()
        request = {'tables': [{'name': runner.source, 'last_modified_epoch': 10}],
                   'query_dependencies': [], 'freshness_tolerance_seconds': 0}
        self.assertTrue(runner.request_inputs(request))
        for tables in ([], [{'name': runner.source, 'last_modified_epoch': None}],
                       [{'name': runner.source + '_other', 'last_modified_epoch': 10}]):
            self.assertFalse(runner.request_inputs({**request, 'tables': tables}))
        self.assertTrue(runner.request_inputs({**request, 'tables': [{'name': runner.source, 'last_modified_epoch': None}]}, speculative=True))
        self.assertFalse(runner.request_inputs({**request, 'tables': []}, speculative=True))

    def test_client_probes_are_exact_and_cannot_read_other_tables(self):
        runner = self.runner()
        exact = ("SELECT last_altered, (table_type = 'VIEW' OR table_type = 'MATERIALIZED_VIEW') AS is_view "
                 "FROM `cdo_demo_db`.INFORMATION_SCHEMA.TABLES "
                 f"WHERE table_schema = 'ds_20261112a134ef_prod' AND table_name = '{runner.source.split('.')[-1]}'")
        runner.check_client_sql(exact)
        runner.check_client_sql(f'DESCRIBE EXTENDED {runner.quoted_source}')
        for statement in (exact.replace("table_schema = 'ds_20261112a134ef_prod'", "table_schema = 'another_schema'"),
                          exact.replace("INFORMATION_SCHEMA.TABLES", "INFORMATION_SCHEMA.COLUMNS"),
                          exact.replace(runner.source.split('.')[-1], runner.source.split('.')[-1] + '_old'),
                          runner.rendered_query(0) + ' UNION ALL SELECT * FROM other_table'):
            with self.assertRaises(Refused):
                runner.check_client_sql(statement)

    def test_inventory_follows_pages_and_refuses_nonadjacent_token_cycle(self):
        runner = self.runner()
        pages = iter([{'tables': [], 'next_page_token': 'A'}, {'tables': [], 'next_page_token': 'B'},
                      {'tables': [], 'next_page_token': 'A'}])
        runner.api = lambda *args: next(pages)
        with self.assertRaisesRegex(Refused, 'pagination-loop'):
            runner.inventory()
        pages = iter([{'tables': [], 'next_page_token': 'A'}, {'tables': []}])
        runner.api = lambda *args: next(pages)
        self.assertEqual(runner.inventory(), {'ds_20261112a134ef_prod': []})

    def test_build_explain_rejects_raw_epoch_instead_of_hosted_temporal_shape(self):
        runner = self.runner()
        def line(text, children=()):
            return {'text': text, 'marker': None, 'badge': None, 'children': list(children)}
        def messages(text):
            return [{'explain_lines': [line('data freshness analysis', [line('upstream dependencies',
                     [line(runner.quoted_source, [line(text)])])])]}]
        submit = {'request': {'tables': [{'last_modified_epoch': 1794750940000}]},
                  'response': {'response': {'ready_to_execute': {}}}}
        runner.check_build_explain(submit, messages('last updated: 9 seconds ago, 2026-11-15 13:55:40 UTC'))
        for text in ('last updated: 1794750940000 (UTC)',
                     'last updated: 9 seconds ago, 2026-11-15 13:55:41 UTC'):
            with self.assertRaises(Refused):
                runner.check_build_explain(submit, messages(text))

    def test_scope_is_one_uuid_in_digest_verified_container(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'ownership.json'
            prod, ref1, ref2 = ('ds_20261112a134ef_' + role for role in ('prod', 'ref', 'ref2'))
            # The original two reference roles must themselves satisfy LeaseScope.
            ref2 = 'ds_20261112e134ef_ref'
            path.write_text(json.dumps({'retained_schemas': [prod, ref1, ref2]}))
            lease = {'catalog': 'cdo_demo_db', 'roles': {'prod': ref1, 'ref': ref2},
                     'source_evidence': [{'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}]}
            args = SimpleNamespace(run_id='20261115a143aa', schema=prod, object_id='a' * 32)
            scope = UpstreamScope(args, lease)
            self.assertEqual(len(scope.allowed), 1)
            owned = next(iter(scope.allowed))
            scope.check(owned)
            for other in (owned + '_old', owned.replace(prod, ref1)):
                with self.assertRaises(Refused):
                    scope.check(other)
            args.schema = 'ds_20261112b134ef_prod'
            with self.assertRaises(Refused):
                UpstreamScope(args, lease)
            args.schema = prod
            path.write_text('{}')
            with self.assertRaises(Refused):
                UpstreamScope(args, lease)


if __name__ == '__main__':
    unittest.main()
