import copy
import json
from pathlib import Path
import unittest
from types import SimpleNamespace

from databricks_audit_upstream import AuditUpstream, AUDIT, SOURCE, reference_rows
from databricks_survey import Refused


class AuditControls(unittest.TestCase):
    def setUp(self):
        self.runner = AuditUpstream.__new__(AuditUpstream)
        self.runner.kind = 'table'
        self.runner.source = f'cdo_demo_db.{SOURCE}.a144_0123456789abcdef0123456789abcdef_inputs'
        self.runner.quoted_source = '.'.join('`' + p + '`' for p in self.runner.source.split('.'))
        self.runner.scope = SimpleNamespace(relation_prefix='a144_0123456789abcdef0123456789abcdef_')
        self.runner.audit_schema = AUDIT

    def request(self):
        return {'tables': [{'name': self.runner.quoted_source, 'last_modified_epoch': 123},
                           {'name': self.runner.relation, 'last_modified_epoch': 234}],
                'default_catalog': 'cdo_demo_db', 'default_schema': AUDIT,
                'freshness_tolerance_seconds': 0, 'query_dependencies': {}}

    def test_input_guard_requires_genuine_known_source_and_only_exact_audit(self):
        req = self.request()
        self.assertTrue(self.runner.request_inputs(req))
        for mutation in ('missing', 'unknown', 'zero', 'foreign', 'duplicate'):
            changed = copy.deepcopy(req)
            if mutation == 'missing': changed['tables'].pop(0)
            elif mutation == 'unknown': changed['tables'][0]['last_modified_epoch'] = None
            elif mutation == 'zero': changed['tables'][0]['last_modified_epoch'] = 0
            elif mutation == 'foreign': changed['tables'][1]['name'] += '_older'
            else: changed['tables'].append(copy.deepcopy(changed['tables'][0]))
            self.assertFalse(self.runner.request_inputs(changed), mutation)
        req['tables'][0]['last_modified_epoch'] = None
        self.assertTrue(self.runner.request_inputs(req, True))

    def test_native_sql_only_writes_exact_active_alias(self):
        relation = '.'.join('`' + p + '`' for p in self.runner.relation.split('.'))
        sql = f'create or replace table {relation} using delta as {self.runner.rendered_query(0)}'
        self.runner.check_client_sql(sql)
        for invalid in (sql.replace('_table`', '_other`'), sql + '; DROP TABLE x',
                        sql.replace(self.runner.quoted_source, '`foreign`.`x`.`y`'),
                        f'DROP TABLE {relation}', f'CREATE SCHEMA IF NOT EXISTS `cdo_demo_db`.`new`'):
            with self.assertRaises(Refused): self.runner.check_client_sql(invalid)

    def test_readonly_reference_rejects_duplicate_loss_null_or_other_sql(self):
        sql = self.runner.rendered_query(0)
        args = ('[{"id":1,"v":-2},{"id":1,"v":-2}]', '', {}, [[1,-2],[1,-2]], sql, '-- generated\n' + sql + ';', sql)
        self.assertTrue(reference_rows(*args))
        for index, value in ((0, '[{"id":1,"v":-2}]'), (0, '[{"id":1,"v":null},{"id":1,"v":-2}]'),
                             (5, sql + '; DROP TABLE x;'), (6, 'SELECT 1')):
            changed = list(args); changed[index] = value
            with self.assertRaises(Refused): reference_rows(*changed)

    def test_complete_explain_rejects_audit_as_input_and_wrong_source_time(self):
        fixture = Path(__file__).resolve().parents[1] / 'golden/fixtures/databricks_audit_upstream.jsonl'
        rows = [json.loads(line) for line in fixture.read_text().splitlines()]
        submits = {next(iter(row['response']['response'].values()))['execution_decision_id']: row
                   for row in rows if row['method'] == 'SubmitEnrichedSQL'}
        for row in rows:
            if row['method'] != 'GetExplainMessages': continue
            messages = row['response']['messages']
            submit = submits[messages[0]['execution_decision_id']]
            self.runner.source = submit['request']['tables'][0]['name'].replace('`', '')
            self.runner.quoted_source = submit['request']['tables'][0]['name']
            self.runner.check_build_explain(submit, messages)
            wrong = json.loads(json.dumps(messages).replace(self.runner.quoted_source, self.runner.relation))
            with self.assertRaises(Refused): self.runner.check_build_explain(submit, wrong)
            wrong = json.loads(json.dumps(messages).replace('2026-11-15', '2026-11-14'))
            with self.assertRaises(Refused): self.runner.check_build_explain(submit, wrong)

    def test_live_view_definition_requires_exact_genuine_source(self):
        self.runner.kind = 'view'
        metadata = {'view_definition': '(' + self.runner.rendered_query(0) + ')',
                    'view_dependencies': {'dependencies': [{'table': {'table_full_name': self.runner.source}}]}}
        history = {'rows': [[self.runner.rendered_query(0)]]}
        self.runner.check_audit_definition(metadata, history)
        for invalid in (dict(metadata, view_definition='SELECT 1'), dict(metadata, view_dependencies={'dependencies': []})):
            with self.assertRaises(Refused): self.runner.check_audit_definition(invalid, history)
        self.runner.kind = 'table'
        with self.assertRaises(Refused): self.runner.check_audit_definition({}, {'columns': [], 'rows': []})

    def test_twelve_cells_include_both_kinds_and_source_changes(self):
        plan = AuditUpstream.plan()
        self.assertEqual(len(plan), 12)
        self.assertEqual(sum(row[2] == 'skip_execution' for row in plan), 6)
        self.assertEqual([row[0] for row in plan if row[0].endswith('data_fail')], ['table_data_fail','view_data_fail'])


if __name__ == '__main__': unittest.main()
