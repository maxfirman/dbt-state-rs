"""Independent audit-output oracle: rows, physical identity and recorded R/fresh F."""
import copy
import unittest

from databricks_survey import Refused
from databricks_audit import assert_audit, audit_client_sql, AuditScope, default_name_authorization, assert_fallback_calls, rust_error_authorization, assert_managed_audit_tables


class AuditOracle(unittest.TestCase):
    def test_default_name_control_requires_exact_separate_retirement_and_live_grants(self):
        names = ['cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144aa',
                 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144ab']
        grant = {'status': 'CLEARED exact144default-name hosted four-cell control',
                 'schema_id': 'dcabff91-9a47-4ff0-9f53-2b71c06f414a',
                 'exact_permanent_objects': names, 'retirement_receipt_sha256': 'digest',
                 'schema': 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit',
                 'managed_project': 'audit_default_20261114a144ad',
                 'reference_project': 'audit_default_ref_20261114a144ad',
                 'body_nonces': ['20261114a144adtablea', '20261114a144adtableb'],
                 'limits': {'managed': 4, 'compile': 4, 'disabled_references': 4,
                            'max_permanent': 2, 'extra_persistent': 0}}
        receipt = {'status': 'RETIRED exact144aaab owned outputs for default-name control',
                   'exact_objects': names, 'metadata_absent': True,
                   'protected_inventory_preserved': True}
        default_name_authorization(grant, receipt, 'digest', '20261114a144aa', False)
        for field, wrong in [('status', 'not cleared'), ('schema_id', 'other'),
                             ('exact_permanent_objects', names + ['other']),
                             ('retirement_receipt_sha256', 'wrong'), ('managed_project', 'old_uid'),
                             ('body_nonces', ['old']), ('limits', {'managed': 12})]:
            changed = copy.deepcopy(grant); changed[field] = wrong
            with self.subTest(field=field), self.assertRaises(Refused):
                default_name_authorization(changed, receipt, 'digest', '20261114a144aa', False)
        for field in ['metadata_absent', 'protected_inventory_preserved']:
            changed = copy.deepcopy(receipt); changed[field] = False
            with self.subTest(field=field), self.assertRaises(Refused):
                default_name_authorization(grant, changed, 'digest', '20261114a144aa', False)
        for run, rust in [('20261114b144bb', False), ('20261114a144aa', True)]:
            with self.assertRaises(Refused):
                default_name_authorization(grant, receipt, 'digest', run, rust)

    def test_expected_fallback_rejects_status_mutations_and_published_results(self):
        status = lambda code, message: {'code': code, 'message': message, 'details': [],
                                        'metadata': {'ascii': [], 'binary': []}}
        request = {'sql': 'SELECT 1 AS id', 'tables': [], 'query_dependencies': []}
        ordinary = copy.deepcopy(request)
        ordinary['tables'] = [{'name': 'cdo_demo_db.audit.owned', 'last_modified_epoch': 1}]
        calls = [{'method': 'SubmitEnrichedSQLSpeculative', 'request': request,
                  'status': status(0, ''), 'response': {'response': {'undecided': {}}}},
                 {'method': 'SubmitEnrichedSQL', 'request': ordinary,
                  'status': status(3, 'Failed to process SQL'), 'response': None}]
        assert_fallback_calls(calls)
        for fault in ['success', 'message', 'details', 'metadata', 'response',
                      'confirm', 'record-executions', 'submit-values', 'no-speculative', 'extra-delta', 'wrong-epoch']:
            changed = copy.deepcopy(calls)
            if fault == 'success': changed[-1]['status']['code'] = 0
            if fault == 'message': changed[-1]['status']['message'] = 'other error'
            if fault == 'details': changed[-1]['status']['details'] = ['unexpected']
            if fault == 'metadata': changed[-1]['status']['metadata']['ascii'] = [['x', 'y']]
            if fault == 'response': changed[-1]['response'] = {'response': {'ready_to_execute': {}}}
            if fault == 'confirm': changed.append({'method': 'ConfirmExecution', 'status': status(0, '')})
            if fault == 'record-executions': changed.append({'method': 'RecordExecutions', 'status': status(0, '')})
            if fault == 'submit-values': changed.append({'method': 'SubmitValues', 'status': status(0, '')})
            if fault == 'no-speculative': changed.pop(0)
            if fault == 'extra-delta': changed[-1]['request']['sql'] = 'other SQL'
            if fault == 'wrong-epoch': changed[-1]['request']['tables'][0]['last_modified_epoch'] = None
            with self.subTest(fault=fault), self.assertRaises(Refused):
                assert_fallback_calls(changed)

    def test_rust_expected_error_admission_is_exact_and_separate_from_hosted(self):
        names = ['cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114b144bb',
                 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114b144bc']
        grant = {'status': 'CLEARED exact144Rust expected-error alias5', 'mode': 'alias5',
                 'hosted_replay_success': True, 'contract_sha256': 'contract',
                 'hosted_causal_fixture_sha256': '6a32d653285fbaf225d2f08f41cde8097eb52527a4fecae523c9abd01d3dca92',
                 'schema_id': 'dcabff91-9a47-4ff0-9f53-2b71c06f414a',
                 'exact_permanent_objects': names,
                 'limits': {'managed': 5, 'compile': 5, 'disabled_references': 5,
                            'max_permanent': 2, 'extra_persistent': 0}}
        self.assertEqual(rust_error_authorization(grant, '20261114b144bb', 'contract'), 'alias5')
        for field, wrong in [('status', 'hosted clearance'), ('mode', 'table-view12'),
                             ('hosted_replay_success', False), ('contract_sha256', 'wrong'),
                             ('exact_permanent_objects', names + ['other']),
                             ('limits', {'managed': 12})]:
            changed = copy.deepcopy(grant); changed[field] = wrong
            with self.subTest(field=field), self.assertRaises(Refused):
                rust_error_authorization(changed, '20261114b144bb', 'contract')
        with self.assertRaises(Refused):
            rust_error_authorization(grant, '20261114a144aa', 'contract')

    def test_reported_managed_audit_cannot_be_the_allowed_reference_alias(self):
        managed = 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114b144bb'
        reference = managed.replace('sf_20261114b144bb', 'sf_20261114b144bc')
        clearance = {'status': 'LIVE CLEARED bounded #144 source-less audit partial',
                     'allocation': {'schema': 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit',
                                    'schema_id': 'dcabff91-9a47-4ff0-9f53-2b71c06f414a'},
                     'exact_permanent_objects': ['sf_20261114b144bb', 'sf_20261114b144bc']}
        scope = AuditScope('20261114b144bb', clearance)
        for name in (managed, '.'.join('`' + p + '`' for p in managed.split('.'))):
            scope.check(name)
            assert_managed_audit_tables([{'name': name, 'last_modified_epoch': 1}], managed)
        scope.check(reference)  # Both are owned; ownership alone is not role evidence.
        for wrong in (reference, '.'.join('`' + p + '`' for p in reference.split('.')),
                      managed.replace('cdo_demo_db', '`cdo_demo_db'), managed + '`'):
            with self.subTest(name=wrong), self.assertRaises(Refused):
                assert_managed_audit_tables([{'name': wrong, 'last_modified_epoch': 1}], managed)

    def test_audit_retains_duplicates_null_schema_and_physical_identity(self):
        rows = {'columns': [dict(name=n, type_text='INT', type_name='INT', position=p)
                            for p, n in enumerate(['id', 'v'])],
                'rows': [['1', '2'], ['1', '2'], ['2', None]]}
        props = {'rows': [['Type', 'MANAGED'], ['Provider', 'delta']]}
        metadata = {'full_name': 'cdo_demo_db.s.a', 'table_type': 'MANAGED', 'data_source_format': 'DELTA'}
        assert_audit(rows, props, metadata, 'cdo_demo_db.s.a', 'table', 3)
        mutations = []
        for change in ('duplicate', 'null', 'type', 'position', 'kind', 'provider', 'name'):
            r, p, m = copy.deepcopy((rows, props, metadata))
            if change == 'duplicate': r['rows'].pop(0)
            if change == 'null': r['rows'][-1][-1] = '0'
            if change == 'type': r['columns'][1]['type_name'] = 'STRING'
            if change == 'position': r['columns'][1]['position'] = True
            if change == 'kind': p['rows'][0][1] = 'VIEW'
            if change == 'provider': m['data_source_format'] = 'PARQUET'
            if change == 'name': m['full_name'] = 'cdo_demo_db.other.a'
            mutations.append((change, r, p, m))
        for change, r, p, m in mutations:
            with self.subTest(change=change), self.assertRaises(Refused):
                assert_audit(r, p, m, 'cdo_demo_db.s.a', 'table', 3)
        with self.assertRaises(Refused):
            assert_audit(rows, props, metadata, 'cdo_demo_db.s.a', 'table', 0)

    def test_native_system_metadata_is_scoped_before_it_is_admitted(self):
        statement = ("SELECT table_name FROM `system`.`information_schema`.`tables` "
                     "WHERE table_catalog = 'cdo_demo_db' AND table_schema = 'ds_20261112a134ef_ref_dbt_test__audit'")
        audit_client_sql(statement, 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144ab')
        for wrong in (statement.replace("table_catalog = 'cdo_demo_db'", "table_catalog = 'other'"),
                      statement.replace("table_schema = 'ds_20261112a134ef_ref_dbt_test__audit'", 'true'),
                      statement + " OR 1=1", statement.replace(" WHERE", " JOIN (SELECT 1) x ON true WHERE"), statement.replace('SELECT table_name', 'DELETE')):
            with self.subTest(sql=wrong), self.assertRaises(Refused):
                audit_client_sql(wrong, 'cdo_demo_db.ds_20261112a134ef_ref_dbt_test__audit.sf_20261114a144ab')

    def test_exact_clearance_is_required_before_campaign_initialization(self):
        for grant in ({}, {'status': 'LIVE CLEARED bounded #144 source-less audit partial'},
                      {'allocation': {'schema': 'cdo_demo_db.other', 'schema_id': 'wrong'}}):
            with self.assertRaises(Refused):
                AuditScope('20261114a144aa', grant)


if __name__ == '__main__':
    unittest.main()
