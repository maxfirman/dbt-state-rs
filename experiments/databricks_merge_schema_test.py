"""Synthetic offline runner probes, never hosted fixtures or policy evidence."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from databricks_survey import Refused
import databricks_merge_schema as m


def api(schema, rows):
    return {'columns':[dict(zip(('name','type_text','type_name','position'),c),nullable=True) for c in schema],
            'rows':[list(r) for r in rows]}

class SchemaRunner(unittest.TestCase):
    def test_failed_result_keeps_three_columns_while_full_evaluation_has_four(self):
        m.validate_checkpoint('schema_fail',api(m.C4,[['1','11','one-new','n1'],['3','30','three','n3']]),
                              api(m.C3,[['1','10','one'],['2','20','retained']]),
                              api(m.C4,[['1','11','one-new','n1'],['3','30','three','n3']]))
        with self.assertRaises(Refused):
            m.validate_checkpoint('schema_fail',api(m.C4,m.B1),api(m.C4,m.B1),api(m.C4,m.B1))

    def test_missing_null_retained_keys_multiplicity_fresh_result_and_schema_are_rejected(self):
        good=api(m.C4,[['1','11','one-new','n1'],['2','20','retained',None],['3','31','three-new',None],['4','40','four','n4']])
        m.assert_typed(good,m.CELLS['next_repeat'].R)
        bads=[]
        for index in (0,1):
            bad=copy.deepcopy(good);bad['rows'].pop(index);bads.append(bad)
        bad=copy.deepcopy(good);bad['rows'][1][3]='backfilled';bads.append(bad)
        bad=copy.deepcopy(good);bad['rows'].append(bad['rows'][1]);bads.append(bad)
        bad=copy.deepcopy(good);bad['columns'][1]['type_text']='BIGINT';bads.append(bad)
        bad=copy.deepcopy(good);bad['columns'].reverse();bads.append(bad)
        bad=copy.deepcopy(good);bad['columns'][1]['nullable']=False;bads.append(bad)
        bads.append(api(m.C4,[['3','31','three-new',None],['4','40','four','n4']]))
        for bad in bads:
            with self.subTest(bad=bad),self.assertRaises(Refused):m.assert_typed(bad,m.CELLS['next_repeat'].R)

    def test_external_complete_ledger_is_mandatory_and_corruption_cannot_be_rehashed_away(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'ledger.json'
            grant={'run_ids':{'hosted':'20261115a145ee','rust':'20261115b145ff'}}
            with self.assertRaises(Refused):m.read_ledger(path,grant,'hosted','20261115a145ee')
            document=m.ledger()
            path.write_text(json.dumps(document));grant['operation_ledger_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
            frozen=m.read_ledger(path,grant,'hosted','20261115a145ee')
            self.assertEqual(frozen['schema_fail']['R']['schema'],[['id','BIGINT','LONG',0],['amount','INT','INT',1],['tag','STRING','STRING',2]])
            for mutate in (lambda d:d['cells']['recover_append']['R']['rows'].pop(1),
                           lambda d:d['cells']['schema_fail']['R'].update(schema=d['cells']['schema_fail']['F']['schema']),
                           lambda d:d['cells']['next_repeat'].pop('F')):
                bad=copy.deepcopy(document);mutate(bad);path.write_text(json.dumps(bad))
                grant['operation_ledger_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
                with self.assertRaises(Refused):m.read_ledger(path,grant,'hosted','20261115a145ee')

    def test_schema_aware_reference_requires_exact_null_complete_rows_and_select(self):
        expected=m.CELLS['recover_repeat'].R
        sql=m.literal_reference(expected)
        stdout='[{"id":1,"amount":11,"tag":"one-new","note":"n1"},{"id":2,"amount":20,"tag":"retained","note":null},{"id":3,"amount":30,"tag":"three","note":"n3"}]'
        m.assert_reference(stdout,'',api(m.C4,expected.rows),expected,sql,sql+';',sql)
        for output,query in ((stdout.replace(',"note":null',''),sql+';'),(stdout,sql+'; delete from target;')):
            with self.assertRaises(Refused):m.assert_reference(output,'',api(m.C4,expected.rows),expected,sql,query,sql)

    def test_expected_failure_needs_specific_reviewed_diagnostic_and_no_publication(self):
        contract={'exit_code':42,'required_diagnostic':'schemas out of sync: source has note; target lacks note'}
        error=Refused('client invocation exited 42; dbt/003.log: schemas out of sync: source has note; target lacks note')
        m.validate_failure(error,[],contract)
        for failure,calls,lock in ((error,[],None),
            (Refused('client invocation exited 124; timeout'),[],contract),
            (Refused('client invocation exited 42; authentication denied'),[],contract),
            (None,[],contract),
            (Refused('client invocation exited 42; authentication denied'),[],{'exit_code':42,'required_diagnostic':'authentication denied'}),
            (error,[{'method':'ConfirmExecution','response':{'success':True}}],contract),
            (error,[{'method':'RecordExecutions'}],contract),
            (error,[{'method':'RegisterClone'}],contract)):
            with self.subTest(failure=failure,calls=calls),self.assertRaises(Refused):m.validate_failure(failure,calls,lock)

    def test_failed_or_reused_target_requires_complete_unchanged_history_properties_schema_rows(self):
        before={'rows':api(m.C3,[['1','10','one'],['2','20','retained']]),
                'properties':{'rows':[['Type','MANAGED'],['Provider','delta']]},
                'history':{'columns':[{'name':'version'}],'rows':[['0','WRITE']]}}
        m.unchanged_target(before,copy.deepcopy(before))
        reordered=copy.deepcopy(before);reordered['rows']['rows'].reverse()
        m.unchanged_target(before,reordered)
        for field in ('rows','properties','history'):
            bad=copy.deepcopy(before);bad[field]['rows'].append(['unexpected'])
            with self.assertRaises(Refused):m.unchanged_target(before,bad)

    def test_failure_native_write_allowlist_rejects_target_changes_and_unknown_commands(self):
        temp='create or replace temporary view tmp as select id, amount, tag, note from source'
        m.assert_failure_sql(temp+';','source','target','tmp')
        for raw in (temp+'; alter table target add columns (note string);',
                    temp+'; merge into target using tmp on true;',
                    temp+'; truncate table target;',temp+'; grant select on target to x;'):
            with self.assertRaises(Refused):m.assert_failure_sql(raw,'source','target','tmp')

    def test_build_requires_correlated_accepted_positive_confirm(self):
        calls=[{'method':'SubmitEnrichedSQL','request':{'labels':{'dbt_node_name':'consumer'},'freshness_tolerance_seconds':0,'stale_upstream_policy':0},'response':{'response':{'ready_to_execute':{'request_id':'pending'}}}},
               {'method':'ConfirmExecution','request':{'request_id':'pending','last_modified_epoch':123,'execution_runtime_ms':4},'response':{'success':True}}]
        m.validate_success(calls,'build')
        for field,value in (('request_id','other'),('last_modified_epoch',0),('execution_runtime_ms',False)):
            bad=copy.deepcopy(calls);bad[1]['request'][field]=value
            with self.assertRaises(Refused):m.validate_success(bad,'build')

    def test_cli_bad_ledger_stops_before_credentials_or_artifact_creation(self):
        import subprocess,sys
        with tempfile.TemporaryDirectory() as tmp:
            work=Path(tmp)/'must-not-exist';grant=Path(tmp)/'grant.json';grant.write_text('{}')
            command=[sys.executable,str(Path(m.__file__)),'--dbt','/missing/dbt','--proxy','/missing/proxy','--probe','/missing/probe',
              '--work-dir',str(work),'--live-clearance',str(grant),'--operation-ledger',str(Path(tmp)/'absent'),
              '--run-id','20261115a145ee','--profiles','/missing/credentials']
            result=subprocess.run(command,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('missing or malformed complete external',result.stderr)
            self.assertFalse(work.exists())

    def test_unknown_failure_engagement_and_wildcard_scope_stop(self):
        with self.assertRaises(Refused):m.validate_request([], 'source','target',True,m.CELLS['schema_fail'])
        grant=m.clearance_contract();scope=m.SchemaScope('20261115a145ee',grant)
        self.assertEqual(len(scope.permanent),2)
        scope.check('cdo_demo_db.state_20261114_append146.me_20261115a145ee_consumer')
        with self.assertRaises(Refused):scope.check('cdo_demo_db.state_20261114_append146.protected')
        grant=copy.deepcopy(grant);grant['persistent_auxiliary_exact_spellings']['hosted']=['*']
        with self.assertRaises(Refused):m.SchemaScope('20261115a145ee',grant)

    def test_disabled_schema_reference_certifies_zero_rpcs_at_external_process_boundary(self):
        import os
        from types import SimpleNamespace
        from databricks_survey import Campaign
        # Synthetic external executables; no credentials, dbt, Cloud or warehouse.
        recorder=r"""#!/usr/bin/env python3
import json,os,signal,socket,time
from pathlib import Path
running=True
def stop(*_):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
sock=socket.socket();host,port=os.environ['PROXY_LISTEN'].split(':');sock.bind((host,int(port)));sock.listen()
while running:time.sleep(.01)
path=Path(os.environ['GOLDEN_DIR'])/'synthetic.jsonl';path.parent.mkdir(parents=True)
n=int(os.environ['SYNTHETIC_REFERENCE_RPCS']);path.write_text(''.join(json.dumps({'method':'unexpected'})+'\n' for _ in range(n)))
Path(str(path)+'.capture.json').write_text(json.dumps({'complete':True,'failed':False,'started':n,'recorded':n}))
"""
        client=r"""#!/usr/bin/env python3
import json,sys
from pathlib import Path
args=sys.argv
def value(key):return args[args.index(key)+1]
sql=value('--inline');target=Path(value('--target-path'))/'compiled';target.mkdir(parents=True)
(target/'inline_synthetic.sql').write_text(sql)
logs=Path(value('--log-path'));logs.mkdir(parents=True);(logs/'query_log.sql').write_text(sql+';')
print('[{"id":1,"amount":11,"tag":"one-new","note":"n1"},{"id":2,"amount":20,"tag":"retained","note":null},{"id":3,"amount":30,"tag":"three","note":"n3"}]')
"""
        for count in (0,1):
            with self.subTest(rpcs=count),tempfile.TemporaryDirectory() as tmp:
                work=Path(tmp);proxy=work/'recorder';proxy.write_text(recorder);proxy.chmod(0o755)
                dbt=work/'client';dbt.write_text(client);dbt.chmod(0o755)
                campaign=Campaign.__new__(Campaign);campaign.work=work;campaign.secrets=[];campaign.events=[];campaign.results=[]
                campaign.a=SimpleNamespace(dbt=str(dbt),proxy=str(proxy));campaign.profiles=work/'profiles';campaign.profiles.mkdir()
                campaign.env={'PATH':os.environ['PATH'],'LANG':'C','RUN_CACHE_API_URL':'127.0.0.1:1','SYNTHETIC_REFERENCE_RPCS':str(count)}
                bag=m.CELLS['recover_repeat'].R;sql=m.literal_reference(bag)
                if count:
                    with self.assertRaisesRegex(Refused,'reference:state-rpc'):
                        m.show_reference(campaign,'synthetic',sql,bag,api(m.C4,bag.rows),validator=m.assert_reference)
                else:
                    checks=m.show_reference(campaign,'synthetic',sql,bag,api(m.C4,bag.rows),validator=m.assert_reference)
                    self.assertTrue(checks['zero_reference_rpcs'])

if __name__=='__main__':unittest.main()
