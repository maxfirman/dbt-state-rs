#!/usr/bin/env python3
"""Exact hosted #151 first-build postprocessing continuation; never a fresh rerun."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import tarfile

from capture_integrity import verify_capture
from databricks_snapshots import (Snapshots, CallGuard, CELLS, SOURCE, clearance_contract,
    snapshot_submit, assert_status, assert_native_sql)
from databricks_snapshot_oracle import (Ledger, NATIVE_SCHEMA, check_schema,
    source_projection_sql, assert_source)
from databricks_survey import Refused, check_manifest


def continuation_contract():
    return {'version':1,'ticket':151,'phase':'hosted','run_id':'20261114a151aa',
      'resume_after':'first_managed_confirm_before_Explain_and_physical',
      'completed':{'parse_disabled':1,'managed':1,'reference':0,'Confirm':1,'Explain':0},
      'remaining':{'parse_disabled':0,'managed':11,'reference':12,'Confirm':5,'Skip':6,
                   'Explain':12,'RecordExecution':0,'RegisterClone':0,'source_CREATE':0,
                   'initial_source_INSERT':0,'source_mutations_per_source':5,'new_permanent':1},
      'preexisting_names':clearance_contract()['permanent_exact_allowlists']['hosted'][:3],
      'required_absent':clearance_contract()['permanent_exact_allowlists']['hosted'][3],
      'max_phase_permanent':4,'max_persistent_auxiliary':0,'max_session_temporary':1,
      'required_original_records':7,'original_capture_sha256':'f2f8331d18a9f30f3da80a10e5488693a6172904dda8b096e597ee5b8dccefd5',
      'original_source_epoch':1794710691000,'original_confirm_epoch':1794710706000,
      'original_runtime':8782,'original_namespace':'f72415cee49eac9b2b28d9ad8e588507',
      'original_request_id':'01a1dd18-049f-7e9f-9b51-f75ea3f9e675',
      'original_decision_id':'01a1dd18-0498-72ff-a63d-f58ec8862a61'}


class ContinuationGuard(CallGuard):
    def __init__(self):
        super().__init__()
        self.expected=self.expected[2:]  # only original parse + first managed completed


def check_resume_inventory(scope,objects,expected):
    contract=continuation_contract()
    if (sorted(objects,key=lambda r:r['full_name'])!=sorted(expected,key=lambda r:r['full_name'])
            or {r['full_name'] for r in objects}!=set(contract['preexisting_names'])
            or any(r.get('table_type')!='MANAGED' for r in objects)
            or contract['required_absent'] in {r['full_name'] for r in objects}):
        raise Refused('continuation requires exact original three managed objects/reference absent')
    if set(scope.permanent)!=set(contract['preexisting_names'])|{contract['required_absent']}:
        raise Refused('continuation footprint changed')


def history_epoch(history,query_ids,expected_epoch=None):
    columns=history.get('columns',[]);names=[c.get('name') for c in columns]
    for name,kind in [('version','LONG'),('timestamp','TIMESTAMP'),('queryHistoryStatementId','STRING')]:
        if names.count(name)!=1 or columns[names.index(name)].get('type_name')!=kind:
            raise Refused('continuation history lacks typed version/time/query identity')
    vi,ti,qi=map(names.index,('version','timestamp','queryHistoryStatementId'))
    rows=history.get('rows',[])
    if (len(rows)!=len(query_ids) or any(len(row)!=len(columns) for row in rows)
            or {row[vi] for row in rows}!=set(map(str,range(len(query_ids))))):
        raise Refused('continuation history gained/missed a Delta version')
    for row in rows:
        if row[qi]!=query_ids[int(row[vi])]:raise Refused('continuation history query identity changed')
        if type(row[ti]) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z',row[ti]):
            raise Refused('continuation history timestamp outside primary observed UTC grammar')
    latest=max(rows,key=lambda row:int(row[vi]))
    delta=datetime.fromisoformat(latest[ti].replace('Z','+00:00'))-datetime(1970,1,1,tzinfo=timezone.utc)
    epoch=delta.days*86400000+delta.seconds*1000+delta.microseconds//1000
    if epoch<=0 or (expected_epoch is not None and epoch!=expected_epoch):
        raise Refused('continuation latest physical epoch differs from original source/Confirm')
    return epoch


def read_anchor(path,grant,run,rust_url):
    try:
        raw=Path(path).read_bytes();anchor=json.loads(raw)
        if (hashlib.sha256(raw).hexdigest()!=grant.get('continuation_anchor_sha256')
                or grant.get('continuation')!=continuation_contract()
                or rust_url is not None or run!=continuation_contract()['run_id']
                or anchor['contract']!=continuation_contract()):raise Refused('no exact hosted continuation anchor grant')
        capacity_evidence=grant['continuation_capacity_receipt']
        capacity_raw=Path(capacity_evidence['path']).read_bytes()
        if hashlib.sha256(capacity_raw).hexdigest()!=capacity_evidence['sha256']:
            raise Refused('fresh continuation capacity receipt changed')
        capacity=json.loads(capacity_raw);contract=continuation_contract()
        if (capacity['catalog']!='cdo_demo_db' or capacity['schema']!='state_20261114_snapshot151'
                or any(capacity[k]!=grant['allocation'][k] for k in ('schema_id','owner','created_at'))
                or capacity['inventory_count']!=3 or len(capacity['inventory'])!=3
                or {r['full_name'] for r in capacity['inventory']}!=set(contract['preexisting_names'])
                or any(r['table_type']!='MANAGED' or r['data_source_format']!='DELTA'
                       or r['owner']!=grant['allocation']['owner'] for r in capacity['inventory'])
                or capacity['reference_target_absent']!=contract['required_absent']
                or capacity['remaining_new_persistent']!=1 or capacity['persistent_auxiliary_allowed']!=0):
            raise Refused('fresh continuation capacity/identity/three-object scope differs')
        for p,digest in anchor['immutable_files'].items():
            if hashlib.sha256(Path(p).read_bytes()).hexdigest()!=digest:raise Refused('original attempt artifact changed')
        capture=Path(anchor['capture']);calls=verify_capture(capture)
        contract=continuation_contract()
        if (len(calls)!=7 or hashlib.sha256(capture.read_bytes()).hexdigest()!=contract['original_capture_sha256']
                or any(type(r.get('status',{}).get('code')) is not int or r['status']['code']!=0 for r in calls)):
            raise Refused('original complete successful seven-record attempt changed')
        source,target=clearance_contract()['permanent_exact_allowlists']['hosted'][:2]
        row=snapshot_submit(calls,source,target,False,'ready_to_execute');request=row['request']
        confirm=next(c for c in calls if c['method']=='ConfirmExecution')['request']
        ready=row['response']['response']['ready_to_execute']
        if (request['tables'][0]['last_modified_epoch']!=contract['original_source_epoch']
                or request['table_namespace']!=contract['original_namespace']
                or request['labels']['dbt_node_unique_id']!='snapshot.survey_'+run+'.consumer'
                or confirm['last_modified_epoch']!=contract['original_confirm_epoch']
                or confirm['execution_runtime_ms']!=contract['original_runtime']
                or confirm['request_id']!=contract['original_request_id']
                or ready['execution_decision_id']!=contract['original_decision_id']):
            raise Refused('original first identity/Confirm anchor changed')
        work=Path(anchor['original_work'])
        required=[capture,Path(str(capture)+'.capture.json'),work/'targets/002_first/run_results.json',
            work/'logs/002_first/query_log.sql',work/'targets/001_inventory/manifest.json',
            work/'operations.json',work/'capture-certification.json',work/'lossless-codec-preflight.json',
            work/'lossless-codec-State-proof.json']
        if any(str(p) not in anchor['immutable_files'] for p in required):raise Refused('original bootstrap evidence unpinned')
        events=json.loads((work/'operations.json').read_text())
        if len([e for e in events if e.get('kind')=='dbt-result' and e.get('label')=='first' and e.get('returncode')==0])!=1:
            raise Refused('original first CLI did not complete successfully')
        cert=json.loads((work/'capture-certification.json').read_text())
        if len(cert)!=1 or cert[0].get('complete') is not True or cert[0].get('sha256')!=contract['original_capture_sha256']:
            raise Refused('original capture certification changed')
        codec=json.loads((work/'lossless-codec-preflight.json').read_text());proof=json.loads((work/'lossless-codec-State-proof.json').read_text())
        if (codec['rows']!=[['Etc/UTC','2026-11-12 01:50:34.797584','1794448234797584']]
                or proof.get('state_proxy_started') is not False or proof.get('State_RPCS')!=0 or proof.get('object_writes')!=0):
            raise Refused('original before-proxy/DDL lossless codec evidence changed')
        status=json.loads((work/'targets/002_first/run_results.json').read_text())
        assert_status(status,'snapshot.survey_'+run+'.consumer',target,False)
        assert_native_sql((work/'logs/002_first/query_log.sql').read_text(),target,'sn_'+run+'_consumer__dbt_tmp',True)
        # Original source/tool snapshots remain independent of the new helper freeze.
        old_path=Path(anchor['original_freeze']);archive_path=Path(anchor['original_archive'])
        if str(old_path) not in anchor['immutable_files'] or str(archive_path) not in anchor['immutable_files']:
            raise Refused('original full source freeze/archive unpinned')
        old_freeze=json.loads(old_path.read_text())
        if (len(old_freeze['source_pins'])!=672
                or hashlib.sha256(archive_path.read_bytes()).hexdigest()!=old_freeze['source_archive_sha256']):
            raise Refused('original complete source archive changed')
        with tarfile.open(archive_path) as archive:
            if sorted(archive.getnames())!=sorted(old_freeze['source_pins']):raise Refused('original source archive member set changed')
            for p,digest in old_freeze['source_pins'].items():
                if hashlib.sha256(archive.extractfile(p).read()).hexdigest()!=digest:raise Refused('original archived source bytes changed')
        for item in old_freeze['binaries'].values():
            if hashlib.sha256(Path(item['frozen']).read_bytes()).hexdigest()!=item['sha256']:
                raise Refused('original frozen executable changed')
        return anchor,calls
    except (OSError,ValueError,KeyError,TypeError,AttributeError) as error:
        raise Refused('missing/malformed continuation evidence') from error


def combine_captures(original,continuation,output):
    """Derived replay artifact: byte-exact complete sessions in actual chronology."""
    paths=[Path(original),Path(continuation)];all_rows=[];parts=[];previous=None
    for path in paths:
        rows=verify_capture(path);raw=path.read_bytes()
        if not rows or not raw.endswith(b'\n'):raise Refused('complete nonempty newline-terminated sessions required')
        for row in rows:
            if type(row.get('status',{}).get('code')) is not int or row['status']['code']!=0:raise Refused('cannot certify successful phase with failed RPC')
            value=row.get('recorded_at','')
            if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{1,9}(?:Z|\+00:00)',value):raise Refused('missing exact capture chronology')
            seconds,fraction=value.removesuffix('Z').removesuffix('+00:00').split('.')
            epoch=datetime.fromisoformat(seconds).replace(tzinfo=timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)
            instant=(epoch.days*86400+epoch.seconds)*1000000000+int(fraction.ljust(9,'0'))
            if previous is not None and instant<previous:raise Refused('capture chronology regressed')
            previous=instant
        all_rows.extend(rows);parts.append({'path':str(path),'sha256':hashlib.sha256(raw).hexdigest(),'records':len(rows)})
    output=Path(output)
    if output.exists():raise Refused('combined capture output already exists')
    raw=b''.join(p.read_bytes() for p in paths);output.write_bytes(raw)
    Path(str(output)+'.capture.json').write_text(json.dumps({'complete':True,'failed':False,'started':len(all_rows),'recorded':len(all_rows),'composition':parts},indent=2)+'\n')
    verify_capture(output)
    receipt={'parts':parts,'records':len(all_rows),'sha256':hashlib.sha256(raw).hexdigest(),'byte_exact_concatenation':True,'no_record_republication':True}
    Path(str(output)+'.composition.json').write_text(json.dumps(receipt,indent=2)+'\n')
    return receipt


class SnapshotContinuation(Snapshots):
    entrypoint=Path(__file__)

    def __init__(self,args):
        grant=json.loads(Path(args.live_clearance).read_text())
        self.anchor,self.original_calls=read_anchor(args.continuation_anchor,grant,args.run_id,args.rust_url)
        self.bootstrap_complete=False
        self.anchor_consumed=False
        super().__init__(args)
        self.guard=ContinuationGuard()
        self.n=2  # original parse and first managed CLI retain their historical indices
        self.save('continuation-anchor.json',self.anchor)
        self.save('continuation-contract.json',continuation_contract())

    def init_schemas(self):
        allocation=self.clearance['allocation']
        schema=self.api('get','/api/2.1/unity-catalog/schemas/cdo_demo_db.'+self.scope.schema('prod'))
        if any(schema.get(k)!=allocation[k] for k in ('schema_id','owner','created_at')):raise Refused('continuation allocation changed')
        inventory=self.inventory();objects=inventory[self.scope.schema('prod')]
        check_resume_inventory(self.scope,objects,self.clearance['preexisting_objects']['hosted'])
        if type(self.clearance['schema_capacity_limit']) is not int or not len(objects)+1<=self.clearance['schema_capacity_limit']<=100:
            raise Refused('continuation final fourth slot unavailable')
        self.before_inventory=inventory;self.save('lease-inventory-before.json',inventory);self.save('allocation-identity-before.json',schema)
        contract=continuation_contract()
        # All physical/source checks are read-only and finish BEFORE State proxy or any dbt.
        original=Path(self.anchor['original_work'])
        for name in ('inputs','ref_inputs'):
            relation=self.scope.relation('prod',name)
            raw=self.observe('bootstrap_'+name,relation);check_schema(raw,NATIVE_SCHEMA[:4])
            props=json.loads((self.work/'physical'/('bootstrap_'+name+'_properties.json')).read_text())
            values={r[0]:r[1] for r in props['rows']}
            if values.get('Type')!='MANAGED' or values.get('Provider','').lower()!='delta':raise Refused('continuation source properties changed')
            full=self.sql('bootstrap_'+name+'_lossless',source_projection_sql(relation));assert_source(full,SOURCE['first'])
            history=self.sql('bootstrap_'+name+'_history','DESCRIBE HISTORY '+relation)
            ids=[]
            for label in ('create_'+name,'initial_'+name):
                paths=list((original/'sql').glob('*_'+label+'.json'))
                if len(paths)!=1 or str(paths[0]) not in self.anchor['immutable_files']:raise Refused('source write query evidence unpinned')
                ids.append(json.loads(paths[0].read_text())['statement_id'])
            history_epoch(history,ids,contract['original_source_epoch'] if name=='inputs' else None)
            self.save('physical/bootstrap_'+name+'_lossless.json',full);self.save('physical/bootstrap_'+name+'_history.json',history)
        ledger=Ledger('timestamp','ignore').execute(SOURCE['first'],'first')
        self.bootstrap_result=super().check_physical('bootstrap_first','consumer',ledger,self.frozen_results['first']['managed'])
        status=json.loads((original/'targets/002_first/run_results.json').read_text())
        history_epoch(self.bootstrap_result[1],[status['results'][0]['adapter_response']['query_id']],contract['original_confirm_epoch'])
        self.bootstrap_complete=True
        self.save('bootstrap-complete.json',{'complete':True,'before_State_proxy':self.proxy is None,'source_epoch':contract['original_source_epoch'],'target_confirm_epoch':contract['original_confirm_epoch'],'original_decision_id':contract['original_decision_id']})

    def run(self):
        result=super().run()
        summary=json.loads((self.work/'summary.json').read_text());summary.update(actual_continuation_CLI=23,inherited_CLI=2,total_phase_CLI=25)
        self.save('summary.json',summary)
        captures=list((self.work/'golden').glob('*.jsonl'))
        if len(captures)!=1:raise Refused('one complete continuation proxy session required')
        combine_captures(self.anchor['capture'],captures[0],self.work/'databricks_snapshots_hosted_whole.jsonl')
        return result

    def prepare_initial(self):
        if not self.bootstrap_complete:raise Refused('continuation bootstrap not complete')
        self.setup_project();self.save('project/models/sources.yml',{'version':2,'sources':[]});self.write_snapshots()
        manifest=json.loads((Path(self.anchor['original_work'])/'targets/001_inventory/manifest.json').read_text())
        check_manifest(self.scope,manifest)
        self.save('original-manifest.json',manifest)

    def dbt(self,label,command,target='prod',managed=True):
        if not self.bootstrap_complete:raise Refused('continuation cannot execute before bootstrap')
        return super().dbt(label,command,target,managed)

    def source_change(self,label):
        if not self.bootstrap_complete:raise Refused('continuation cannot mutate source before bootstrap')
        return super().source_change(label)

    def managed_calls(self,label):
        if label=='first':
            if not self.bootstrap_complete or self.guard.index!=0 or self.anchor_consumed:raise Refused('first evidence cannot be replayed twice')
            self.anchor_consumed=True
            return self.original_calls
        return super().managed_calls(label)

    def evidence_root(self,label):
        return Path(self.anchor['original_work']) if label=='first' else self.work

    def check_physical(self,label,name,ledger,frozen):
        if label=='first' and name=='consumer':
            if not self.bootstrap_complete:raise Refused('missing first physical bootstrap')
            return self.bootstrap_result
        return super().check_physical(label,name,ledger,frozen)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','probe','work-dir','live-clearance','run-id','operation-ledger','result-ledger','continuation-anchor'):
        parser.add_argument('--'+name,required=True)
    parser.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));parser.add_argument('--profile',default='databricks')
    parser.add_argument('--warehouse',default='47fd3fa312624d91');parser.add_argument('--rust-url')
    return SnapshotContinuation(parser.parse_args()).run()


if __name__=='__main__':raise SystemExit(main())
