#!/usr/bin/env python3
"""Zero-object pilot: pinned State-disabled show + typed SELECT + unchanged inventories."""
import argparse
import hashlib
import json
from pathlib import Path

from capture_integrity import verify_capture
from databricks_baseline import Baseline,LeaseScope,check_inventory
from databricks_readonly_reference import show_reference
from databricks_survey import Campaign,Refused

SQL=("SELECT cast(id as bigint) id, cast(amount as int) amount, cast(tag as string) tag FROM VALUES "
     "(1,10,'pilot'),(1,10,'pilot'),(2,cast(null as int),cast(null as string)),"
     "(3,30,'three'),(4,40,'four'),(5,50,'five'),(6,60,'six'),(7,70,'seven'),(8,80,'eight'),"
     "(9,90,'nine'),(10,100,'ten'),(11,110,'eleven') as declared(id,amount,tag)")
ROWS=[[1,10,'pilot'],[1,10,'pilot'],[2,None,None],[3,30,'three'],[4,40,'four'],[5,50,'five'],
      [6,60,'six'],[7,70,'seven'],[8,80,'eight'],[9,90,'nine'],[10,100,'ten'],[11,110,'eleven']]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('dbt','proxy','schema-lease','work-dir','run-id'):p.add_argument('--'+name,required=True)
    p.add_argument('--profiles',default=str(Path.home()/'.dbt/profiles.yml'));p.add_argument('--profile',default='databricks')
    p.add_argument('--warehouse',default='47fd3fa312624d91');args=p.parse_args()
    lease=json.loads(Path(args.schema_lease).read_text());scope=LeaseScope(args.run_id,lease);scope.allowed=set()
    c=Campaign(args,scope);c.save('pilot-runner.py',Path(__file__).read_text());c.save('schema-lease.json',lease)
    import yaml
    profile=yaml.safe_load((c.profiles/'profiles.yml').read_text())
    for role,output in profile['survey']['outputs'].items():
        profile['survey']['outputs'][role]={k:v for k,v in output.items() if k in {'type','host','http_path','catalog','schema','token','threads'}}
    c.save('profiles/profiles.yml',profile)
    before=Baseline.inventory(c);check_inventory(scope,lease,before);c.save('inventory-before.json',before)
    checks=None
    try:
        c.start()
        direct=c.sql('independent_literal',SQL);c.save('typed-direct.json',direct)
        checks=show_reference(c,'literal',SQL,ROWS,direct)
    finally:
        c.stop();after=Baseline.inventory(c);c.save('inventory-after.json',after)
        if before!=after:raise Refused('pilot inventory changed')
        captures=list((c.work/'golden').glob('*.jsonl'))
        if len(captures)!=1 or verify_capture(captures[0]):raise Refused('pilot must certify zero RPCs')
        c.save('canary.json',{'certified':True,'count':0,'sha256':hashlib.sha256(captures[0].read_bytes()).hexdigest()})
    c.save('checks.json',dict(checks,inventories_unchanged=True,physical_objects_created=0))
    return 0

if __name__=='__main__':raise SystemExit(main())
