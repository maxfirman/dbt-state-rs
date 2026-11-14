"""Independent bounded SCD ledger from native 2.0.11 snapshot macro evidence.

No State logic or actual target rows enter Ledger. Timestamp-ignore is exact.
Check/invalidate use one symbolic warehouse clock per executed statement. The
constraint checker nominates clocks from lossless timestamp fields (never hashes),
requires independent same-warehouse before/after bounds, and compares complete
histories under ONE shared assignment. This establishes bounded, not exact clock
fidelity. It is not live acceptance or support for defective new_record behavior.
"""
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
import hashlib
from itertools import product
import re
from databricks_survey import Refused

EPOCH=datetime(1970,1,1)
STAMP=r'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}'


def micros(value):
    if type(value) is not str or not re.fullmatch(STAMP,value):
        raise Refused('SCD timestamp needs lossless six-microsecond SQL UTC projection')
    try:delta=datetime.strptime(value,'%Y-%m-%d %H:%M:%S.%f')-EPOCH
    except ValueError as error:raise Refused('invalid SCD timestamp') from error
    return delta.days*86400000000+delta.seconds*1000000+delta.microseconds


def stamp(value):
    if type(value) is not int:raise Refused('warehouse clock must be integer microseconds')
    return (EPOCH+timedelta(microseconds=value)).strftime('%Y-%m-%d %H:%M:%S.%f')


def spark_stamp(value):
    micros(value)
    return value.rstrip('0').rstrip('.')


@dataclass(frozen=True)
class Clock:
    statement: str


@dataclass(frozen=True)
class Version:
    business: tuple
    updated: object
    start: object
    end: object=None


@dataclass(frozen=True)
class Bounds:
    before_us: int
    after_us: int
    before_query_id: str
    after_query_id: str
    warehouse_id: str
    statement: str

    def check(self):
        if (type(self.before_us) is not int or type(self.after_us) is not int
                or self.before_us>self.after_us or self.after_us-self.before_us>600000000
                or not all(type(v) is str and v for v in
                    (self.before_query_id,self.after_query_id,self.warehouse_id,self.statement))
                or self.before_query_id==self.after_query_id):
            raise Refused('missing/invalid independent warehouse-clock bounds')


class Ledger:
    def __init__(self,strategy,hard_deletes,versions=(),statements=()):
        if strategy not in ('timestamp','check') or hard_deletes not in ('ignore','invalidate'):
            raise Refused('unsupported snapshot strategy/delete mode; new_record blocker remains')
        self.strategy=strategy;self.hard_deletes=hard_deletes
        self.versions=tuple(versions);self.statements=tuple(statements)

    def execute(self,source,statement):
        if not statement or statement in self.statements:raise Refused('statement identity reused')
        rows=tuple(tuple(r) for r in source)
        if any(len(r)!=4 or type(r[0]) is not str or not r[0].isdigit()
               or (r[1] is not None and type(r[1]) is not str)
               or type(r[2]) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\d',r[2]) for r in rows):
            raise Refused('outside bounded unique non-NULL scalar-key/business schema')
        keys=[r[0] for r in rows]
        if len(set(keys))!=len(keys):raise Refused('duplicate source key requires native characterization')
        for r in rows:micros(r[3])
        current={v.business[0]:i for i,v in enumerate(self.versions) if v.end is None}
        versions=list(self.versions)
        clock=Clock(statement)
        for row in rows:
            prior=current.get(row[0]);old=versions[prior] if prior is not None else None
            when=row[3] if self.strategy=='timestamp' else clock
            changed=(old is None or (micros(old.start)<micros(row[3]) if self.strategy=='timestamp'
                                   else old.business[1]!=row[1]))
            if changed:
                if old is not None:versions[prior]=replace(old,end=when)
                versions.append(Version(row,when,when))
        if self.hard_deletes=='invalidate':
            for key,index in current.items():
                if key not in keys:versions[index]=replace(versions[index],end=clock)
        return Ledger(self.strategy,self.hard_deletes,versions,self.statements+(statement,))

    def rows(self,assignment):
        def resolve(v):return stamp(assignment[v.statement]) if isinstance(v,Clock) else v
        rows=[]
        for version in self.versions:
            updated,start,end=map(resolve,(version.updated,version.start,version.end))
            digest=hashlib.md5((version.business[0]+'|'+spark_stamp(updated)).encode()).hexdigest()
            rows.append((*version.business,digest,updated,start,end))
        return tuple(rows)

    def clock_names(self):
        return sorted({x.statement for v in self.versions for x in (v.updated,v.start,v.end) if isinstance(x,Clock)})


def assert_history(actual,ledger,bounds):
    rows=tuple(tuple(r) for r in actual)
    if any(len(r)!=8 for r in rows):raise Refused('incomplete SCD row projection')
    for row in rows:
        for i in (3,5,6,7):
            if row[i] is not None:micros(row[i])
    names=ledger.clock_names()
    if set(bounds)!=set(names):raise Refused('missing/extra independently acquired statement bounds')
    choices=[];previous=None;warehouse=None;query_ids=set()
    observed={micros(r[i]) for r in rows for i in (5,6,7) if r[i] is not None}
    # Operation order matters; labels need not sort chronologically.
    ordered=[n for n in ledger.statements if n in names]
    for name in ordered:
        interval=bounds[name];interval.check()
        if interval.statement!=name:raise Refused('bounds belong to another statement')
        if warehouse is not None and warehouse!=interval.warehouse_id:raise Refused('warehouse clock changed')
        if previous is not None and previous>=interval.before_us:raise Refused('overlapping/nonforward warehouse bounds')
        ids={interval.before_query_id,interval.after_query_id}
        if ids & query_ids:raise Refused('warehouse bound query reused')
        query_ids.update(ids);warehouse=interval.warehouse_id;previous=interval.after_us
        candidates=sorted(t for t in observed if interval.before_us<=t<=interval.after_us)
        if not candidates:raise Refused('no independently bounded warehouse statement clock')
        choices.append(candidates)
    combinations=1
    for values in choices:combinations*=len(values)
    if combinations>100000:raise Refused('clock constraint search exceeds frozen bound')
    actual_counter=Counter(rows)
    for values in product(*choices):
        assignment=dict(zip(ordered,values))
        if Counter(ledger.rows(assignment))==actual_counter:
            return {'clock_evidence':'bounded shared statement variables' if names else 'exact source timestamps',
                    'statement_clock_us':assignment,'complete_scd_multiset':True}
    raise Refused('complete independent SCD ledger mismatch')

NATIVE_SCHEMA=(('id','BIGINT','LONG',0),('v','STRING','STRING',1),('p','DATE','DATE',2),
               ('ts','TIMESTAMP','TIMESTAMP',3),('dbt_scd_id','STRING','STRING',4),
               ('dbt_updated_at','TIMESTAMP','TIMESTAMP',5),('dbt_valid_from','TIMESTAMP','TIMESTAMP',6),
               ('dbt_valid_to','TIMESTAMP','TIMESTAMP',7))
PROJECTION_SCHEMA=tuple((n,'STRING','STRING',i) if i in (3,5,6,7) else (n,t,k,i)
                        for n,t,k,i in NATIVE_SCHEMA)+tuple(
    (n+'_us','BIGINT','LONG',8+i) for i,n in enumerate(('ts','dbt_updated_at','dbt_valid_from','dbt_valid_to')))


def check_schema(result,schema):
    columns=result.get('columns',[])
    if (tuple(tuple(c.get(k) for k in ('name','type_text','type_name','position')) for c in columns)!=schema
            or any(type(c.get('position')) is not int for c in columns)):
        raise Refused('physical:snapshot-schema')


def projection_sql(target):
    # timezone is checked separately by the native preflight; no SET/macro override.
    columns=[]
    for name,_,_,i in NATIVE_SCHEMA:
        columns.append("date_format("+name+", 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS "+name if i in (3,5,6,7) else name)
    columns.extend('unix_micros('+name+') AS '+name+'_us' for name in ('ts','dbt_updated_at','dbt_valid_from','dbt_valid_to'))
    return 'SELECT '+','.join(columns)+' FROM '+target


def decode_projection(result):
    check_schema(result,PROJECTION_SCHEMA)
    rows=[]
    for raw in result['rows']:
        if len(raw)!=12:raise Refused('physical:incomplete-lossless-projection')
        for i,j in ((3,8),(5,9),(6,10),(7,11)):
            value,epoch=raw[i],raw[j]
            if value is None:
                if epoch is not None:raise Refused('physical:timestamp-null-disagrees')
            elif (type(epoch) is not str or not re.fullmatch(r'-?\d+',epoch)
                  or micros(value)!=int(epoch)):
                raise Refused('physical:timestamp-epoch-disagrees')
        rows.append(tuple(raw[:8]))
    return rows

SOURCE_PROJECTION_SCHEMA=PROJECTION_SCHEMA[:4]+(('ts_us','BIGINT','LONG',4),)


def source_projection_sql(source):
    return "SELECT id,v,p,date_format(ts,'yyyy-MM-dd HH:mm:ss.SSSSSS') AS ts,unix_micros(ts) AS ts_us FROM "+source


def assert_source(result,expected):
    check_schema(result,SOURCE_PROJECTION_SCHEMA)
    rows=[]
    for row in result['rows']:
        if (len(row)!=5 or type(row[4]) is not str or not re.fullmatch(r'-?\d+',row[4])
                or micros(row[3])!=int(row[4])):raise Refused('physical:source-lossless-projection')
        rows.append(tuple(row[:4]))
    if Counter(rows)!=Counter(map(tuple,expected)):raise Refused('physical:complete-source-multiset')
