"""Independent immutable BUILD snapshots for bounded Databricks policy campaigns.

These helpers do not choose a State decision or reproduce freshness policy.
The capture supplies the decision; declared operation-derived rows supply the
fresh reference. A SKIP retains the saved BUILD multiset, including duplicates.
"""
from collections import Counter
from dataclasses import dataclass

from databricks_freshness import assert_rows
from databricks_survey import Refused


@dataclass(frozen=True, init=False)
class PolicySnapshot:
    label: str
    rows: tuple
    upstream_epochs: tuple
    build_epoch: int

    def __init__(self, label, rows, upstream_epochs, build_epoch):
        epochs=tuple(upstream_epochs.items()) if isinstance(upstream_epochs,dict) else tuple(upstream_epochs)
        if (not epochs or len({name for name,_ in epochs}) != len(epochs)
                or any(not isinstance(name,str) or type(epoch) is not int for name,epoch in epochs)
                or type(build_epoch) is not int):
            raise Refused('snapshot:complete-unique-known-epochs-required')
        object.__setattr__(self,'label',label)
        object.__setattr__(self,'rows',tuple(tuple(row) for row in rows))
        object.__setattr__(self,'upstream_epochs',epochs)
        object.__setattr__(self,'build_epoch',build_epoch)

    def artifact(self):
        return {'label':self.label,'rows':self.rows,'upstream_epochs':dict(self.upstream_epochs),
                'build_epoch':self.build_epoch,'origin':'declared-operation-reference-at-BUILD'}


def assert_policy_result(actual, properties, snapshot, fresh_rows, decision):
    if decision not in ('skip_execution','ready_to_execute'):
        raise Refused('policy:unhandled-decision')
    expected=snapshot.rows if decision=='skip_execution' else fresh_rows
    expected_cells=[[None if cell is None else str(cell) for cell in row] for row in expected]
    assert_rows(actual,expected_cells)
    props={row[0]:row[1] for row in properties['rows']}
    if props.get('Type')!='MANAGED' or props.get('Provider','').lower()!='delta':
        raise Refused('physical:properties')
    return {'complete_typed_multiset_schema_properties':True,
            'reference':'saved-BUILD' if decision=='skip_execution' else 'fresh-transformation',
            'snapshot_label':snapshot.label,
            'different_fresh_reference':Counter(snapshot.rows)!=Counter(map(tuple,fresh_rows))}
