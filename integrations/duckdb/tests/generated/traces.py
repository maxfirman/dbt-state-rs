"""Versioned symbolic operations and conservative prerequisite-preserving shrink."""
import random

VERSION = 1


class Trace:
    def __init__(self):
        self.operations = []
        self.previous = None

    def add(self, kind, **arguments):
        identifier = 'o' + str(len(self.operations))
        # Operations within an episode form a causal chain. Reset starts an
        # independent episode: it preserves user rows but invalidates history.
        needs = [] if kind == 'reset' or self.previous is None else [self.previous]
        self.operations.append(dict(id=identifier, op=kind, needs=needs, **arguments))
        self.previous = identifier
        return identifier

    def run(self, env='prod', k=1, expect=None, clones=False, stale=False):
        submit = self.add('submit', env=env, k=k, expect=expect,
                          clones=clones, stale=stale,
                          policy='recorded-source-result-permitted' if stale else 'current-result')
        execution = self.add('copy' if expect == 'copy' else 'execute', submit=submit)
        self.add('confirm', execute=execution)
        return submit


def generate(seed, steps):
    rng = random.Random(seed)
    t = Trace()
    t.add('reset')
    s = t.add('submit', env='prod', k=1, expect='build', clones=True, stale=False)
    e = t.add('execute', submit=s)
    t.add('restart')
    t.add('confirm', execute=e)
    t.add('submit', env='prod', k=1, expect='skip', clones=True, stale=False)
    t.run('dev', 1, 'copy', clones=True)
    t.add('submit', env='dev', k=1, expect='skip', clones=True, stale=False)
    t.add('mutate_input', revision=1)
    t.add('drop', env='dev')
    t.run('dev', 1, 'copy', clones=True, stale=True)
    t.add('submit', env='dev', k=1, expect='skip', clones=True, stale=False,
          allowed_recorded_result=True, policy='confirmed-stale-copy-result-permitted')
    e = t.add('execute', env='prod', k=2)
    t.add('record', execute=e)
    t.add('submit', env='prod', k=2, expect='skip', clones=False, stale=False)
    t.add('mutate_target', env='prod')
    t.run('prod', 2, 'build')
    t.add('mutate_schema', env='prod')
    t.run('prod', 2, 'build')
    t.add('drop', env='prod')
    t.run('prod', 2, 'build')
    t.add('reset')
    t.run('prod', 2, 'build')
    # The fixed spine establishes positives and all operation kinds. Each
    # random episode is independently shrinkable and exercises interactions.
    for episode in range(steps):
        t.add('reset')
        env, k = rng.choice(['prod', 'dev']), rng.choice([1, 2, 7])
        t.run(env, k, 'build')
        action = rng.choice(['logic', 'input', 'target', 'schema', 'drop', 'record', 'restart'])
        if action == 'logic':
            k += 3
        elif action == 'input':
            t.add('mutate_input', revision=2 + episode * 1000 + rng.randint(0, 999))
        elif action == 'target':
            t.add('mutate_target', env=env)
        elif action == 'schema':
            t.add('mutate_schema', env=env)
        elif action == 'drop':
            t.add('drop', env=env)
        elif action == 'record':
            e = t.add('execute', env=env, k=k)
            t.add('record', execute=e)
        else:
            t.add('restart')
        t.run(env, k, 'skip' if action in ['record', 'restart'] else 'build')
        t.add('submit', env=env, k=k, expect='skip', clones=False, stale=False)
    return dict(version=VERSION, seed=seed, random_episodes=steps,
                workload='integer-add-v1; duplicates and NULL; rows-schema-v1',
                operations=t.operations)


def without(operations, removed):
    """Delete a chunk and all descendants; never orphan a pending/copy source."""
    retained = []
    present = set()
    for op in operations:
        if op['id'] in removed or not set(op['needs']) <= present:
            continue
        retained.append(op)
        present.add(op['id'])
    return retained


def validate(trace):
    if trace['version'] != VERSION:
        raise ValueError('unsupported trace version')
    present = {}
    ancestors = {}
    for op in trace['operations']:
        if op['id'] in present or not set(op['needs']) <= present.keys():
            raise ValueError('invalid prerequisites: ' + op['id'])
        ancestors[op['id']] = set(op['needs'])
        for dependency in op['needs']:
            ancestors[op['id']].update(ancestors[dependency])
        for field, kinds in [('submit', {'submit'}), ('execute', {'execute', 'copy'})]:
            if field in op and (op[field] not in present or present[op[field]] not in kinds):
                raise ValueError('invalid symbolic reference: ' + op['id'])
            if field in op and op[field] not in ancestors[op['id']]:
                raise ValueError('undeclared prerequisite: ' + op['id'])
        present[op['id']] = op['op']


def minimize(trace, reproduces):
    """Bounded ddmin; reproduces always starts a fresh file + service scope."""
    current = dict(trace)
    width = max(1, len(current['operations']) // 2)
    attempts = 0
    while width and attempts < 40:
        changed = False
        for start in range(0, len(current['operations']), width):
            removed = {op['id'] for op in current['operations'][start:start + width]}
            candidate = dict(current, operations=without(current['operations'], removed))
            if not candidate['operations']:
                continue
            validate(candidate)
            attempts += 1
            if reproduces(candidate):
                current = candidate
                changed = True
                break
            if attempts >= 40:
                break
        if not changed:
            width //= 2
    return current
