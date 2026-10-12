#!/usr/bin/env bash
# dbt State `state:*` selector capture driver.
#
# Drives GetStateSelection through the recording proxy against the REAL hosted
# service, using jaffle-shop. The hosted comparison state is each node's own last
# execution in the `defer_to_target` env; we build the baseline in `prod_demo`
# and query the selector from `dev_clone` (whose `defer_to_target: prod_demo`),
# so the selector's `target` == `prod_demo` and lines up with the baseline.
#
# `dbt compile --select state:<criterion>` triggers GetStateSelection without
# materializing anything in the warehouse (compile is in the run-cache command
# set; `dbt ls` is NOT and does not engage the service).
#
# Subcommands:
#   baseline            Build the full project in prod_demo (records node state).
#   query CRIT          Run state:CRIT from dev_clone; print the captured request/response.
#   mutate CRIT EDIT    git-restore, apply EDIT (python, argv1=proj root), run state:CRIT.
#   all-clean           Query every criterion with NO mutation (partition baseline).
#
# Only ever mutates CDO_DEMO_DB. git-restores jaffle-shop after each mutate.
set -uo pipefail
PROJ=~/projects/jaffle-shop
GOLDEN_DIR="${GOLDEN_DIR:-/tmp/sel_golden}"
PORT="${PORT:-50099}"
CRITERIA_ALL="new old modified unmodified modified.body modified.configs modified.relation modified.persisted_descriptions modified.macros modified.contract"

cd "$PROJ"
export DBT_ENGINE_MANAGE_STATE=true RUN_CACHE_API_URL="127.0.0.1:$PORT" RUN_CACHE_API_SECURE=false

gf() { ls -t "$GOLDEN_DIR"/golden_*.jsonl 2>/dev/null | head -1; }

# Print the GetStateSelection entries appended since line $1 of the golden file.
report_since() {
  local before="$1"
  python3 - "$(gf)" "$before" <<'PY'
import json,sys,collections
gf,before=sys.argv[1],int(sys.argv[2])
rows=[json.loads(l) for l in open(gf) if l.strip()][before:]
for r in rows:
    if r['method']!='GetStateSelection': continue
    req,resp=r['request'],r['response']
    sel=resp.get('node_unique_ids',[])
    tracked=[s for s in sel if s.startswith(('model.','test.','seed.','unit_test.','snapshot.'))]
    print("SEL "+json.dumps({
      "target":req['target'],"criteria":req['selector_criteria'],
      "sent":len(req['nodes']),"selected":len(sel),
      "kinds":dict(collections.Counter(s.split('.')[0] for s in sel)),
      "tracked_selected":tracked,
    }))
PY
}

line_count() { local f; f="$(gf)"; [ -n "$f" ] && wc -l < "$f" || echo 0; }

case "${1:-}" in
  baseline)
    git restore . 2>/dev/null || true
    echo "### building baseline in prod_demo ###"
    dbt build --profile snowflake --target prod_demo --skip-semantic-manifest-validation \
      --vars 'load_source_data: true' > /tmp/sel_baseline.txt 2>&1
    echo "exit=$?"; tail -3 /tmp/sel_baseline.txt
    ;;
  query)
    CRIT="$2"
    git restore . 2>/dev/null || true
    B=$(line_count)
    dbt compile --profile snowflake --target dev_clone --select "state:$CRIT" \
      --skip-semantic-manifest-validation > "/tmp/sel_q_${CRIT//./_}.txt" 2>&1 || true
    echo "### query state:$CRIT ###"
    report_since "$B"
    ;;
  mutate)
    CRIT="$2"; EDIT="$3"
    git restore . 2>/dev/null || true
    python3 "$EDIT" "$PROJ" || { echo "EDIT FAILED"; exit 2; }
    B=$(line_count)
    dbt compile --profile snowflake --target dev_clone --select "state:$CRIT" \
      --skip-semantic-manifest-validation > "/tmp/sel_m_${CRIT//./_}.txt" 2>&1 || true
    echo "### mutate ($EDIT) + query state:$CRIT ###"
    report_since "$B"
    git restore . 2>/dev/null || true
    ;;
  all-clean)
    git restore . 2>/dev/null || true
    for c in $CRITERIA_ALL; do
      B=$(line_count)
      dbt compile --profile snowflake --target dev_clone --select "state:$c" \
        --skip-semantic-manifest-validation > "/tmp/sel_clean_${c//./_}.txt" 2>&1 || true
      echo "### clean state:$c ###"
      report_since "$B"
    done
    ;;
  *)
    echo "usage: $0 {baseline|query CRIT|mutate CRIT EDIT.py|all-clean}"; exit 1;;
esac
