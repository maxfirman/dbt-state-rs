import sys
root = sys.argv[1]
p = root + "/models/staging/stg_customers.sql"
s = open(p).read()
anchor = "        name as customer_name"
repl = "        name as customer_name,\n        upper(name) as customer_name_upper_c1"
assert anchor in s, "stg_customers anchor not found"
open(p, "w").write(s.replace(anchor, repl, 1))
print("changed stg_customers view logic")
