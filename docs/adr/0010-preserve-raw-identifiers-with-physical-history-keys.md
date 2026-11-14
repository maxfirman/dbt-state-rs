# Preserve raw identifiers alongside physical history keys

Hosted Databricks captures demonstrate that plain three-part relation names,
independently backtick-quoted components and ASCII case variants address one
physical history. Store a generated physical history key while preserving the
original target spelling and versioned fingerprints. This lets pre-upgrade
confirmed rows and pending rows confirmed after upgrade participate in the same
latest-row selection; canonicalizing only new writes would overlook older
aliases and can reuse stale history even when epochs are equal.

The projection uses the same restricted grammar and ASCII folding as Warehouse
metadata ingress, with exact fallback for unsupported names and dialect forms.
Organization, warehouse family, namespace and distinct relation components remain
part of the scope. Raw spelling remains available for deferral and other client
output. The additive column costs storage and an index, but avoids rewriting
historical evidence or broadening unsupported identifier equivalences.
