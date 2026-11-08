"""Successful warehouse completion is required for every published outcome."""


def completed_result(cache, node, result):
    table = cache._node_resolution.node_to_table(node)
    extension = cache._adapter_ext
    if str(result.status) == "success":
        extension.complete_write(table)
    else:
        # A returned error can follow a committed or partial write. Neither the
        # old extant target nor a fresh scan certifies successful execution.
        extension._uncertain_writes.add(extension._sql(extension._to_fqn(table)))
        extension.clear_cache([table])
        from .recovery import uncertain

        uncertain()


def may_confirm(cache, extension_type, result, original):
    if cache and isinstance(cache._adapter_ext, extension_type):
        return str(result.status) == "success" and original(result)
    return original(result)
