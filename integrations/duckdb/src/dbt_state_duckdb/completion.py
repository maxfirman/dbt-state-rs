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


def install_result_guard(extension_type):
    from dbt_state.run_cache import RunCache

    original = RunCache.on_run_result

    def on_result(cache, node, result):
        # Data-test aggregate outcomes use their separate complete-result route;
        # an ordinary failed materialization is never a successful Values/SQL row.
        if (
            isinstance(cache._adapter_ext, extension_type)
            and str(result.status) != "success"
        ):
            return
        return original(cache, node, result)

    RunCache.on_run_result = on_result
