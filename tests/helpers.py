"""Home Assistant test-client compatibility helpers (not integration code)."""


def get_device_by_identifier(registry, identifier, config_entry_id):
    """Look up this entry's device using the scoped API when available."""
    if (
        lookup := getattr(registry, "async_get_device_by_identifier", None)
    ) is not None:
        return lookup(identifier, config_entry_id)
    # Older Core releases may lack the scoped API. Keep the ownership check
    # even though our test identifiers already include the entry ID.
    device = registry.async_get_device(identifiers={identifier})
    if device is not None and config_entry_id in device.config_entries:
        return device
    return None
