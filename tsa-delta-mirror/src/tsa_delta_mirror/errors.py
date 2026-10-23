"""Exception types. Keep them few and explicit."""


class MirrorError(Exception):
    """A failure that should stop the current table and mark the run unsuccessful."""


class ConfigError(MirrorError):
    """Invalid configuration."""


class CdfUnavailableError(MirrorError):
    """Change Data Feed was requested but the share cannot serve it for the requested range."""


class SchemaMismatchError(MirrorError):
    """Incoming data cannot be aligned to the target table schema without a type change."""


class LockHeldError(MirrorError):
    """Another run holds the per-table run lock."""
