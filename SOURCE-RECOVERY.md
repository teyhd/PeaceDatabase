# Source recovery

This snapshot preserves a distinct local .NET code variant. Dependency binaries, executable builds, caches, datasets, runtime configuration and media are excluded. Restore dependencies from the project/package manifests. No original worktrees or production services were modified.

For PlatonAlarm, operator-specific file paths use the environment variables in `.env.example`. Compiled icon data is excluded and the default application icon is used. Builds write inside the isolated source copy.
