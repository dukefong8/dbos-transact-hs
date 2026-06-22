# Use a three-level module structure for source and test code

DBOS Haskell source and tests will use the shape `DBOS.<FunctionalDomain>.<InternalModule>`, with each `DBOS.<FunctionalDomain>` module exported as the public entry point for that domain. Internal modules should hold database rows, parsers, query fragments, checkpoint internals, and test helpers, while the exported domain module owns the stable API surface; this keeps compatibility-driven implementation details local without flattening every DBOS concept into one namespace.
