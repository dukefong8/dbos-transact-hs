# Parse database rows into domain types at the boundary

DBOS Haskell will treat Postgres rows and serialized text values as external input and parse them into refined domain types before workflow execution logic acts on them. Raw row types may mirror Python compatibility exactly, including nullable columns and flexible output/error/child workflow fields, but domain code should consume parsed ADTs such as workflow outcomes and checkpoint bodies so invalid combinations are rejected once at the boundary instead of repeatedly validated throughout execution.
