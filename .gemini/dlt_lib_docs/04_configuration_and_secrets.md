# Configuration Management

## 1. Secrets vs Config
* **secrets.toml:** Sensitive data (passwords, tokens). **Never commit to Git.**
* **config.toml:** Logic configuration (timeouts, behavior flags). Safe for Git.

## 2. Schema Contracts
To prevent bad data from breaking the pipeline, you can enforce contracts in `config.toml`.

```toml
[resources]
schema_contract = "evolve" 
````

  * **evolve:** (Default) Allow new columns to be added to the destination.
  * **freeze:** Raise an exception if the source schema changes.

## 3\. Resolution Order

`dlt` looks for values in:

1.  Environment Variables (e.g., `SOURCES__PG_REPLICATION__CREDENTIALS`)
2.  `./.dlt/secrets.toml` (Local project)
3.  `~/.dlt/secrets.toml` (Global user)