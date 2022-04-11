# v0.0.1

Initial release with the following operators:

* `DbtRunOperator`
  * Calls [`dbt run`](https://docs.getdbt.com/docs/run)
* `DbtTestOperator`
  * Calls [`dbt test`](https://docs.getdbt.com/docs/test)
* `DbtDepsOperator`
  * Calls [`dbt deps`](https://docs.getdbt.com/docs/deps)

# v0.1.0

Removed the operator `DbtDepsOperator`. Dependencies defined in dbt's `packages.yml` file should be collected before deployment, otherwise we assume workers use a shared filesystem.

* `DbtDepsOperator`
  * Calls [`dbt deps`](https://docs.getdbt.com/docs/deps)


# v0.1.1

Makes `vars` a jinja templated field. See [here](https://airflow.apache.org/docs/stable/concepts.html#jinja-templating) for more information.

# v0.1.2

Fix verbose logging of command to include `--full_refresh`.

# v0.2.0

Add the `DbtSnapshotOperator`.

# v0.3.0

Add the `DbtSeedOperator`.

Support `--select` for the `DbtSnapshotOperator`.

# v0.4.0

Add the `DbtDocsGenerateOperator` and `DbtDepsOperator`.