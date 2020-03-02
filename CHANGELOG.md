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
