# airflow-dbt

This is a collection of [Airflow](https://airflow.apache.org/) operators to provide easy integration with [dbt](https://www.getdbt.com).

```py
default_args = {
  dbt_dir = '/srv/app/dbt'
}

with DAG(dag_id='dbt', default_args=default_args, schedule_interval='@daily') as dag:

  dbt_run = DbtRunOperator(
    task_id='dbt_run',
  )

  dbt_test = DbtTestOperator(
    task_id='dbt_test',
    retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
  )

  dbt_run >> dbt_test
```

## Installation

Install from PyPI:

```sh
pip install airflow-dbt
```

It will also need access to the `dbt` CLI, which should either be on your `PATH` or can be set with the `dbt_bin` argument in each operator.

## Usage

There are three operators currently implemented:

* `DbtRunOperator`
  * Calls [`dbt run`](https://docs.getdbt.com/docs/run)
* `DbtTestOperator`
  * Calls [`dbt test`](https://docs.getdbt.com/docs/test)
* `DbtDepsOperator`
  * Calls [`dbt deps`](https://docs.getdbt.com/docs/deps)

Each of the above operators accept the following arguments:

* `profiles_dir`
  * If set, passed as the `--profiles-dir` argument to the `dbt` command
* `target`
  * If set, passed as the `--target` argument to the `dbt` command
* `dir`
  * The directory to run the `dbt` command in
* `vars`
  * If set, passed as the `--vars` argument to the `dbt` command. Should be set as a Python dictionary, as will be passed to the `dbt` command as YAML
* `models`
  * If set, passed as the `--models` argument to the `dbt` command
* `exclude`
  * If set, passed as the `--exclude` argument to the `dbt` command
* `dbt_bin`
  * The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
* `verbose`
  * The operator will log verbosely to the Airflow logs

Typically you will want to use the `DbtRunOperator`, followed by the `DbtTestOperator`, as shown earlier.

You can also use the hook directly. Typically this can be used for when you need to combine the `dbt` command with another task in the same operators, for example running `dbt docs` and uploading the docs to somewhere they can be served from.

## License & Contributing

* This is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
* Bug reports and pull requests are welcome on GitHub at https://github.com/gocardless/airflow-dbt.

GoCardless ♥ open source. If you do too, come [join us](https://gocardless.com/about/jobs).
