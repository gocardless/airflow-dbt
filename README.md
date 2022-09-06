# airflow-dbt

This is a collection of [Airflow](https://airflow.apache.org/) operators to provide easy integration with [dbt](https://www.getdbt.com).

```py
from airflow import DAG
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtCleanOperator,
)
from airflow.utils.dates import days_ago

default_args = {
  'dir': '/srv/app/dbt',
  'start_date': days_ago(0)
}

with DAG(dag_id='dbt', default_args=default_args, schedule_interval='@daily') as dag:

  dbt_seed = DbtSeedOperator(
    task_id='dbt_seed',
  )

  dbt_snapshot = DbtSnapshotOperator(
    task_id='dbt_snapshot',
  )

  dbt_run = DbtRunOperator(
    task_id='dbt_run',
  )

  dbt_test = DbtTestOperator(
    task_id='dbt_test',
    retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
  )

  dbt_clean = DbtCleanOperator(
    task_id='dbt_clean',
  )

  dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test >> dbt_clean
```

## Installation

Install from PyPI:

```sh
pip install airflow-dbt
```

It will also need access to the `dbt` CLI, which should either be on your `PATH` or can be set with the `dbt_bin` argument in each operator.

## Usage

There are five operators currently implemented:

* `DbtDocsGenerateOperator`
  * Calls [`dbt docs generate`](https://docs.getdbt.com/reference/commands/cmd-docs)
* `DbtDepsOperator`
  * Calls [`dbt deps`](https://docs.getdbt.com/docs/deps)
* `DbtSeedOperator`
  * Calls [`dbt seed`](https://docs.getdbt.com/docs/seed)
* `DbtSnapshotOperator`
  * Calls [`dbt snapshot`](https://docs.getdbt.com/docs/snapshot)
* `DbtRunOperator`
  * Calls [`dbt run`](https://docs.getdbt.com/docs/run)
* `DbtTestOperator`
  * Calls [`dbt test`](https://docs.getdbt.com/docs/test)
* `DbtCleanOperator`
  * Calls [`dbt clean`](https://docs.getdbt.com/docs/clean)


Each of the above operators accept the following arguments:

* `env`
  * If set as a kwarg dict, passed the given environment variables as the arguments to the dbt task
* `profiles_dir`
  * If set, passed as the `--profiles-dir` argument to the `dbt` command
* `target`
  * If set, passed as the `--target` argument to the `dbt` command
* `dir`
  * The directory to run the `dbt` command in
* `full_refresh`
  * If set to `True`, passes `--full-refresh`
* `vars`
  * If set, passed as the `--vars` argument to the `dbt` command. Should be set as a Python dictionary, as will be passed to the `dbt` command as YAML
* `models`
  * If set, passed as the `--models` argument to the `dbt` command
* `exclude`
  * If set, passed as the `--exclude` argument to the `dbt` command
* `select`
  * If set, passed as the `--select` argument to the `dbt` command
* `selector`
  * If set, passed as the `--selector` argument to the `dbt` command
* `dbt_bin`
  * The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
* `verbose`
  * The operator will log verbosely to the Airflow logs
* `warn_error`
  * If set to `True`, passes `--warn-error` argument to `dbt` command and will treat warnings as errors

Typically you will want to use the `DbtRunOperator`, followed by the `DbtTestOperator`, as shown earlier.

You can also use the hook directly. Typically this can be used for when you need to combine the `dbt` command with another task in the same operators, for example running `dbt docs` and uploading the docs to somewhere they can be served from.

## Building Locally

To install from the repository:
First it's recommended to create a virtual environment:
```bash
python3 -m venv .venv

source .venv/bin/activate
```

Install using `pip`:
```bash
pip install .
```

## Testing

To run tests locally, first create a virtual environment (see [Building Locally](https://github.com/gocardless/airflow-dbt#building-locally) section)

Install dependencies:
```bash
pip install . pytest
```

Run the tests:
```bash
pytest tests/
```

## Code style
This project uses [flake8](https://flake8.pycqa.org/en/latest/).

To check your code, first create a virtual environment (see [Building Locally](https://github.com/gocardless/airflow-dbt#building-locally) section):
```bash
pip install flake8
flake8 airflow_dbt/ tests/ setup.py
```

## Package management

If you use dbt's package manager you should include all dependencies before deploying your dbt project.

For Docker users, packages specified in `packages.yml` should be included as part your docker image by calling `dbt deps` in your `Dockerfile`.

## Amazon Managed Workflows for Apache Airflow (MWAA)

If you use MWAA, you just need to update the `requirements.txt` file and add `airflow-dbt` and `dbt` to it.

Then you can have your dbt code inside a folder `{DBT_FOLDER}` in the dags folder on S3 and configure the dbt task like below:

```python
dbt_run = DbtRunOperator(
  task_id='dbt_run',
  dbt_bin='/usr/local/airflow/.local/bin/dbt',
  profiles_dir='/usr/local/airflow/dags/{DBT_FOLDER}/',
  dir='/usr/local/airflow/dags/{DBT_FOLDER}/'
)
```

## Templating and parsing environments variables

If you would like to run DBT using custom profile definition template with environment-specific variables, like for example profiles.yml using jinja:
```yaml
<profile_name>:
  outputs:
    <source>:
      database: "{{ env_var('DBT_ENV_SECRET_DATABASE') }}"
      password: "{{ env_var('DBT_ENV_SECRET_PASSWORD') }}"
      schema: "{{ env_var('DBT_ENV_SECRET_SCHEMA') }}"
      threads: "{{ env_var('DBT_THREADS') }}"
      type: <type>
      user: "{{ env_var('USER_NAME') }}_{{ env_var('ENV_NAME') }}"
  target: <source>
```

You can pass the environment variables via the `env` kwarg parameter:

```python
import os
...

dbt_run = DbtRunOperator(
  task_id='dbt_run',
  env={
    'DBT_ENV_SECRET_DATABASE': '<DATABASE>',
    'DBT_ENV_SECRET_PASSWORD': '<PASSWORD>',
    'DBT_ENV_SECRET_SCHEMA': '<SCHEMA>',
    'USER_NAME': '<USER_NAME>',
    'DBT_THREADS': os.getenv('<DBT_THREADS_ENV_VARIABLE_NAME>'),
    'ENV_NAME': os.getenv('ENV_NAME')
  }
)
```

## License & Contributing

* This is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
* Bug reports and pull requests are welcome on GitHub at https://github.com/gocardless/airflow-dbt.

GoCardless ♥ open source. If you do too, come [join us](https://gocardless.com/about/jobs).
