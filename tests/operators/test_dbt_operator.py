import datetime
from unittest import mock
from unittest.mock import MagicMock

import pytest
from airflow import DAG, configuration

from airflow_dbt import DbtCliHook
from airflow_dbt.operators.dbt_operator import (
    DbtBuildOperator, DbtCleanOperator, DbtCompileOperator, DbtDebugOperator, DbtDepsOperator, DbtDocsGenerateOperator,
    DbtInitOperator, DbtListOperator, DbtParseOperator, DbtRunOperator,
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtSourceOperator, DbtTestOperator,
)


@pytest.fixture
def spy_cli_run(mocker) -> MagicMock:
    yield mocker.patch("airflow_dbt.hooks.dbt_hook.DbtCliHook.run_cli")


@pytest.fixture
def mock_dag() -> MagicMock:
    configuration.conf.load_test_config()
    args = {
        'owner': 'airflow',
        'start_date': datetime.datetime(2020, 2, 27)
    }
    yield DAG('test_dag_id', default_args=args)


@pytest.mark.parametrize(
    ['operator', 'expected_command'],
    [
        (DbtRunOperator, ['run']),
        (DbtTestOperator, ['test']),
        (DbtSnapshotOperator, ['snapshot']),
        (DbtDocsGenerateOperator, ['docs', 'generate']),
        (DbtSeedOperator, ['seed']),
        (DbtDepsOperator, ['deps']),
        (DbtBuildOperator, ['build']),
        (DbtCleanOperator, ['clean']),
        (DbtCompileOperator, ['compile']),
        (DbtDebugOperator, ['debug']),
        (DbtInitOperator, ['init']),
        (DbtListOperator, ['list']),
        (DbtParseOperator, ['parse']),
        (DbtListOperator, ['list']),
        (DbtSourceOperator, ['source']),
    ]
)
@mock.patch.object(DbtCliHook, 'run_cli')
def test_operators_commands(
    spy_cli_run,
    operator: DbtRunOperator,
    expected_command: [str],
    mock_dag,
):
    """Every operator passess down to the execution the correct dbt command"""
    task_id =  'test_dbt_' + '_'.join(expected_command)
    operator = operator(task_id=task_id, dag=mock_dag)
    operator.execute(None)
    spy_cli_run.assert_called_once_with(*expected_command)
