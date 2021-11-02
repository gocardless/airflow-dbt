import datetime
from typing import Union
from unittest.mock import MagicMock, patch

import pytest
from airflow import DAG, configuration
from pytest import fixture, mark

from airflow_dbt.hooks.cli import DbtCliHook
from airflow_dbt.hooks.google import DbtCloudBuildHook
from airflow_dbt.operators.dbt_operator import (
    DbtBaseOperator,
    DbtCleanOperator,
    DbtDepsOperator,
    DbtDocsGenerateOperator,
    DbtRunOperator,
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtTestOperator,
)
from airflow_dbt.operators.google import DbtCloudBuildOperator


@fixture
def airflow_dag():
    """Instantiates an Airflow DAG to be used as a test fixture"""
    configuration.conf.load_test_config()
    args = {
        'owner': 'airflow',
        'start_date': datetime.datetime(2020, 2, 27)
    }
    yield DAG('test_dag_id', default_args=args)


@mark.parametrize(
    ['Operator', 'expected_command'], [
        (DbtBaseOperator, ValueError()),
        (DbtDepsOperator, ['dbt', 'deps']),
        (DbtRunOperator, ['dbt', 'run']),
        (DbtSeedOperator, ['dbt', 'seed']),
        (DbtDocsGenerateOperator, ['dbt', 'docs generate']),
        (DbtSnapshotOperator, ['dbt', 'snapshot']),
        (DbtCleanOperator, ['dbt', 'clean']),
        (DbtTestOperator, ['dbt', 'test']),
    ]
)
@patch.object(DbtCliHook, 'run_dbt')
def test_basic_dbt_operators(
    mock_run_dbt: MagicMock,
    Operator: DbtBaseOperator,
    expected_command: Union[list[str], Exception],
    airflow_dag: DAG,
):
    """
    Test that all the basic Dbt{Command}Operators instantiate the right
    default dbt command. And that the basic DbtBaseOperator raises a value
    Error since there's no base command defined to be executed
    command
    """
    # noinspection PyCallingNonCallable
    operator = Operator(
        task_id=f'{Operator.__name__}',
        dag=airflow_dag
    )
    if isinstance(expected_command, Exception):
        with pytest.raises(expected_command.__class__):
            operator.execute(None)
    else:
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(expected_command)


def test_dbt_warns_about_dir_param(airflow_dag: DAG):
    """
    Test that the DbtBaseOperator warns about the use of the dir parameter
    """
    with pytest.warns(PendingDeprecationWarning):
        DbtBaseOperator(
            task_id='test_task_id',
            dag=airflow_dag,
            dir='/tmp/dbt'
        )


@patch.object(DbtCloudBuildHook, '__init__', return_value=None)
def test_cloud_build_operator_instantiates_hook(
    cloud_build_hook_constructor: MagicMock,
    airflow_dag: DAG
):
    hook = DbtCloudBuildOperator(
        task_id='test_cloud_build',
        gcs_staging_location='gs://my_bucket/dbt_proj.tar.gz',
        env={'CONFIG_VAR': 'HELLO'},
        config={'project_dir': 'not used'},
        project_id='my_project',
        gcp_conn_id='test_gcp_conn',
        dbt_version='0.19.2',
        service_account='dbt-sa@google.com',
        dag=airflow_dag
    )
    hook.instantiate_hook()

    cloud_build_hook_constructor.assert_called_once_with(
        env={'CONFIG_VAR': 'HELLO'},
        gcs_staging_location='gs://my_bucket/dbt_proj.tar.gz',
        project_id='my_project',
        gcp_conn_id='test_gcp_conn',
        dbt_version='0.19.2',
        service_account='dbt-sa@google.com'
    )
