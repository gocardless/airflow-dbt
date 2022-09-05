import datetime
from unittest import TestCase, mock
from airflow import DAG, configuration
from airflow_dbt.hooks.dbt_hook import DbtCliHook
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtDepsOperator,
    DbtCleanOperator,
)


class TestDbtOperator(TestCase):
    def setUp(self):
        configuration.conf.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2020, 2, 27)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_run(self, mock_run_cli):
        operator = DbtRunOperator(
            task_id='run',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'run', '--project-dir', '.'])

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_test(self, mock_run_cli):
        operator = DbtTestOperator(
            task_id='test',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'test', '--project-dir', '.'])

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_snapshot(self, mock_run_cli):
        operator = DbtSnapshotOperator(
            task_id='snapshot',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'snapshot', '--project-dir', '.'])

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_seed(self, mock_run_cli):
        operator = DbtSeedOperator(
            task_id='seed',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'seed', '--project-dir', '.'])

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_deps(self, mock_run_cli):
        operator = DbtDepsOperator(
            task_id='deps',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'deps', '--project-dir', '.'])

    @mock.patch.object(DbtCliHook, 'run_cli')
    def test_dbt_clean(self, mock_run_cli):
        operator = DbtCleanOperator(
            task_id='clean',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_cli.assert_called_once_with(['dbt', 'clean', '--project-dir', '.'])
