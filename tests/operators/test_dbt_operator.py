import datetime
from unittest import TestCase, mock
from unittest.mock import patch

from airflow import DAG, configuration

from airflow_dbt.hooks.dbt_google_hook import DbtCloudBuildHook
from airflow_dbt.hooks.dbt_hook import DbtCliHook
from airflow_dbt.operators.dbt_operator import (
    DbtDepsOperator, DbtRunOperator, DbtSeedOperator, DbtSnapshotOperator,
    DbtTestOperator,
)


class TestDbtCliOperator(TestCase):
    def setUp(self):
        configuration.conf.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2020, 2, 27)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    @mock.patch.object(DbtCliHook, 'run_dbt')
    def test_dbt_run(self, mock_run_dbt):
        operator = DbtRunOperator(
            task_id='run',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(['dbt', 'run'])

    @mock.patch.object(DbtCliHook, 'run_dbt')
    def test_dbt_test(self, mock_run_dbt):
        operator = DbtTestOperator(
            task_id='test',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(['dbt', 'test'])

    @mock.patch.object(DbtCliHook, 'run_dbt')
    def test_dbt_snapshot(self, mock_run_dbt):
        operator = DbtSnapshotOperator(
            task_id='snapshot',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(['dbt', 'snapshot'])

    @mock.patch.object(DbtCliHook, 'run_dbt')
    def test_dbt_seed(self, mock_run_dbt):
        operator = DbtSeedOperator(
            task_id='seed',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(['dbt', 'seed'])

    @mock.patch.object(DbtCliHook, 'run_dbt')
    def test_dbt_deps(self, mock_run_dbt):
        operator = DbtDepsOperator(
            task_id='deps',
            dag=self.dag
        )
        operator.execute(None)
        mock_run_dbt.assert_called_once_with(['dbt', 'deps'])


class TestDbtRunWithCloudBuild(TestCase):
    def setUp(self):
        configuration.conf.load_test_config()
        args = {
            'owner': 'airflow',
            'start_date': datetime.datetime(2020, 2, 27)
        }
        self.dag = DAG('test_dag_id', default_args=args)

    @patch('airflow_dbt.hooks.dbt_google_hook.CloudBuildHook')
    @patch('airflow_dbt.hooks.dbt_google_hook.GCSHook')
    def test_dbt_deps(self, MockLocalCloudBuildHook, MockGCSHook):
        operator = DbtRunOperator(
            task_id='test_dbt_run_on_cloud_build',
            dbt_hook=DbtCloudBuildHook(
                project_id='my-project-id',
                gcp_conn_id='my_conn_id',
                gcs_staging_location='gs://my-bucket/certain-folder/'
            ),
            dag=self.dag
        )
        operator.execute(None)
        MockLocalCloudBuildHook.assert_called_once_with(gcp_conn_id='my_conn_id')
        MockGCSHook.assert_called_once_with(gcp_conn_id='my_conn_id')
        MockGCSHook().upload.assert_called_once()


{
    'steps': [{
        'name': 'fishtownanalytics/dbt:0.21.0', 'entrypoint': '/bin/sh',
        'args': ['-c', 'dbt', 'run'], 'env': []
    }],
    'source': {
        'storageSource': {
            'bucket': 'my-bucket',
            'object':
                'certain-folder/dbt_staging_c5013d9965b54386bcf84ab76e42c848'
                '.tar.gz'
        }
    }
}
