from unittest import TestCase, mock
from unittest.mock import patch

from airflow.hooks.subprocess import SubprocessHook, SubprocessResult

from airflow_dbt.hooks.cli import DbtCliHook
from airflow_dbt.hooks.google import DbtCloudBuildHook


class TestDbtCliHook(TestCase):
    @mock.patch.object(SubprocessHook, 'run_command')
    def test_sub_commands(self, mock_run_command):
        mock_run_command.return_value = SubprocessResult(
            exit_code=0, output='all good')
        hook = DbtCliHook()
        hook.run_dbt(['dbt', 'docs', 'generate'])
        mock_run_command.assert_called_once_with(
            command=['dbt', 'docs', 'generate'],
            env={}
        )

    def test_vars(self):
        hook = DbtCliHook()
        generated_command = hook.generate_dbt_cli_command(
            dbt_bin='dbt',
            command='run',
            vars={"foo": "bar", "baz": "true"}
        )

        assert generated_command == ['dbt', 'run', '--vars',
            '{"foo": "bar", "baz": "true"}']


class TestDbtCloudBuildHook(TestCase):
    @patch('airflow_dbt.hooks.google.CloudBuildHook')
    def test_create_build(self, MockCloudBuildHook):
        mock_create_build = MockCloudBuildHook().create_build
        hook = DbtCloudBuildHook(
            project_id='test_project_id',
            gcs_staging_location='gs://hello/file.tar.gz',
            dbt_version='0.10.10',
            env={'TEST_ENV_VAR': 'test'},
            service_account='robot@mail.com'
        )
        hook.run_dbt(['docs', 'generate'])

        expected_body = {
            'steps': [{
                'name': f'fishtownanalytics/dbt:0.10.10',
                'args': ['docs', 'generate'],
                'env': ['TEST_ENV_VAR=test']
            }],
            'source': {
                'storageSource': {
                    'bucket': 'hello',
                    'object': 'file.tar.gz',
                }
            },
            'serviceAccount': 'robot@mail.com'
        }

        mock_create_build.assert_called_once_with(
            body=expected_body,
            project_id='test_project_id'
        )