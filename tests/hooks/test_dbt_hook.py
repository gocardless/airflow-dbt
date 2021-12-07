from typing import Union
from unittest import TestCase, mock
from unittest.mock import patch

import pytest
from airflow import AirflowException
from airflow.hooks.subprocess import SubprocessHook, SubprocessResult
from pytest import mark

from airflow_dbt.dbt_command_config import DbtCommandConfig
from airflow_dbt.hooks.base import generate_dbt_cli_command
from airflow_dbt.hooks.cli import DbtCliHook
from airflow_dbt.hooks.google import DbtCloudBuildHook

cli_command_from_params_data = [
        [("dbt", "run", {}, ["dbt", "run"]), "regular dbt run"],
        # check it runs with empty params
        [("dbt", None, {}, ValueError()),  "it fails with no command"],
        [(None, "run", {}, ValueError()),  "it fails with no dbt_bin"],
        [("dbt", "test", {}, ["dbt", "test"]),  "test without params"],
        [
            ("dbt", "test", {'non_existing_param'}, TypeError()),
            "invalid param raises TypeError"
        ],
        # test invalid param
        [
            ("dbt", "test", {'--models': None}, ValueError()),
            "required --models value raises ValueError if not provided"
        ],
        # test mandatory value
        [
            ("dbt", "test", {'--models': 3}, ValueError()),
            "required --models value raises ValueError if not correct type"
        ],
        [
            ("/bin/dbt", "test", {}, ["/bin/dbt", "test"]),
            "dbt_bin other than the default gets passed through"
        ],
        [
            ("dbt", "run", {'full_refresh': False}, ValueError()),
            "flags param fails if contains False value"
        ],
        # test flags always positive
        [('/home/airflow/.local/bin/dbt', 'run', {
            'full_refresh': True,
            'profiles_dir': '/opt/airflow/dags/dbt_project',
            'project_dir': '/opt/airflow/dags/project_dir',
            'vars': {'execution_date': '2021-01-01'},
            'select': 'my_model',
        }, ['/home/airflow/.local/bin/dbt', 'run', '--full-refresh',
            '--profiles-dir', '/opt/airflow/dags/dbt_project',
            '--project-dir', '/opt/airflow/dags/project_dir',
            '--vars', '{"execution_date": "2021-01-01"}', '--select',
            'my_model']),
            "fully fledged dbt run with all types of params"
            ],
        # test all the params
        [
            ("dbt", "test", {'profiles_dir': '/path/profiles_folder'},
            ["dbt", "test", "--profiles-dir", "/path/profiles_folder"]),
            "test profiles_dir param"
        ],
        [
            ("dbt", "run", {'project_dir': '/path/dbt_project_dir'},
            ["dbt", "run", "--project-dir", "/path/dbt_project_dir"]),
            "test project_dir param"
        ],
        [
            ("dbt", "test", {'target': 'model_target'},
            ["dbt", "test", "--target", "model_target"]),
            "test target param"
        ],
        [
            ("dbt", "test", {'vars': {"hello": "world"}},
            ["dbt", "test", "--vars", '{"hello": "world"}']),
            "test vars param"
        ],
        [
            ("dbt", "run", {'models': 'my_model'},
            ["dbt", "run", "--models", "my_model"]),
            "test models param"
        ],
        [
            ("dbt", "run", {'exclude': 'my_model'},
            ["dbt", "run", "--exclude", "my_model"]),
            "test exclude param"
        ],

        # run specific params
        [
            ("dbt", "run", {'full_refresh': True},
            ["dbt", "run", "--full-refresh"]),
            "[dbt run] test full_refresh flag succeeds"
        ],
        [
            ("dbt", "run", {'full_refresh': 3}, TypeError()),
            "[dbt run] test full_refresh param fails if not bool but integer"
        ],
        [
            ("dbt", "run", {'full_refresh': 'hello'}, TypeError()),
            "[dbt run] test full_refresh project_dir fails if not bool but string"
        ],
        [
            ("dbt", "run", {'profile': 'test_profile'},
            ["dbt", "run", "--profile", "test_profile"]),
            "[dbt run] test profile param"
        ],

        # docs specific params
        [
            ("dbt", "docs", {'no_compile': True},
            ["dbt", "docs", "--no-compile"]),
            "test no_compile flag succeeds"
        ],
        # debug specific params
        [
            ("dbt", "debug", {'config_dir': '/path/to/config_dir'},
            ["dbt", "debug", "--config-dir", '/path/to/config_dir']),
            "[dbt debug] test config_dir param"
        ],

        # ls specific params
        [
            ("dbt", "ls", {'resource_type': '/path/to/config_dir'},
            ["dbt", "ls", "--resource-type", '/path/to/config_dir']),
            "[dbt ls] test resource_type param"
        ],
        [
            ("dbt", "ls", {'select': 'my_model'},
            ["dbt", "ls", "--select", "my_model"]),
            "[dbt ls] test select param"
        ],
        [
            ("dbt", "ls", {'exclude': 'my_model'},
            ["dbt", "ls", "--exclude", "my_model"]),
            "[dbt ls] test exclude param"
        ],
        [
            ("dbt", "ls", {'output': 'my_model'},
            ["dbt", "ls", "--output", "my_model"]),
            "[dbt ls] test output param"
        ],
        [
            ("dbt", "ls", {'output_keys': 'my_model'},
            ["dbt", "ls", "--output-keys", "my_model"]),
            "[dbt ls] test output_keys param"
        ],

        # rpc specific params
        [
            ("dbt", "rpc", {'host': 'http://my-host-url.com'},
            ["dbt", "rpc", "--host", 'http://my-host-url.com']),
            "[dbt rpc] test host param"
        ],
        [
            ("dbt", "rpc", {'port': '8080'}, TypeError()),
            "[dbt rpc] test port param fails if not integer"
        ],
        [
            ("dbt", "rpc", {'port': 8080}, ["dbt", "rpc", "--port", '8080']),
            "[dbt rpc] test port param"
        ],

        # run specific params
        [
            ("dbt", "run", {'fail_fast': True}, ["dbt", "run", "--fail-fast"]),
            "[dbt run] test fail_fast flag succeeds"
        ],

        # test specific params
        [
            ("dbt", "test", {'data': True}, ["dbt", "test", '--data']),
            "[dbt test] test data flag succeeds"
        ],
        [
            ("dbt", "test", {'schema': True}, ["dbt", "test", '--schema']),
            "[dbt test] test schema flag succeeds"
        ],
    ]


@mark.parametrize(
    ["dbt_bin", "command", "params", "expected_command"],
    [test_params[0] for test_params in cli_command_from_params_data],
    ids=[test_params[1] for test_params in cli_command_from_params_data]
)
def test_create_cli_command_from_params(
    dbt_bin: str,
    command: str,
    params: DbtCommandConfig,
    expected_command: Union[list[str], Exception]
):
    """
    Test that the function create_cli_command_from_params returns the
    correct
    command or raises the correct exception
    :type expected_command: object
    """
    if isinstance(expected_command, Exception):
        with pytest.raises(expected_command.__class__):
            generate_dbt_cli_command(dbt_bin, command, **params)
    else:
        assert generate_dbt_cli_command(dbt_bin, command, **params) \
               == expected_command


class TestDbtCliHook(TestCase):
    @mock.patch.object(
        SubprocessHook,
        'run_command',
        return_value=SubprocessResult(exit_code=0, output='all good')
    )
    def test_sub_commands(self, mock_run_command):
        """
        Test that sub commands are called with the right params
        """
        hook = DbtCliHook(env={'GOOGLE_APPLICATION_CREDENTIALS': 'my_creds'})
        hook.run_dbt(['dbt', 'docs', 'generate'])
        mock_run_command.assert_called_once_with(
            command=['dbt', 'docs', 'generate'],
            env={'GOOGLE_APPLICATION_CREDENTIALS': 'my_creds'}
        )

    @mock.patch.object(
        SubprocessHook,
        'run_command',
        return_value=SubprocessResult(exit_code=1, output='some error')
    )
    def test_run_dbt(self, mock_run_command):
        """
        Patch SubProcessHook to return a non-0 exit code and check we raise
        an exception for such a result
        """

        with pytest.raises(AirflowException):
            hook = DbtCliHook(env={'GOOGLE_APPLICATION_CREDENTIALS': 'my_creds'})
            hook.run_dbt(['dbt', 'run'])
            mock_run_command.assert_called_once_with(
                command=['dbt', 'run'],
                env={'GOOGLE_APPLICATION_CREDENTIALS': 'my_creds'}
            )

    @mock.patch.object(SubprocessHook, 'get_conn')
    def test_subprocess_kill_called(self, mock_get_conn):
        hook = DbtCliHook()
        hook.get_conn()
        mock_get_conn.assert_called_once()

    @mock.patch.object(SubprocessHook, 'send_sigterm')
    def test_subprocess_get_conn_called(self, mock_send_sigterm):
        hook = DbtCliHook()
        hook.on_kill()
        mock_send_sigterm.assert_called_once()


class TestDbtCloudBuildHook(TestCase):
    @patch('airflow_dbt.hooks.google.CloudBuildHook')
    @patch('airflow_dbt.hooks.google.GCSHook')
    def test_create_build(self, _, MockCloudBuildHook):
        mock_create_build = MockCloudBuildHook().create_build
        mock_create_build.return_value = {
            'id': 'test_id', 'logUrl': 'http://testurl.com'
        }
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
                'name': 'fishtownanalytics/dbt:0.10.10',
                'args': ['docs', 'generate'],
                'env': ['TEST_ENV_VAR=test']
            }],
            'source': {
                'storageSource': {
                    'bucket': 'hello',
                    'object': 'file.tar.gz',
                }
            },
            'serviceAccount': 'projects/test_project_id/serviceAccounts/robot@mail.com',
            'options': {
                'logging': 'GCS_ONLY',

            },
            'logsBucket': 'hello',
        }

        mock_create_build.assert_called_once_with(
            body=expected_body,
            project_id='test_project_id'
        )
