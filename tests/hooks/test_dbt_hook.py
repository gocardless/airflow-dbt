import subprocess
from unittest import TestCase, mock

from airflow_dbt.hooks.dbt_hook import DbtCliHook


class TestDbtHook(TestCase):

    @mock.patch('subprocess.Popen')
    def test_sub_commands(self, mock_subproc_popen):
        mock_subproc_popen.return_value \
            .communicate.return_value = ('output', 'error')
        mock_subproc_popen.return_value.returncode = 0
        mock_subproc_popen.return_value \
            .stdout.readline.side_effect = [b"placeholder"]

        hook = DbtCliHook()
        hook.run_cli(['dbt', 'docs', 'generate'])

        mock_subproc_popen.assert_called_once_with(
            args=['dbt', 'docs', 'generate'],
            env={},
            close_fds=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

    @mock.patch('subprocess.Popen')
    def test_envs(self, mock_subproc_popen):
        mock_subproc_popen.return_value.communicate.return_value = ('output', 'error')
        mock_subproc_popen.return_value.returncode = 0
        mock_subproc_popen.return_value.stdout.readline.side_effect = [b"placeholder"]

        hook = DbtCliHook(env={"foo": "bar", "baz": "true"})
        hook.run_cli(['dbt', 'run'])

        mock_subproc_popen.assert_called_once_with(
            args=['dbt', 'run'],
            env={"foo": "bar", "baz": "true"},
            close_fds=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )
