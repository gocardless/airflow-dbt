from unittest import TestCase, mock

from airflow.hooks.subprocess import SubprocessHook

from airflow_dbt.hooks.dbt_hook import DbtCliHook


class TestDbtHook(TestCase):
    @mock.patch.object(SubprocessHook, 'run_command')
    def test_sub_commands(self, mock_run_command):
        hook = DbtCliHook()
        hook.run_dbt(['dbt', 'docs', 'generate'])
        mock_run_command.assert_called_once_with(
            command=['dbt', 'docs', 'generate'],
            env={},
            cwd='.',
        )

    def test_vars(self):
        hook = DbtCliHook()
        generated_command = hook.generate_dbt_cli_command(
            'run',
            vars={"foo": "bar", "baz": "true"}
        )

        assert generated_command == ['dbt', 'run', '--vars',
            '{"foo": "bar", "baz": "true"}']
