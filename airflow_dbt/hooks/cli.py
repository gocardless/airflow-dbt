from __future__ import print_function

from typing import Any, Dict, List, Union

from airflow import AirflowException
from airflow.hooks.subprocess import SubprocessHook

from airflow_dbt.hooks.base import DbtBaseHook


class DbtCliHook(DbtBaseHook):
    """
    Run the dbt command in the same airflow worker the task is being run.
    This requires the `dbt` python package to be installed in it first.
    """

    def __init__(self, env: Dict = None):
        """
        :type env:
        :param env: Environment variables that will be passed to the
            subprocess. Must be a dictionary of key-values
        """
        self.sp = SubprocessHook()
        super().__init__(env=env)

    def get_conn(self) -> Any:
        """
        Return the subprocess connection, which isn't implemented, just for
        conformity
        """
        return self.sp.get_conn()

    def run_dbt(self, dbt_cmd: Union[str, List[str]]):
        """
         Run the dbt cli

         :param dbt_cmd: The dbt whole command to run
         :type dbt_cmd: List[str]
         """
        result = self.sp.run_command(
            command=dbt_cmd,
            env=self.env,
        )

        if result.exit_code != 0:
            raise AirflowException(f'Error executing the DBT command: '
                                   f'{result.output}')

    def on_kill(self):
        """Kill the open subprocess if the task gets killed by Airflow"""
        self.sp.send_sigterm()
