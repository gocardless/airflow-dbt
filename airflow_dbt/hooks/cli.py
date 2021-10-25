from __future__ import print_function

from typing import Any, Dict, List, Union

from airflow.hooks.subprocess import SubprocessHook

from hooks.base import DbtBaseHook


class DbtCliHook(DbtBaseHook):
    """
    Run the dbt command in the same airflow worker the task is being run.
    This requires the `dbt` python package to be installed in it first. Also
    the dbt_bin path might not be set in the `PATH` variable, so it could be
    necessary to set it in the constructor.

    :type dir: str
    :param dir: The directory to run the CLI in
    :type env: dict
    :param env: If set, passed to the dbt executor
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your
        `PATH`
    :type dbt_bin: str
    """

    def __init__(self, env: Dict = None, dbt_bin='dbt'):
        self.sp = SubprocessHook()
        super().__init__(env=env, dbt_bin=dbt_bin)

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
        self.sp.run_command(
            command=dbt_cmd,
            env=self.env,
        )

    def on_kill(self):
        """Kill the open subprocess if the task gets killed by Airflow"""
        self.sp.send_sigterm()
