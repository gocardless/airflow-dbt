from __future__ import print_function

import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Union

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.subprocess import SubprocessHook


class DbtBaseHook(BaseHook, ABC):
    """
    Simple wrapper around the dbt CLI.

    :type dir: str
    :param dir: The directory to run the CLI in
    :type env: dict
    :param env: If set, passed to the dbt executor
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your
        `PATH`
    :type dbt_bin: str
    """

    def __init__(self, env: Dict = None, dbt_bin='dbt'):
        super().__init__()
        self.env = env if env is not None else {}
        self.dbt_bin = dbt_bin

    def generate_dbt_cli_command(
        self,
        base_command: str,
        profiles_dir: str = '.',
        project_dir: str = '.',
        target: str = None,
        vars: Dict = None,
        full_refresh: bool = False,
        data: bool = False,
        schema: bool = False,
        models: str = None,
        exclude: str = None,
        select: str = None,
        use_colors: bool = None,
        warn_error: bool = False,
    ) -> List[str]:
        """
        Generate the command that will be run based on class properties,
        presets and dbt commands

        :param base_command: The dbt sub-command to run
        :type base_command: str
        :param profiles_dir: If set, passed as the `--profiles-dir` argument to
            the `dbt` command
        :type profiles_dir: str
        :param target: If set, passed as the `--target` argument to the `dbt`
            command
        :type vars: Union[str, dict]
        :param vars: If set, passed as the `--vars` argument to the `dbt`
            command
        :param full_refresh: If `True`, will fully-refresh incremental models.
        :type full_refresh: bool
        :param data:
        :type data: bool
        :param schema:
        :type schema: bool
        :param models: If set, passed as the `--models` argument to the `dbt`
            command
        :type models: str
        :param warn_error: If `True`, treat warnings as errors.
        :type warn_error: bool
        :param exclude: If set, passed as the `--exclude` argument to the `dbt`
        command
        :type exclude: str
        :param select: If set, passed as the `--select` argument to the `dbt`
        command
        :type select: str
        """
        # if there's no bin do not append it. Rather generate the command
        # without the `/path/to/dbt` prefix. That is useful for running it
        # inside containers
        if self.dbt_bin == '' or self.dbt_bin is None:
            dbt_cmd = []
        else:
            dbt_cmd = [self.dbt_bin]

        dbt_cmd.append(base_command)

        if profiles_dir is not None:
            dbt_cmd.extend(['--profiles-dir', profiles_dir])

        if project_dir is not None:
            dbt_cmd.extend(['--project-dir', project_dir])

        if target is not None:
            dbt_cmd.extend(['--target', target])

        if vars is not None:
            dbt_cmd.extend(['--vars', json.dumps(vars)])

        if data:
            dbt_cmd.append('--data')

        if schema:
            dbt_cmd.append('--schema')

        if models is not None:
            dbt_cmd.extend(['--models', models])

        if exclude is not None:
            dbt_cmd.extend(['--exclude', exclude])

        if select is not None:
            dbt_cmd.extend(['--select', select])

        if full_refresh:
            dbt_cmd.append('--full-refresh')

        if warn_error:
            dbt_cmd.insert(1, '--warn-error')

        if use_colors is not None:
            colors_flag = "--use-colors" if use_colors else "--no-use-colors"
            dbt_cmd.append(colors_flag)

        return dbt_cmd

    @abstractmethod
    def run_dbt(self, dbt_cmd: Union[str, List[str]]):
        """Run the dbt command"""


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

    def __init__(self, dir: str = '.', env: Dict = None, dbt_bin='dbt'):
        self.sp = SubprocessHook()
        super().__init__(dir=dir, env=env, dbt_bin=dbt_bin)

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
