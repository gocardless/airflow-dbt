from __future__ import print_function

import json
from abc import ABC, abstractmethod
from typing import Dict, List, Union

from airflow.hooks.base_hook import BaseHook


class DbtBaseHook(BaseHook, ABC):
    """
    Simple wrapper around the dbt CLI.

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
        :param project_dir: If set, passed as the `--project-dir` argument to
            the `dbt` command. It is required but by default points to the
            current folder: '.'
        :type project_dir: str
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
        :param use_colors: If set it adds the flag `--use-colors` or
            `--no-use-colors`, depending if True or False.
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
