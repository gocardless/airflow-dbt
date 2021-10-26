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

    def __init__(self, env: Dict = None):
        super().__init__()
        self.env = env if env is not None else {}

    def generate_dbt_cli_command(
        self,
        dbt_bin: str = None,
        command: str = None,
        # global flags
        version: bool = False,
        record_timing_info: bool = False,
        debug: bool = False,
        log_format: str = None,  # either 'text', 'json' or 'default'
        write_json: bool = None,
        strict: bool = False,
        warn_error: bool = False,
        partial_parse: bool = False,
        use_experimental_parser: bool = False,
        use_colors: bool = None,
        # command specific config
        profiles_dir: str = None,
        project_dir: str = None,
        profile: str = None,
        target: str = None,
        config_dir: str = None,
        resource_type: str = None,
        vars: Dict = None,
        # run specific
        full_refresh: bool = False,
        # ls specific
        data: bool = False,
        schema: bool = False,
        models: str = None,
        exclude: str = None,
        select: str = None,
        selector: str = None,
        output: str = None,
        output_keys: str = None,
        # rpc specific
        host: str = None,
        port: str = None,
        # test specific
        fail_fast: bool = False,
        args: dict = None,
        no_compile: bool = False,
    ) -> List[str]:
        """
        Generate the command that will be run based on class properties,
        presets and dbt commands

        :param command: The dbt sub-command to run
        :type command: str
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

        dbt_cmd: List[str] = []

        # if there's no bin do not append it. Rather generate the command
        # without the `/path/to/dbt` prefix. That is useful for running it
        # inside containers that have already set the entrypoint.
        if dbt_bin is not None and not dbt_bin == '':
            dbt_cmd.append(dbt_bin)

        # add global flags at the beginning
        if version:
            dbt_cmd.append('--version')

        if record_timing_info:
            dbt_cmd.append('--record-timing-info')

        if debug:
            dbt_cmd.append('--debug')

        if log_format is not None:
            dbt_cmd.extend(['--log-format', log_format])

        if write_json is not None:
            write_json_flag = '--write-json' if write_json else \
                '--no-write-json'
            dbt_cmd.append(write_json_flag)

        if strict:
            dbt_cmd.append('--strict')

        if warn_error:
            dbt_cmd.append('--warn-error')

        if partial_parse:
            dbt_cmd.append('--partial-parse')

        if use_experimental_parser:
            dbt_cmd.append('--use-experimental-parser')

        if use_colors is not None:
            colors_flag = "--use-colors" if use_colors else "--no-use-colors"
            dbt_cmd.append(colors_flag)

        # appends the main command
        dbt_cmd.append(command)

        # appends configuration relative to the command
        if profiles_dir is not None:
            dbt_cmd.extend(['--profiles-dir', profiles_dir])

        if project_dir is not None:
            dbt_cmd.extend(['--project-dir', project_dir])

        if profile is not None:
            dbt_cmd.extend(['--profile', profile])

        if target is not None:
            dbt_cmd.extend(['--target', target])

        # debug specific
        if config_dir is not None:
            dbt_cmd.extend(['--config-dir', config_dir])

        # ls specific
        if resource_type is not None:
            dbt_cmd.extend(['--resource-type', resource_type])

        if select is not None:
            dbt_cmd.extend(['--select', select])

        if models is not None:
            dbt_cmd.extend(['--models', models])

        if exclude is not None:
            dbt_cmd.extend(['--exclude', exclude])

        if selector is not None:
            dbt_cmd.extend(['--selector', selector])

        if output is not None:
            dbt_cmd.extend(['--output', output])

        if output_keys is not None:
            dbt_cmd.extend(['--output-keys', output_keys])

        # rpc specific
        if host is not None:
            dbt_cmd.extend(['--host', host])

        if port is not None:
            dbt_cmd.extend(['--port', str(port)])

        # run specific
        if full_refresh:
            dbt_cmd.append('--full-refresh')

        if fail_fast:
            dbt_cmd.append('--fail-fast')

        if vars is not None:
            dbt_cmd.extend(['--vars', json.dumps(vars)])

        # run-operation specific
        if args is not None:
            dbt_cmd.extend(['--args', json.dumps(args)])

        # test specific
        if data:
            dbt_cmd.append('--data')

        if schema:
            dbt_cmd.append('--schema')

        if no_compile:
            dbt_cmd.append('--no-compile')

        return dbt_cmd

    @abstractmethod
    def run_dbt(self, dbt_cmd: Union[str, List[str]]):
        """Run the dbt command"""
