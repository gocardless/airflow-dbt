from __future__ import print_function

import json
import os
import signal
import subprocess
from typing import Any, Dict, List, Union

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from airflow_dbt.dbt_command_params import DbtCommandParamsConfig


def render_config(config: Dict[str, Union[str, bool]]) -> List[str]:
    """Renders a dictionary of options into a list of cli strings"""
    dbt_command_config_annotations = DbtCommandParamsConfig.__annotations__
    command_params = []
    for key, value in config.items():
        if key not in dbt_command_config_annotations:
            raise ValueError(f"{key} is not a valid key")
        if value is not None:
            param_value_type = type(value)
            # check that the value has the correct type from dbt_command_config_annotations
            if param_value_type != dbt_command_config_annotations[key]:
                raise TypeError(f"{key} has to be of type {dbt_command_config_annotations[key]}")
            # if the param is not bool it must have a non-null value
            flag_prefix = ''
            if param_value_type is bool and not value:
                flag_prefix = 'no-'
            cli_param_from_kwarg = "--" + flag_prefix + key.replace("_", "-")
            command_params.append(cli_param_from_kwarg)
            if param_value_type is str:
                command_params.append(value)
            elif param_value_type is int:
                command_params.append(str(value))
            elif param_value_type is dict:
                command_params.append(json.dumps(value))
    return command_params


def generate_dbt_cli_command(
    dbt_bin: str,
    command: str,
    global_config: Dict[str, Union[str, bool]],
    command_config: Dict[str, Union[str, bool]],
) -> List[str]:
    """
    Creates a CLI string from the keys in the dictionary. If the key is none
    it is ignored. If the key is of type boolean the name of the key is added.
    If the key is of type string it adds the the key prefixed with tow dashes.
    If the key is of type integer it adds the the key prefixed with three
    dashes.
    dbt_bin and command are mandatory.
    Boolean flags must always be positive.
    Available params are:
    :param command_config: Specific params for the commands
    :type command_config: dict
    :param global_config: Params that apply to the `dbt` program regardless of
        the command it is running
    :type global_config: dict
    :param command: The dbt sub-command to run
    :type command: str
    :param dbt_bin: Path to the dbt binary, defaults to `dbt` assumes it is
        available in the PATH.
    :type dbt_bin: str
    :param command: The dbt sub command to run, for example for `dbt run`
        the base_command will be `run`. If any other flag not contemplated
        must be included it can also be added to this string
    :type command: str
    """
    if not dbt_bin:
        raise ValueError("dbt_bin is mandatory")
    if not command:
        raise ValueError("command mandatory")
    base_params = render_config(global_config)
    command_params = render_config(command_config)
    # commands like 'dbt docs generate' need the command to be split in two
    command_pieces = command.split(" ")
    return [dbt_bin, *base_params, *command_pieces, *command_params]


class DbtCliHook(BaseHook):
    """
    Simple wrapper around the dbt CLI.

    :param env: Environment variables to pass to the dbt process
    :type env: dict
    :param dbt_bin: Path to the dbt binary
    :type dbt_bin: str
    :param global_flags: Global flags to pass to the dbt process
    :type global_flags: dict
    :param command_flags: Command flags to pass to the dbt process
    :type command_flags: dict
    :param command: The dbt command to run
    :type command: str
    :param output_encoding: The encoding of the output
    :type output_encoding: str
    """

    def __init__(
        self,
        env: dict = None,
        output_encoding: str = 'utf-8',
    ):
        super().__init__()
        self.env = env or {}
        self.output_encoding = output_encoding
        self.sp = None  # declare the terminal to be user later on

    def get_conn(self) -> Any:
        """Implements the get_conn method of the BaseHook class"""
        pass

    def run_cli(self, dbt_cmd: List[str]):
        """
        Run the rendered dbt command

        :param dbt_cmd: The dbt command to run
        :type dbt_cmd: list
        """
        sp = subprocess.Popen(
            args=dbt_cmd,
            env=self.env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            close_fds=True
        )
        self.sp = sp
        self.log.info("Output:")
        line = ''
        for line in iter(sp.stdout.readline, b''):
            line = line.decode(self.output_encoding).rstrip()
            self.log.info(line)
        sp.wait()
        self.log.info(
            "Command exited with return code %s",
            sp.returncode
        )

        if sp.returncode:
            raise AirflowException("dbt command failed")

    def on_kill(self):
        """Called when the task is killed by Airflow. This will kill the dbt process and wait for it to exit"""
        self.log.info('Sending SIGTERM signal to dbt command')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
