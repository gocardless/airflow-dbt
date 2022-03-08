import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

# noinspection PyDeprecation
from airflow.hooks.base_hook import BaseHook

from airflow_dbt.dbt_command_config import DbtCommandConfig


def render_config(config: Dict[str, Union[str, bool]]) -> List[str]:
    """Renders a dictionary of options into a list of cli strings"""
    dbt_command_config_annotations = DbtCommandConfig.__annotations__
    command_params = []
    for key, value in config.items():
        if key not in dbt_command_config_annotations:
            raise ValueError(f"{key} is not a valid key")
        if value is not None:
            param_value_type = type(value)
            # check that the value has the correct type from dbt_command_config_annotations
            if param_value_type != dbt_command_config_annotations[key]:
                raise TypeError(f"{key} has to be of type {dbt_command_config_annotations[key]}")
            # if the param is not bool it must have a non null value
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
    base_config: Dict[str, Union[str, bool]],
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
    :param base_config: Params that apply to the `dbt` program regardless of
        the command it is running
    :type base_config: dict
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
    base_params = render_config(base_config)
    command_params = render_config(command_config)
    # commands like 'dbt docs generate' need the command to be split in two
    command_pieces = command.split(" ")
    return [dbt_bin, *base_params, *command_pieces, *command_params]


class DbtBaseHook(BaseHook, ABC):
    """
    Base abstract class for all DbtHooks to have a common interface and force
    implement the mandatory `run_dbt()` function.
    """

    def __init__(self, env: Optional[Dict] = None):
        """
        :param env: If set will be passed over to cloud build to run in the
            dbt step
        :type env: dict
        """
        super().__init__()
        self.env = env or {}

    @abstractmethod
    def run_dbt(self, dbt_cmd: Union[str, List[str]]):
        """Run the dbt command"""
        pass
