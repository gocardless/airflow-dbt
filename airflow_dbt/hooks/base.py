import json
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union

from airflow.hooks.base_hook import BaseHook

from airflow_dbt.dbt_command_config import DbtCommandConfig


def generate_dbt_cli_command(
    dbt_bin: str,
    command: str,
    **params: Union[str, bool],
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
    :param command: The dbt sub-command to run
    :type command: str
    :param dbt_bin: Path to the dbt binary, defaults to `dbt` assumes it is
        available in the PATH.
    :type dbt_bin: str
    :param command: The dbt sub command to run, for example for `dbt run`
        the base_command will be `run`. If any other flag not contemplated
        must be included it can also be added to this string
    :type command: str
    :param version: Dbt version to use, in SEMVER. Defaults
        to the last one '0.21.0'
    :type version: str
    :param record_timing_info: Dbt flag to add '--record-timing-info'
    :type record_timing_info: bool
    :param debug: Dbt flag to add '--debug'
    :type debug: bool
    :param log_format: Specifies how dbt's logs should be formatted. The
        value for this flag can be one of: text, json, or default
    :type log_format: str
    :param write_json: If set to no it adds the `--no-write-json` Dbt flag
    :type write_json: bool
    :param strict: Only for use during dbt development. It performs extra
        validation of dbt objects and internal consistency checks during
        compilation
    :type strict: bool
    :param warn_error: Converts dbt warnings into errors
    :type warn_error: bool
    :param partial_parse: configure partial parsing in your project, and
        will override the value set in `profiles.yml
    :type partial_parse: bool
    :param use_experimental_parser: Statically analyze model files in your
        project and, if possible, extract needed information 3x faster than
        a full Jinja render
    :type use_experimental_parser: bool
    :param use_colors: Displays colors in dbt logs
    :type use_colors: bool
    :param profiles_dir: Path to profiles.yaml dir. Can be relative from
        the folder the DAG is being run, which usually is the home or de
        DAGs folder
    :type profiles_dir: str
    :param project_dir: Path to the dbt project you want to run. Can be
        relative to the path the DAG is being run
    :type project_dir: str
    :param profile: Which profile to load. Overrides setting in
        dbt_project.yml
    :type profile: Which profile to load. Overrides setting in
        dbt_project.yml
    :param target: Which target to load for the given profile
    :type target: str
    :param config_dir: Sames a profile_dir
    :type config_dir: str
    :param resource_type: One of: model,snapshot,source,analysis,seed,
        exposure,test,default,all
    :type resource_type: str
    :param vars: Supply variables to the project. This argument overrides
        variables defined in your dbt_project.yml file. This argument should
        be a YAML string, eg. '{my_variable: my_value}'
    :type vars: dict
    :param full_refresh: If specified, dbt will drop incremental models and
        fully-recalculate the incremental table from the model definition
    :type full_refresh: bool
    :param data: Run data tests defined in "tests" directory.
    :type data: bool
    :param schema: Run constraint validations from schema.yml files
    :type schema: bool
    :param models: Flag used to choose a node or subset of nodes to apply
        the command to (v0.210.0 and lower)
    :type models: str
    :param exclude: Nodes to exclude from the set defined with
        select/models
    :type exclude: str
    :param select: Flag used to choose a node or subset of nodes to apply
        the command to (v0.21.0 and higher)
    :type select: str
    :param selector: Config param to reference complex selects defined in
        the config yaml
    :type selector: str
    :param output: {json,name,path,selector}
    :type output: str
    :param output_keys: Which keys to output
    :type output_keys: str
    :param host: Specify the host to listen on for the rpc server
    :type host: str
    :param port: Specify the port number for the rpc server
    :type port: int
    :param fail_fast: Stop execution upon a first test failure
    :type fail_fast: bool
    :param args:
    :type args:
    :param no_compile: Do not run "dbt compile" as part of docs generation
    :type no_compile: bool
    """
    dbt_command_config_annotations = DbtCommandConfig.__annotations__
    if not dbt_bin:
        raise ValueError("dbt_bin is mandatory")
    if not command:
        raise ValueError("command mandatory")
    command_params = []
    for key, value in params.items():
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

    return [dbt_bin, *command_params, command]


class DbtBaseHook(BaseHook, ABC):
    """
    Simple wrapper around the dbt CLI and interface to implement dbt hooks
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
