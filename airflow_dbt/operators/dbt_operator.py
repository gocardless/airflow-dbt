import logging
import warnings
from typing import Any, Dict, List, Optional

from airflow.models import BaseOperator
# noinspection PyDeprecation
from airflow.utils.decorators import apply_defaults

from airflow_dbt.dbt_command_config import DbtCommandConfig
from airflow_dbt.hooks.base import DbtBaseHook, generate_dbt_cli_command
from airflow_dbt.hooks.cli import DbtCliHook


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator. All other dbt operators should inherit from this one.

    It receives all possible dbt options in the constructor. If no hook is
    provided it uses the DbtCliHook to run the generated command.
    """

    ui_color = '#d6522a'
    ui_fgcolor = "white"
    # add all the str/dict params to the templates
    template_fields = ['dbt_env', 'dbt_bin', 'dbt_command', 'dbt_config']
    template_fields_renderers = {
        'dbt_env': 'json',
        'dbt_config': 'json',
    }

    dbt_env: Dict
    dbt_bin: str
    dbt_command: str
    dbt_config: Dict
    dbt_hook: DbtBaseHook
    dbt_cli_command: List[str]

    # noinspection PyShadowingBuiltins, PyDeprecation
    @apply_defaults
    def __init__(
        self,
        env: Dict = None,
        dbt_bin: Optional[str] = 'dbt',
        dbt_hook=None,
        command: Optional[str] = None,
        config: DbtCommandConfig = None,
        # dir deprecated in favor of dbt native project and profile directories
        dir: str = None,
        # if config was not provided we un-flatten them from the kwargs
        # global flags
        version: bool = None,
        record_timing_info: bool = None,
        debug: bool = None,
        log_format: str = None,  # either 'text', 'json' or 'default'
        write_json: bool = None,
        strict: bool = None,
        warn_error: bool = None,
        partial_parse: bool = None,
        use_experimental_parser: bool = None,
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
        full_refresh: bool = None,
        # ls specific
        data: bool = None,
        schema: bool = None,
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
        fail_fast: bool = None,
        args: dict = None,
        no_compile: bool = None,

        *vargs,
        **kwargs
    ):
        """
        :param env: Dictionary with environment variables to be used in the
            runtime
        :type env: dict
        :param dbt_bin: Path to the dbt binary, defaults to `dbt` assumes it is
            available in the PATH.
        :type dbt_bin: str
        :param dbt_hook: The dbt hook to use as executor. For now the
            implemented ones are: DbtCliHook, DbtCloudBuildHook. It should be an
            instance of one of those, or another that inherits from DbtBaseHook.
            If not provided by default a DbtCliHook will be instantiated with
            the provided params
        :type dbt_hook: DbtBaseHook
        :param command: The dbt sub command to run, for example for `dbt run`
            the base_command will be `run`. If any other flag not contemplated
            must be included it can also be added to this string
        :type command: str
        :param config: TypedDictionary which accepts all of the commands
            related to executing dbt. This way you can separate them from the
            ones destined for execution
        :type config: DbtCommandConfig
        :param dir: Legacy param to set the dbt project directory
        :type dir: str
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
        :param vargs: rest of the positional args
        :param kwargs: rest of the keyword args

        """
        super(DbtBaseOperator, self).__init__(*vargs, **kwargs)

        if dir is not None:
            warnings.warn(
                '"dir" param is deprecated in favor of dbt native '
                'param "project_dir"', PendingDeprecationWarning
            )
            if project_dir is None:
                logging.warning('Using "dir" as "project_dir"')
                project_dir = dir

        self.dbt_env = env or {}
        self.dbt_bin = dbt_bin
        self.dbt_command = command
        # defaults to an empty dict
        config = config or {}
        # overrides with the top level config
        config.update({
            # global flags
            'version': version,
            'record_timing_info': record_timing_info,
            'debug': debug,
            'log_format': log_format,
            'write_json': write_json,
            'strict': strict,
            'warn_error': warn_error,
            'partial_parse': partial_parse,
            'use_experimental_parser': use_experimental_parser,
            'use_colors': use_colors,
            # per command flags
            'profiles_dir': profiles_dir,
            'project_dir': project_dir,
            'target': target,
            'vars': vars,
            # run specific
            'full_refresh': full_refresh,
            'profile': profile,
            # docs specific
            'no_compile': no_compile,
            # debug specific
            'config_dir': config_dir,
            # ls specific
            'resource_type': resource_type,
            'select': select,
            'models': models,
            'exclude': exclude,
            'selector': selector,
            'output': output,
            'output_keys': output_keys,
            # rpc specific
            'host': host,
            'port': port,
            # run specific
            'fail_fast': fail_fast,
            # run-operation specific
            'args': args,
            # test specific
            'data': data,
            'schema': schema,
        })
        # filter out None values from the constructor
        config = {
            key: val
            for key, val in config.items()
            if val is not None
        }
        self.dbt_config = config
        self.dbt_env = env
        self.dbt_hook = dbt_hook

    def instantiate_hook(self):
        """
        Instantiates the underlying dbt hook. This has to be deferred until
        after the constructor or the templated params won't be interpolated.
        """
        dbt_hook = self.dbt_hook
        self.dbt_hook = dbt_hook if dbt_hook is not None else DbtCliHook(
            env=self.dbt_env,
        )

    def execute(self, context: Any):
        """Runs the provided command in the provided execution environment"""
        self.instantiate_hook()
        dbt_base_params = [
            'log_format', 'version', 'use_colors', 'warn_error',
            'partial_parse', 'use_experimental_parser', 'profiles_dir'
        ]

        dbt_base_config = {
            key: val
            for key, val in self.dbt_config.items()
            if key in dbt_base_params
        }

        dbt_command_config = {
            key: val
            for key, val in self.dbt_config.items()
            if key not in dbt_base_params
        }

        self.dbt_cli_command = generate_dbt_cli_command(
            dbt_bin=self.dbt_bin,
            command=self.dbt_command,
            base_config=dbt_base_config,
            command_config=dbt_command_config,
        )
        self.dbt_hook.run_dbt(self.dbt_cli_command)


class DbtRunOperator(DbtBaseOperator):
    """Runs a dbt run command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='run', **kwargs)


class DbtTestOperator(DbtBaseOperator):
    """Runs a dbt test command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='test', **kwargs)


class DbtDocsGenerateOperator(DbtBaseOperator):
    """Runs a dbt docs generate command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='docs generate', **kwargs)


class DbtSnapshotOperator(DbtBaseOperator):
    """Runs a dbt snapshot command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='snapshot', **kwargs)


class DbtSeedOperator(DbtBaseOperator):
    """Runs a dbt seed command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='seed', **kwargs)


class DbtDepsOperator(DbtBaseOperator):
    """Runs a dbt deps command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='deps', **kwargs)


class DbtCleanOperator(DbtBaseOperator):
    """Runs a dbt clean command"""

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='clean', **kwargs)
