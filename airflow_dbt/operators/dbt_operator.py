import json
import warnings

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_dbt.dbt_command_params import DbtCommandParamsConfig, DbtGlobalParamsConfig
from airflow_dbt.hooks.dbt_hook import DbtCliHook, generate_dbt_cli_command


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator
    All other dbt operators are derived from this operator.

    :param env: If set, passes the env variables to the subprocess handler
    :type env: dict
    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :type target: str
    :param dir:
        The directory to run the CLI in
        ..deprecated:: 0.4.0
            Use ``project_dir`` instead
    :type dir: str
    :param project_dir: The directory to run the CLI in
    :type project_dir: str
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    :type vars: dict
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :type full_refresh: bool
    :param models: If set, passed as the `--models` argument to the `dbt` command
    :type models: str
    :param warn_error: If `True`, treat warnings as errors.
    :type warn_error: bool
    :param exclude: If set, passed as the `--exclude` argument to the `dbt` command
    :type exclude: str
    :param select: If set, passed as the `--select` argument to the `dbt` command
    :type select: str
    :param selector: If set, passed as the `--selector` argument to the `dbt` command
    :type selector: str
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
    :type dbt_bin: str
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    :param record_timing_info: stores runtime info in a text file to be analyzed later
    :type record_timing_info: str
    :param debug: if true prints debug information during the runtime
    :type debug: bool
    :param log_format: Determines the log format for the debug information. Only allowed value is 'json'
    :type log_format: str. Must be 'json'. If set all output will be json instead of plain text
    :param write_json: determines whether dbt writes JSON artifacts (eg. manifest.json, run_results.json) to the
    target/ directory. JSON serialization can be slow, and turning this flag off might make invocations of dbt
    faster. Alternatively, you might disable this config if you want to perform a dbt operation and avoid overwriting
    artifacts from a previous run step.
    :type write_json: bool
    :param partial_parse: turn partial parsing on or off in your project
    :type partial_parse: bool
    :param use_experimental_parser: use experimental parser
    :type use_experimental_parser: bool
    :param use_colors: display logs using escaped colors in the terminal
    :type use_colors: bool
    :param fail_fast: stop execution as soon as one error is found
    :type fail_fast: bool
    :param command: the main command to use for dbt. Can be used to invoke the Operator raw with an arbitrary command
    :type command: str
    :param version: print the version of dbt installed
    :type version: bool
    """

    ui_color = '#d6522a'

    template_fields = ['env', 'dbt_bin', 'command', 'command_config', 'global_config']

    @apply_defaults
    def __init__(
        self,
        env: dict = None,
        profiles_dir: str = None,
        target=None,
        dir: str = None,
        project_dir: str = '.',
        vars: dict = None,
        models: str = None,
        exclude: str = None,
        select: str = None,
        selector: str = None,
        dbt_bin: str = 'dbt',
        verbose: bool = None,
        warn_error: bool = None,
        full_refresh: bool = None,
        data=None,
        schema=None,
        record_timing_info: bool = None,
        debug: bool = None,
        log_format: str = None,
        write_json: bool = None,
        partial_parse: bool = None,
        use_experimental_parser: bool = None,
        use_colors: bool = None,
        fail_fast: bool = None,
        command: str = None,
        version: bool = None,
        *args,
        **kwargs
    ):
        super(DbtBaseOperator, self).__init__(*args, **kwargs)

        # dbt has a global param to specify the directory containing the project. Also, `dir` shadows a global
        # python function for listing directory contents.
        if dir is not None:
            warnings.warn('"dir" param is deprecated in favor of dbt native param "project_dir"')

        # global flags
        global_config: DbtGlobalParamsConfig = {
            'record_timing_info': record_timing_info,
            'debug': debug,
            'log_format': log_format,
            'warn_error': warn_error,
            'write_json': write_json,
            'partial_parse': partial_parse,
            'use_experimental_parser': use_experimental_parser,
            'use_colors': use_colors,
            'verbose': verbose,
            'target': target,
            'version': version,
        }
        # per command flags
        command_config: DbtCommandParamsConfig = {
            'profiles_dir': profiles_dir,
            'project_dir': project_dir or dir,
            'full_refresh': full_refresh,
            'models': models,
            'exclude': exclude,
            'select': select,
            'selector': selector,
            'data': data,
            'fail_fast': fail_fast,
            'schema': schema,
            'vars': json.dumps(vars) if vars is not None else None,
        }
        self.env = env or {}
        self.dbt_bin = dbt_bin
        self.command = command
        # filter out None values from the config
        self.global_config = {k: v for k, v in global_config.items() if v is not None}
        self.command_config = {k: v for k, v in command_config.items() if v is not None}
        self.hook = self.create_hook()

    def create_hook(self) -> DbtCliHook:
        """Create the hook to be used by the operator. This is useful for subclasses to override"""
        return DbtCliHook(env=self.env)

    def execute(self, context):
        """Execute the dbt command"""
        dbt_full_command = generate_dbt_cli_command(
            dbt_bin=self.dbt_bin,
            command=self.command,
            global_config=self.global_config,
            command_config=self.command_config,
        )
        self.hook.run_cli(dbt_full_command)


class DbtRunOperator(DbtBaseOperator):
    """ Runs a dbt run command. """
    @apply_defaults
    def __init__(self, command='', *args, **kwargs):
        super(DbtRunOperator, self).__init__(command='run', *args, **kwargs)


class DbtTestOperator(DbtBaseOperator):
    """ Runs a dbt test command. """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DbtTestOperator, self).__init__(command='test', *args, **kwargs)


class DbtDocsGenerateOperator(DbtBaseOperator):
    """ Runs a dbt docs generate command. """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DbtDocsGenerateOperator, self).__init__(command='docs generate', *args, **kwargs)


class DbtSnapshotOperator(DbtBaseOperator):
    """ Runs a dbt snapshot command. """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DbtSnapshotOperator, self).__init__(command='snapshot', *args, **kwargs)


class DbtSeedOperator(DbtBaseOperator):
    """ Runs a dbt seed command. """
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DbtSeedOperator, self).__init__(command='seed', *args, **kwargs)


class DbtDepsOperator(DbtBaseOperator):
    """ Runs a dbt deps command. """
    @apply_defaults
    def __init__(self, command='', *args, **kwargs):
        super(DbtDepsOperator, self).__init__(command='deps', *args, **kwargs)


class DbtCleanOperator(DbtBaseOperator):
    """ Runs a dbt clean command. """
    @apply_defaults
    def __init__(self, command='', *args, **kwargs):
        super(DbtCleanOperator, self).__init__(command='clean', *args, **kwargs)
