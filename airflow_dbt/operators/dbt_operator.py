import logging
import warnings
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_dbt.dbt_command_config import DbtCommandConfig
from airflow_dbt.hooks import DbtCliHook, DbtCloudBuildHook


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator
    All other dbt operators are derived from this operator.

    :param profiles_dir: If set, passed as the `--profiles-dir` argument to
    the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt`
    command
    :type dir: str
    :param dir: The directory to run the CLI in
    :param env: If set, passed to the dbt executor
    :type env: dict
    :type vars: str
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    :type vars: dict
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :type full_refresh: bool
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
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your
    `PATH`
    :type dbt_bin: str
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    :param dbt_hook: The dbt hook to use as executor. For now the
    implemented ones are: DbtCliHook, DbtCloudBuildHook. It should be an
    instance of one of those, or another that inherits from DbtBaseHook. If
    not provided by default a DbtCliHook will be instantiated with the
    provided params
    :type dbt_hook: DbtBaseHook
    :param base_command: The dbt sub command to run, for example for `dbt
        run` the base_command will be `run`. If any other flag not
        contemplated must be included it can also be added to this string
    :type base_command: str
    """

    ui_color = '#d6522a'

    template_fields = ['env', 'dbt_bin', 'command', 'config']

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
        no_compile=False,

        *vargs,
        **kwargs
    ):
        super(DbtBaseOperator, self).__init__(*vargs, **kwargs)

        if dir is not None:
            warnings.warn('"dir" param is deprecated in favor of dbt native '
                          'param "project_dir"')
            if project_dir is None:
                logging.warning('Using "dir" as "project_dir"')
                project_dir = dir

        self.env = {} if env is None else env
        self.dbt_bin = dbt_bin
        self.command = command
        self.config = config if config is not None else {
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
        }
        self.env = env
        self.hook = dbt_hook

    def instantiate_hook(self):
        """
        Instantiates the underlying dbt hook. This has to be deferred until
        after the constructor or the templated params wont be interpolated.
        """
        dbt_hook = self.hook
        self.hook = dbt_hook if dbt_hook is not None else DbtCliHook(
            env=self.env,
        )

    def execute(self, context: Any):
        """Runs the provided command in the provided execution environment"""
        self.instantiate_hook()

        dbt_cli_command = self.hook.generate_dbt_cli_command(
            dbt_bin=self.dbt_bin,
            command=self.command,
            **self.config
        )
        self.hook.run_dbt(dbt_cli_command)


class DbtRunOperator(DbtBaseOperator):
    """Runs a dbt run command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='run', **kwargs)


class DbtTestOperator(DbtBaseOperator):
    """Runs a dbt test command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='test', **kwargs)


class DbtDocsGenerateOperator(DbtBaseOperator):
    """Runs a dbt docs generate command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='docs generate', **kwargs)


class DbtSnapshotOperator(DbtBaseOperator):
    """Runs a dbt snapshot command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='snapshot', **kwargs)


class DbtSeedOperator(DbtBaseOperator):
    """Runs a dbt seed command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='seed', **kwargs)


class DbtDepsOperator(DbtBaseOperator):
    """Runs a dbt deps command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='deps', **kwargs)


class DbtCleanOperator(DbtBaseOperator):
    """Runs a dbt clean command"""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, command='clean', **kwargs)


class DbtCloudBuildOperator(DbtBaseOperator):
    """Uses the CloudBuild Hook to run the operation in there by default"""

    template_fields = [
        'env', 'dbt_bin', 'command', 'config', 'gcs_staging_location',
        'project_id', 'dbt_version', 'service_account'
    ]

    @apply_defaults
    def __init__(
        self,
        gcs_staging_location: str,
        env: Dict = None,
        config: DbtCommandConfig = None,
        project_id: str = None,
        gcp_conn_id: str = None,
        dbt_version: str = None,
        dbt_bin: Optional[str] = None,
        service_account: str = None,
        *args,
        **kwargs
    ):
        self.gcs_staging_location = gcs_staging_location
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.dbt_version = dbt_version
        self.service_account = service_account

        super(DbtCloudBuildOperator, self).__init__(
            env=env,
            config=config,
            dbt_bin=dbt_bin,
            *args,
            **kwargs
        )

    def instantiate_hook(self):
        """
        Instantiates a Cloud build dbt hook. This has to be done out of the
        constructor because by the time the constructor runs the params have
        not been yet interpolated.
        """
        hook_config = {
            'env': self.env,
            'gcs_staging_location': self.gcs_staging_location,
        }
        if self.project_id is not None:
            hook_config['project_id'] = self.project_id
        if self.gcp_conn_id is not None:
            hook_config['gcp_conn_id'] = self.gcp_conn_id
        if self.dbt_version is not None:
            hook_config['dbt_version'] = self.dbt_version
        if self.service_account is not None:
            hook_config['service_account'] = self.service_account

        self.hook = DbtCloudBuildHook(**hook_config)
