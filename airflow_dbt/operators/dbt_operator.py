import logging
from typing import Any, Dict

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow_dbt.hooks.dbt_hook import DbtCliHook


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator
    All other dbt operators are derived from this operator.

    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :type dir: str
    :param dir: The directory to run the CLI in
    :param env: If set, passed to the dbt executor
    :type env: dict
    :type vars: str
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
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
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

    template_fields = ['profiles_dir', 'project_dir', 'target', 'env',
        'vars', 'models', 'exclude', 'select', 'dbt_bin', 'verbose',
        'warn_error', 'full_refresh', 'data', 'schema', 'base_command']

    @apply_defaults
    def __init__(self,
                 profiles_dir=None,
                 project_dir = None,
                 dir: str = '.',
                 target=None,
                 env: Dict = None,
                 vars=None,
                 models=None,
                 exclude=None,
                 select=None,
                 dbt_bin='dbt',
                 verbose=True,
                 warn_error=False,
                 full_refresh=False,
                 data=False,
                 schema=False,
                 dbt_hook=None,
                 base_command=None,
                 *args,
                 **kwargs):
        super(DbtBaseOperator, self).__init__(*args, **kwargs)

        self.profiles_dir = profiles_dir
        self.project_dir = project_dir if project_dir is not None else dir
        self.target = target
        self.env = {} if env is None else env
        self.vars = vars
        self.models = models
        self.full_refresh = full_refresh
        self.data = data
        self.schema = schema
        self.exclude = exclude
        self.select = select
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.warn_error = warn_error
        self.base_command = base_command
        self.hook = dbt_hook if dbt_hook is not None else DbtCliHook(
            dir=dir,
            env=self.env,
            dbt_bin=dbt_bin
        )

    def execute(self, context: Any):
        """Runs the provided command in the provided execution environment"""
        dbt_cli_command = self.hook.generate_dbt_cli_command(
            base_command=self.base_command,
            profiles_dir=self.profiles_dir,
            project_dir=self.project_dir,
            target=self.target,
            vars=self.vars,
            full_refresh=self.full_refresh,
            data=self.data,
            schema=self.schema,
            models=self.models,
            exclude=self.exclude,
            select=self.select,
            warn_error=self.warn_error,
        )
        logging.info(f'Running dbt command "{dbt_cli_command}"')
        self.hook.run_dbt(dbt_cli_command)


class DbtRunOperator(DbtBaseOperator):
    """Runs a dbt run command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='run',
            *args,
            **kwargs
        )


class DbtTestOperator(DbtBaseOperator):
    """Runs a dbt test command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='test',
            *args,
            **kwargs
        )


class DbtDocsGenerateOperator(DbtBaseOperator):
    """Runs a dbt docs generate command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='docs generate',
            *args,
            **kwargs
        )


class DbtSnapshotOperator(DbtBaseOperator):
    """Runs a dbt snapshot command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='snapshot',
            *args,
            **kwargs
        )


class DbtSeedOperator(DbtBaseOperator):
    """Runs a dbt seed command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='seed',
            *args,
            **kwargs
        )


class DbtDepsOperator(DbtBaseOperator):
    """Runs a dbt deps command"""
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super().__init__(
            profiles_dir=profiles_dir,
            target=target,
            base_command='deps',
            *args,
            **kwargs
        )
