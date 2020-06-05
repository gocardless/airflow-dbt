from airflow_dbt.hooks.dbt_hook import DbtCliHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DbtBaseOperator(BaseOperator):
    """
    Base dbt operator
    All other dbt operators are derived from this operator.

    :param profiles_dir: If set, passed as the `--profiles-dir` argument to the `dbt` command
    :type profiles_dir: str
    :param target: If set, passed as the `--target` argument to the `dbt` command
    :type dir: str
    :param dir: The directory to run the CLI in
    :type vars: str
    :param vars: If set, passed as the `--vars` argument to the `dbt` command
    :type vars: dict
    :param full_refresh: If `True`, will fully-refresh incremental models.
    :type full_refresh: bool
    :param models: If set, passed as the `--models` argument to the `dbt` command
    :type models: str
    :param exclude: If set, passed as the `--exclude` argument to the `dbt` command
    :type exclude: str
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
    :type dbt_bin: str
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    """

    ui_color = '#d6522a'

    @apply_defaults
    def __init__(self,
                 profiles_dir=None,
                 target=None,
                 dir='.',
                 vars=None,
                 models=None,
                 exclude=None,
                 dbt_bin='dbt',
                 verbose=True,
                 full_refresh=False,
                 *args,
                 **kwargs):
        super(DbtBaseOperator, self).__init__(*args, **kwargs)
        self.hook = DbtCliHook(
            profiles_dir=profiles_dir,
            target=target,
            dir=dir,
            vars=vars,
            full_refresh=full_refresh,
            models=models,
            exclude=exclude,
            dbt_bin=dbt_bin,
            verbose=verbose)


class DbtRunOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtRunOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def execute(self, context):
        self.hook.run_cli('run')


class DbtTestOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtTestOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def execute(self, context):
        self.hook.run_cli('test')
