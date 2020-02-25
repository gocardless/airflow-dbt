from airflow_dbt.hooks.dbt_hook import DbtCliHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DbtBaseOperator(BaseOperator):
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
                 *args,
                 **kwargs):
        super(DbtBaseOperator, self).__init__(*args, **kwargs)
        self.hook = DbtCliHook(
            profiles_dir=profiles_dir,
            target=target,
            dir=dir,
            vars=vars,
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


class DbtDepsOperator(DbtBaseOperator):
    @apply_defaults
    def __init__(self, profiles_dir=None, target=None, *args, **kwargs):
        super(DbtDepsOperator, self).__init__(profiles_dir=profiles_dir, target=target, *args, **kwargs)

    def execute(self, context):
        self.hook.run_cli('deps')
