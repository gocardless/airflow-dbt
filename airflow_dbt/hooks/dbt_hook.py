from __future__ import print_function
import os
import signal
import subprocess
import json
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook


class DbtCliHook(BaseHook):
    """
    Simple wrapper around the dbt CLI.

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
    :param warn_error: If `True`, treat warnings as errors.
    :type warn_error: bool
    :param exclude: If set, passed as the `--exclude` argument to the `dbt` command
    :type exclude: str
    :param select: If set, passed as the `--select` argument to the `dbt` command
    :type select: str
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your `PATH`
    :type dbt_bin: str
    :param output_encoding: Output encoding of bash command. Defaults to utf-8
    :type output_encoding: str
    :param verbose: The operator will log verbosely to the Airflow logs
    :type verbose: bool
    """

    def __init__(self,
                 profiles_dir=None,
                 target=None,
                 dir='.',
                 vars=None,
                 full_refresh=False,
                 data=False,
                 schema=False,
                 models=None,
                 exclude=None,
                 select=None,
                 dbt_bin='dbt',
                 output_encoding='utf-8',
                 verbose=True,
                 warn_error=False):
        self.profiles_dir = profiles_dir
        self.dir = dir
        self.target = target
        self.vars = vars
        self.full_refresh = full_refresh
        self.data = data
        self.schema = schema
        self.models = models
        self.exclude = exclude
        self.select = select
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.warn_error = warn_error
        self.output_encoding = output_encoding

    def _dump_vars(self):
        # The dbt `vars` parameter is defined using YAML. Unfortunately the standard YAML library
        # for Python isn't very good and I couldn't find an easy way to have it formatted
        # correctly. However, as YAML is a super-set of JSON, this works just fine.
        return json.dumps(self.vars)

    def run_cli(self, *command):
        """
        Run the dbt cli

        :param command: The dbt command to run
        :type command: str
        """

        dbt_cmd = [self.dbt_bin, *command]

        if self.profiles_dir is not None:
            dbt_cmd.extend(['--profiles-dir', self.profiles_dir])

        if self.target is not None:
            dbt_cmd.extend(['--target', self.target])

        if self.vars is not None:
            dbt_cmd.extend(['--vars', self._dump_vars()])

        if self.data:
            dbt_cmd.extend(['--data'])

        if self.schema:
            dbt_cmd.extend(['--schema'])

        if self.models is not None:
            dbt_cmd.extend(['--models', self.models])

        if self.exclude is not None:
            dbt_cmd.extend(['--exclude', self.exclude])

        if self.select is not None:
            dbt_cmd.extend(['--select', self.select])

        if self.full_refresh:
            dbt_cmd.extend(['--full-refresh'])

        if self.warn_error:
            dbt_cmd.insert(1, '--warn-error')

        if self.verbose:
            self.log.info(" ".join(dbt_cmd))

        sp = subprocess.Popen(
            dbt_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self.dir,
            close_fds=True)
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
        self.log.info('Sending SIGTERM signal to dbt command')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)
