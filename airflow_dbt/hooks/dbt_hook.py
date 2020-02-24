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
    :param output_encoding: Output encoding of bash command
    :type output_encoding: str
    """

    def __init__(self,
                 profiles_dir=None,
                 target=None,
                 dir='.',
                 vars=None,
                 models=None,
                 exclude=None,
                 dbt_bin='dbt',
                 output_encoding='utf-8',
                 verbose=True):
        """
        :param profiles_dir: the directory to load the profiles from
        :type profiles_dir: string
        :param dir: The directory to run the CLI in
        :type dir: string
        """
        self.profiles_dir = profiles_dir
        self.dir = dir
        self.target = target
        self.vars = vars
        self.models = models
        self.exclude = exclude
        self.dbt_bin = dbt_bin
        self.verbose = verbose
        self.output_encoding = output_encoding

    def _dump_vars(self):
        # The dbt `vars` parameter is defined using YAML. Unfortunately the standard YAML library
        # for Python isn't very good and I couldn't find an easy way to have it formatted
        # correctly. However, as YAML is a super-set of JSON, this works just fine.
        return json.dumps(self.vars)

    def run_cli(self, command):
        """
        Run the dbt cli
        """

        dbt_cmd = [self.dbt_bin, command]

        if self.profiles_dir is not None:
            dbt_cmd.extend(['--profiles-dir', self.profiles_dir])

        if self.target is not None:
            dbt_cmd.extend(['--target', self.target])

        if self.vars is not None:
            dbt_cmd.extend(['--vars', self._dump_vars()])

        if self.models is not None:
            dbt_cmd.extend(['--models', self.models])

        if self.exclude is not None:
            dbt_cmd.extend(['--exclude', self.exclude])

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
