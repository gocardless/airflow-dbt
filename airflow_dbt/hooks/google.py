"""Provides hooks and helper functions to allow running dbt in GCP"""

import logging
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import (
    CloudBuildHook,
)
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.utils.yaml import dump
from google.api_core.operation import Operation
from google.cloud.devtools.cloudbuild_v1 import (
    Build,
)

from airflow_dbt.hooks.base import DbtBaseHook


class DbtCloudBuildHook(DbtBaseHook):
    """
    Connects to GCP Cloud Build, creates a build config, submits it and waits
    for results.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        gcs_staging_location: str = None,
        gcp_conn_id: str = "google_cloud_default",
        env: Optional[Dict] = None,
        service_account: Optional[str] = None,
        dbt_version: str = '1.0.0',
        dbt_image: str = 'fishtownanalytics/dbt',
        dbt_project_dir: str = None,
        dbt_artifacts_dest: str = None,
    ):
        """
        Runs the dbt command in a Cloud Build job in GCP

        :param dbt_artifacts_dest: Folder in GCS destination for the artifacts.
            For example `gs://my-bucket/path/to/artifacts/`
        :type dbt_artifacts_dest: str
        :type env: dict
        :param env: If set, passed to the dbt executor
        :param project_id: GCP Project ID as stated in the console
        :type project_id: str
        :param gcp_conn_id: The connection ID to use when fetching connection
            info.
        :type gcp_conn_id: str
        :param gcs_staging_location: Where to store the sources to be fetched
            later by the cloud build job. It should be the GCS url of a folder.
            For example: `gs://my-bucket/stored. A sub-folder will be generated
            to avoid collision between possible different concurrent runs.
        :type gcs_staging_location: str
        :param dbt_version: the DBT version to be fetched from dockerhub.
        Defaults to '1.0.0'. It represents the image tag. So it must also be
        a tag for your custom Docker dbt image if you provide one.
        :type dbt_version: str
        :param service_account: email for the service account. If set must be
            accompanied by the project_id


        """
        staging_bucket, staging_blob = _parse_gcs_url(gcs_staging_location)
        # we have provided something similar to
        # 'gs://<staging_bucket>/<staging_blob.tar.gz>'
        if Path(staging_blob).suffix not in ['.gz', '.gzip', '.zip']:
            raise AirflowException(
                f'The provided blob "{staging_blob}" to a compressed file '
                'does not have the right extension ".tar.gz" or ".gzip"'
            )
        # gcp config
        self.gcs_staging_bucket = staging_bucket
        self.gcs_staging_blob = staging_blob
        self.cloud_build_hook = CloudBuildHook(gcp_conn_id=gcp_conn_id)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id or self.cloud_build_hook.project_id
        self.service_account = service_account
        # dbt config
        self.dbt_version = dbt_version
        self.dbt_image = dbt_image
        self.dbt_project_dir = dbt_project_dir
        self.dbt_artifacts_dest = dbt_artifacts_dest

        super().__init__(env=env)

    def get_conn(self) -> Any:
        """Returns the cloud build connection, which is a gcp connection"""
        return self.cloud_build_hook.get_conn()

    def _get_cloud_build_config(self, dbt_cmd: List[str]) -> Dict:
        cloud_build_config = {
            'steps': [{
                'name': f'{self.dbt_image}:{self.dbt_version}',
                'entrypoint': dbt_cmd[0],
                'args': dbt_cmd[1:],
                'env': [f'{k}={v}' for k, v in self.env.items()],
            }],
            'source': {
                'storage_source': {
                    "bucket": self.gcs_staging_bucket,
                    "object_": self.gcs_staging_blob,
                }
            },
            'options': {
                # default is legacy and its behaviour is subject to change
                'logging': 'GCS_ONLY',
            },
            'logs_bucket': self.gcs_staging_bucket,
        }

        if self.service_account:
            cloud_build_config['service_account'] = (
                f'projects/{self.project_id}/serviceAccounts/'
                f'{self.service_account}'
            )

        if self.dbt_artifacts_dest:
            # ensure the path ends with a slash as it should if it's a folder
            gcs_dest_url = self.dbt_artifacts_dest.lstrip('/') + '/'
            artifacts_step = {
                'name': 'gcr.io/cloud-builders/gsutil',
                'args': [
                    '-m', 'cp', '-r',
                    f'{self.dbt_project_dir}/target/**',
                    gcs_dest_url
                ]
            }
            cloud_build_config['steps'].append(artifacts_step)

        return cloud_build_config

    def run_dbt(self, dbt_cmd: List[str]):
        """
         Run the dbt command. In version 5 of the providers

         :param dbt_cmd: The dbt whole command to run
         :type dbt_cmd: List[str]
         """
        # See: https://cloud.google.com/cloud-build/docs/api/reference/rest
        # /v1/projects.builds
        cloud_build_config = self._get_cloud_build_config(dbt_cmd)
        logging.info(
            f'Running the following cloud build'
            f' config:\n{dump(cloud_build_config)}'
        )

        try:
            cloud_build_client = self.get_conn()

            self.log.info("Start creating build.")

            operation: Operation = cloud_build_client.create_build(
                request={
                    'project_id': self.project_id,
                    'build': cloud_build_config
                }
            )
            # wait for the operation to complete
            operation.result()

            result_build: Build = operation.metadata.build

            self.log.info(
                f"Build has been created: {result_build.id}.\n"
                f'Build logs available at: {result_build.log_url} and the '
                f'file gs://{result_build.logs_bucket}/log-'
                f'{result_build.id}.txt'
            )

            # print logs from GCS
            with GCSHook().provide_file(
                bucket_name=result_build.logs_bucket,
                object_name=f'log-{result_build.id}.txt',
            ) as log_file_handle:
                clean_lines = [
                    line.decode('utf-8').strip()
                    for line in log_file_handle if line
                ]
                log_block = '\n'.join(clean_lines)
                hr = '-' * 80
                logging.info(
                    f'Logs from the build {result_build.id}:\n'
                    f'{hr}\n'
                    f'{log_block}\n'
                    f'{hr}'
                )
            return result_build
        except Exception as ex:
            traceback.print_exc()
            raise AirflowException("Exception running the build: ", str(ex))

    def on_kill(self):
        """Stopping the build is not implemented until google providers v6"""
        raise NotImplementedError
