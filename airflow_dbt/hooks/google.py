import logging
import os
import pprint
import tarfile
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.providers.google.cloud.hooks.gcs import (
    GCSHook, _parse_gcs_url,
)
from airflow.providers.google.get_provider_info import get_provider_info
from packaging import version

from .dbt_hook import DbtBaseHook

# Check we're using the right google provider version. As composer is the
# most brad used Airflow installation we will default to the latest version
# composer is using
google_providers_version = get_provider_info().get('versions')[0]
v_min = version.parse('5.0.0')
v_max = version.parse('6.0.0')
v_provider = version.parse(google_providers_version)
if not v_min <= v_provider < v_max:
    raise Exception(
        f'The provider "apache-airflow-providers-google" version "'
        f'{google_providers_version}" is not compatible with the current API. '
        f'Please install a compatible version in the range [{v_min}, {v_max})"'
    )


class DbtCloudBuildHook(DbtBaseHook):
    """
    Runs the dbt command in a Cloud Build job in GCP

    :type dir: str
    :param dir: Optional, if set the process considers that sources must be
        uploaded prior to running the DBT job
    :type env: dict
    :param env: If set, passed to the dbt executor
    :param dbt_bin: The `dbt` CLI. Defaults to `dbt`, so assumes it's on your
        `PATH`
    :type dbt_bin: str

    :param project_id: GCP Project ID as stated in the console
    :type project_id: str
    :param timeout: Default is set in Cloud Build itself as ten minutes. A
        duration in seconds with up to nine fractional digits, terminated by
        's'. Example: "3.5s"
    :type timeout: str
    :param wait: Waits for the cloud build process to finish. That is waiting
        for the DBT command to finish running or run asynchronously
    :type wait: bool
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param gcs_staging_location: Where to store the sources to be fetch later
        by the cloud build job. It should be the GCS url for a folder. For
        example: `gs://my-bucket/stored. A sub-folder will be generated to
        avoid collision between possible different concurrent runs.
    :param gcs_staging_location: str
    :param dbt_version: the DBT version to be fetched from dockerhub. Defaults
        to '0.21.0'
    :type dbt_version: str
    """

    def __init__(
        self,
        project_id: str,
        # dir: str = None,
        gcs_staging_location: str = None,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '0.21.0',
        env: Dict = None,
        dbt_bin='',
        service_account=None,
    ):
        staging_bucket, staging_blob = _parse_gcs_url(gcs_staging_location)
        # we have provided something similar to
        # 'gs://<staging_bucket>/<staging_blob.tar.gz>'
        if not staging_blob.endswith('.tar.gz'):
            raise AirflowException(
                f'The provided blob "{staging_blob}" to a compressed file does not ' +
                f'have the right extension ".tar.gz'
            )
        self.gcs_staging_bucket = staging_bucket
        self.gcs_staging_blob = staging_blob

        self.dbt_version = dbt_version
        self.cloud_build_hook = CloudBuildHook(gcp_conn_id=gcp_conn_id)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.service_account = service_account

        super().__init__(
            # dir=dir,
            env=env,
            dbt_bin=dbt_bin
        )

    def get_conn(self) -> Any:
        """Returns the cloud build connection, which is a gcp connection"""
        return self.cloud_build_hook.get_conn()

    def upload_dbt_sources(self) -> None:
        """Upload sources from local to a staging location"""
        logging.info(
            f'Files in "{dir}" will be uploaded to GCS with the '
            f'prefix "gs://{self.gcs_staging_bucket}/{self.gcs_staging_blob}"'
        )
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)
        with \
            NamedTemporaryFile() as compressed_file, \
            tarfile.open(compressed_file.name, "w:gz") as tar:
            tar.add(self.dir, arcname=os.path.basename(self.dir))
            gcs_hook.upload(
                bucket_name=self.gcs_staging_bucket,
                object_name=self.gcs_staging_blob,
                filename=compressed_file.name,
            )

    def run_dbt(self, dbt_cmd: List[str]):
        """
         Run the dbt cli

         :param dbt_cmd: The dbt whole command to run
         :type dbt_cmd: List[str]
         """
        """See: https://cloud.google.com/cloud-build/docs/api/reference/rest
        /v1/projects.builds"""

        # if we indicate that the sources are in a local directory by setting
        # the "dir" pointing to a local path, then those sources will be
        # uploaded to the expected blob
        if self.dir is not None:
            self.upload_dbt_sources()

        cloud_build_config = {
            'steps': [{
                'name': f'fishtownanalytics/dbt:{self.dbt_version}',
                'args': dbt_cmd,
                'env': [f'{k}={v}' for k, v in self.env.items()]
            }],
            'source': {
                'storageSource': {
                    "bucket": self.gcs_staging_bucket,
                    "object": self.gcs_staging_blob,
                }
            }
        }

        if self.service_account is not None:
            cloud_build_config['serviceAccount'] = self.service_account

        cloud_build_config_str = pprint.pformat(cloud_build_config)
        logging.info(f'Running the following cloud build config:\n{cloud_build_config_str}')

        results = self.cloud_build_hook.create_build(
            body=cloud_build_config,
            project_id=self.project_id,
        )
        logging.info(
            f'Triggered build {results["id"]}. You can find the logs at '
            f'{results["logUrl"]}'
        )

    def on_kill(self):
        """Stopping the build is not implemented until google providers v6"""
        raise NotImplementedError
