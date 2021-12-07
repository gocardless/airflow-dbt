import logging
from typing import Any, Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_build import CloudBuildHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from airflow.providers.google.get_provider_info import get_provider_info
from airflow.settings import json
from packaging import version

from airflow_dbt.hooks.base import DbtBaseHook

MIN_AIRFLOW_GOOGLE_PROVIDER_VERSION = '5.0.0'
MAX_AIRFLOW_GOOGLE_PROVIDER_VERSION = '6.0.0'


def check_google_provider_version(version_min: str, version_max: str) -> None:
    """
    Check we're using the right Google provider version. As Cloud Composer is
    the most broadly used Airflow installation we will default to the latest
    version composer is using

    :param version_min: Minimum version of the Google provider in semver format
    :type version_min: str
    :param version_max: Maximum version of the Google provider in semver format
    :type version_max: str
    """
    google_providers_version = get_provider_info().get('versions')[0]
    version_min = version.parse(version_min)
    version_max = version.parse(version_max)
    version_provider = version.parse(google_providers_version)
    if not version_min <= version_provider < version_max:
        raise Exception(
            'The provider "apache-airflow-providers-google" version "'
            f'{google_providers_version}" is not compatible with the current '
            'API. Please install a compatible version in the range '
            f'>={version_min}, <{version_max}"'
        )


check_google_provider_version(
    version_min=MIN_AIRFLOW_GOOGLE_PROVIDER_VERSION,
    version_max=MAX_AIRFLOW_GOOGLE_PROVIDER_VERSION,
)


class DbtCloudBuildHook(DbtBaseHook):
    """
    Runs the dbt command in a Cloud Build job in GCP

    :type env: dict
    :param env: If set, passed to the dbt executor
    :param project_id: GCP Project ID as stated in the console
    :type project_id: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param gcs_staging_location: Where to store the sources to be fetch later
        by the cloud build job. It should be the GCS url for a folder. For
        example: `gs://my-bucket/stored. A sub-folder will be generated to
        avoid collision between possible different concurrent runs.
    :type gcs_staging_location: str
    :param dbt_version: the DBT version to be fetched from dockerhub. Defaults
        to '0.21.0'
    :type dbt_version: str
    :param service_account: email for the service account. If set must be
        accompanied by the project_id
    """
    def __init__(

        self,
        project_id: Optional[str] = None,
        gcs_staging_location: str = None,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '0.21.0',
        env: Optional[Dict] = None,
        service_account: Optional[str] = None,
    ):
        staging_bucket, staging_blob = _parse_gcs_url(gcs_staging_location)
        # we have provided something similar to
        # 'gs://<staging_bucket>/<staging_blob.tar.gz>'
        if not staging_blob.endswith('.tar.gz'):
            raise AirflowException(
                f'The provided blob "{staging_blob}" to a compressed file does not ' +
                'have the right extension ".tar.gz"'
            )
        self.gcs_staging_bucket = staging_bucket
        self.gcs_staging_blob = staging_blob

        self.dbt_version = dbt_version
        self.cloud_build_hook = CloudBuildHook(gcp_conn_id=gcp_conn_id)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id or self.cloud_build_hook.project_id
        self.service_account = service_account

        super().__init__(env=env)

    def get_conn(self) -> Any:
        """Returns the cloud build connection, which is a gcp connection"""
        return self.cloud_build_hook.get_conn()

    def run_dbt(self, dbt_cmd: List[str]):
        """
         Run the dbt command. In version 5 of the providers

         :param dbt_cmd: The dbt whole command to run
         :type dbt_cmd: List[str]
         """
        # See: https://cloud.google.com/cloud-build/docs/api/reference/rest/v1/projects.builds
        cloud_build_config = {
            'steps': [{
                # use the official dbt docker image from dockerhub
                'name': f'fishtownanalytics/dbt:{self.dbt_version}',
                'args': dbt_cmd,
                'env': [f'{k}={v}' for k, v in self.env.items()]
            }],
            'source': {
                'storageSource': {
                    "bucket": self.gcs_staging_bucket,
                    "object": self.gcs_staging_blob,
                }
            },
            'options': {
                # default is legacy and its behaviour is subject to change
                'logging': 'GCS_ONLY',
            },
            # mandatory if using a service_account, it also is relevant as
            # transactional data
            'logsBucket': self.gcs_staging_bucket,
        }

        if self.service_account:
            service_account_path_path = f'projects/{self.project_id}/serviceAccounts/' \
                      f'{self.service_account}'
            cloud_build_config['serviceAccount'] = service_account_path_path

        cloud_build_config_str = json.dumps(cloud_build_config, indent=2)
        logging.info(
            f'Running the following cloud build'
            f' config:\n{cloud_build_config_str}'
        )

        build_results = self.cloud_build_hook.create_build(
            body=cloud_build_config,
            project_id=self.project_id,
        )

        # print logs from GCS
        build_logs_blob = f'log-{build_results["id"]}.txt'
        with GCSHook().provide_file(
            bucket_name=self.gcs_staging_bucket,
            object_name=build_logs_blob
        ) as log_file_handle:
            for line in log_file_handle:
                clean_line = line.decode('utf-8').strip()
                if not clean_line == '':
                    logging.info(clean_line)

        # print result from build
        logging.info('Build results:\n' + json.dumps(build_results, indent=2))
        # set the log_url class param to be read from the "links"
        return build_results

    def on_kill(self):
        """Stopping the build is not implemented until google providers v6"""
        raise NotImplementedError
