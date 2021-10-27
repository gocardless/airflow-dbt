from typing import Dict, Optional

from airflow.models import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow_dbt.operators import DbtBaseOperator
from airflow_dbt.dbt_command_config import DbtCommandConfig
from airflow_dbt.hooks import DbtCloudBuildHook


class CloudBuildLogsLink(BaseOperatorLink):
    """Add a link to the logs generated from a build in cloud build"""
    name = "Cloud Build Logs"

    def get_link(self, operator, _):
        """Returns the log url for the cloud build logs stored as class prop"""
        return operator.log_url


class CloudBuildLinkPlugin(AirflowPlugin):
    """Registers the extra links"""
    name = "cloud_build_link_plugin"
    operator_extra_links = [CloudBuildLogsLink()]


class DbtCloudBuildOperator(DbtBaseOperator):
    """Uses the CloudBuild Hook to run the operation in there by default"""

    template_fields = [
        'env', 'dbt_bin', 'command', 'config', 'gcs_staging_location',
        'project_id', 'dbt_version', 'service_account'
    ]

    operator_extra_links = [CloudBuildLogsLink]

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
