from airflow.models import BaseOperatorLink
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

from airflow_dbt.hooks.google import DbtCloudBuildHook
from airflow_dbt.operators.dbt_operator import DbtBaseOperator


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

    operator_extra_links = [CloudBuildLogsLink]

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(
        self,
        gcs_staging_location: str,
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '1.0.0',
        dbt_image: str = 'fishtownanalytics/dbt',
        service_account: str = None,
        *args,
        **kwargs
    ):
        self.gcs_staging_location = gcs_staging_location
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.dbt_version = dbt_version
        self.dbt_image = dbt_image
        self.service_account = service_account

        super(DbtCloudBuildOperator, self).__init__(
            *args,
            **kwargs
        )

        self.template_fields += [
            'gcs_staging_location', 'project_id', 'dbt_version',
            'service_account'
        ]

    def instantiate_hook(self):
        """
        Instantiates a Cloud build dbt hook. This has to be done out of the
        constructor because by the time the constructor runs the params have
        not been yet interpolated.
        """
        self.dbt_hook = DbtCloudBuildHook(
            env=self.dbt_env,
            gcs_staging_location=self.gcs_staging_location,
            gcp_conn_id=self.gcp_conn_id,
            dbt_version=self.dbt_version,
            dbt_image=self.dbt_image,
            service_account=self.service_account,
            project_id=self.project_id,
        )
