from airflow.utils.decorators import apply_defaults

from airflow_dbt.hooks.google import DbtCloudBuildHook
from airflow_dbt.operators.dbt_operator import DbtBaseOperator


class DbtCloudBuildOperator(DbtBaseOperator):
    """Uses the CloudBuild Hook to run the provided dbt config"""

    template_fields = DbtBaseOperator.template_fields + [
        'gcs_staging_location', 'project_id', 'dbt_version',
        'service_account', 'dbt_artifacts_dest'
    ]

    # noinspection PyDeprecation
    @apply_defaults
    def __init__(
        self,
        gcs_staging_location: str,
        project_id: str = None,
        gcp_conn_id: str = "google_cloud_default",
        dbt_version: str = '1.3.latest',
        dbt_image: str = 'ghcr.io/dbt-labs/dbt-bigquery',
        dbt_artifacts_dest: str = None,
        service_account: str = None,
        *args,
        **kwargs
    ):
        self.dbt_artifacts_dest = dbt_artifacts_dest
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
            dbt_project_dir=self.dbt_config.get('project_dir'),
            dbt_artifacts_dest=self.dbt_artifacts_dest,
        )
