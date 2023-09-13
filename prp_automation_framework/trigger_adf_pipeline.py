from utils.utils import *


def get_adf_client(subscription_id):
    default_credential = DefaultAzureCredential()
    return DataFactoryManagementClient(default_credential, subscription_id)


def trigger_adf_pipeline(config=None):
    if config is None:
        config = read_config(base_folder=os.path.dirname(__file__),
                             key="trigger_adf_pipeline")

    # Read Environment Variables
    azure_details_dict = get_env_vars()
    subscription_id = azure_details_dict.get("AZURE_SUBSCRIPTION_ID")
    resource_group_name = azure_details_dict.get("RESOURCE_GROUP_NAME")

    # Initialize the Azure Data Factory Management client
    adf_client = get_adf_client(subscription_id)

    # Reading HTMl content
    base_html_content = config.get("base_html_content", None)
    x_comm = {}
    for run_pipeline in config.get("run_pipelines", None):
        # Trigger the pipeline run
        logging.info(f"\tTriggering Pipeline {run_pipeline.get('pipeline_name')}")
        run_response = adf_client.pipelines.create_run(
            resource_group_name=resource_group_name,
            factory_name=config.get("factory_name", None),
            pipeline_name=run_pipeline.get("pipeline_name", None),
            parameters=run_pipeline.get("parameters", None))
        sleep(1)
        logging.info(f"\tPipeline run triggered. Run ID: {run_response.run_id}")
        # Track Status until Completed or Failed
        while True:
            pipeline_run = adf_client.pipeline_runs.get(resource_group_name,
                                                        config.get("factory_name", None),
                                                        run_response.run_id)
            pipeline_status = pipeline_run.status
            logging.info(f"\tPipeline run status: {pipeline_status} ")
            if pipeline_status == "Succeeded":
                status = 0
                break
            elif pipeline_status == "Failed":
                status = 1
                logging.info(f"Pipeline Failed name {run_pipeline.get('pipeline_name')} run_id- {run_response.run_id}")
                break
            else:
                sleep(run_pipeline.get("check_status_freq") * 60)
                logging.info(f"""\tRe-Checking Status after {run_pipeline.get('check_status_freq') * 60}s""")

        details_dict = {"pipeline_name": run_pipeline.get("pipeline_name", None),
                        "run_id": run_response.run_id,
                        "Status": pipeline_run.status}

        base_html_content = add_segment_content(tag="trigger_adf_pipeline",
                                                value_dict=details_dict,
                                                base_content=base_html_content)

        x_comm.update({run_pipeline.get("run_id_key"): run_response.run_id, "base_html_content": base_html_content})

    logging.info(f"Passing x_comm values to config {x_comm} and status {status}")

    return status, x_comm


if __name__ == "__main__":
    logging.basicConfig(filename='logs/trigger_adf_pipeline.log',
                        filemode='w', encoding='utf-8', level=logging.INFO)
    logging.info("-------------------------Trigger Pipeline START----------------------------------")
    start_time = datetime.now()
    trigger_adf_pipeline()
    logging.info("-------------------------Trigger Pipeline END----------------------------------")


# from azure.mgmt.datafactory.models import RunFilterParameters

# from azure.mgmt.datafactory import DataFactoryManagementClient
# from azure.identity import ClientSecretCredential
# from azure.mgmt.resource import ResourceManagementClient
# def trigger_adf_pipeline2():
#     credentials = ClientSecretCredential(client_id=principal_id,
#                                          client_secret='<service principal key>',
#                                          tenant_id=tenant_id)
#     resource_client = ResourceManagementClient(credentials, subscription_id)
#     adf_client = DataFactoryManagementClient(credentials, subscription_id)
#     # Create a pipeline with the copy activity
#     df_name = 'prp-datafactory-ppe'
#     p_name = ''
#     params_for_pipeline = {}
#     # Create a pipeline run
#     run_response = adf_client.pipelines.create_run(rg_name, df_name, p_name, parameters={})

