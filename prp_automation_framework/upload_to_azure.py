from utils.utils import *


def read_file_chunks(file_path, chunk_size):
    # Upload the file to the landing zone folder in chunks
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            yield chunk


def get_blog_service_client(account_url):
    default_credential = DefaultAzureCredential()
    # azure_details_dict = get_env_vars()
    return BlobServiceClient(account_url, credential=default_credential)


def get_blob_client_object(storage_service_client, container, blob):
    return storage_service_client.get_blob_client(container=container, blob=blob)


def upload_to_azure(config=None):
    """connect blob using DefaultAzureCredential. """
    if config is None:
        config = read_config(base_folder=os.path.dirname(__file__),
                             key="upload_to_azure")

    if config["file"] is None:
        logging.error("\t--file_name is neither passed in parameter nor in config json", exc_info=True)
        raise Exception("\t--file_name is neither passed in parameter nor in config json")

    logging.info(f"\tAzure Container- {config.get('container_name')}")

    account_url = f"https://{config.get('storage_account_name')}.blob.core.windows.net"
    storage_service_client = get_blog_service_client(account_url)

    # Create a blob client using the local file name as the name for the blob
    chunk_size = int(config.get("chunk_size_mb")) * 1024 * 1024  # chunk_size_mb MB
    source_file_path = os.path.join(os.path.dirname(__file__),
                                    config.get("source_folder"),
                                    config.get("file"))
    blob_file_raw = config.get("blob_folder_raw") + "/" + config.get("file")
    logging.info(f"\tSource file_path  {source_file_path}")
    logging.info(f"\tBlob file_path  {blob_file_raw}")

    try:
        blob_client_raw = get_blob_client_object(storage_service_client,
                                                 container=config.get("container_name"),
                                                 blob=blob_file_raw)

        blob_client_raw.upload_blob(read_file_chunks(source_file_path, chunk_size))
        logging.info("\tFile has been successfully uploaded to the landing zone folder.")

        # encrypted blob
        blob_file_encrypted = config.get("blob_folder_encrypted") + "/" + config.get("file")
        blob_client_encrypt = get_blob_client_object(storage_service_client,
                                                     container=config.get("container_name"),
                                                     blob=blob_file_encrypted)

        # process blob
        blob_file_process = config.get("blob_folder_process") + "/" + config.get("file")
        blob_client_process = get_blob_client_object(storage_service_client,
                                                     container=config.get("container_name"),
                                                     blob=blob_file_process)
        # error blob
        blob_file_error = config.get("blob_folder_error") + "/" + config.get("file")
        blob_client_error = get_blob_client_object(storage_service_client,
                                                   container=config.get("container_name"),
                                                   blob=blob_file_error)

    except Exception as e:
        logging.error(f"\tAn error occurred while uploading the file: {e}", exc_info=True)
        raise Exception(f"\tAn error occurred while uploading the file: {e}")

    sleep(30)

    encrypt_exist = blob_client_encrypt.exists()
    logging.info(f"File exist in Encrypted is  {encrypt_exist}  Location {blob_file_encrypted}")

    # check in encrypt folder
    while encrypt_exist is False:
        logging.info(f"File exist in Encrypted is {encrypt_exist} Re-Checking after 5 sec")
        sleep(5)
        encrypt_exist = blob_client_encrypt.exists()

    if encrypt_exist:
        logging.info(f"File Moved to Encrypted folder")

    sleep(90)

    process_exist = blob_client_process.exists()
    error_exist = blob_client_error.exists()
    logging.info(f"File exist in processed is  {process_exist} or error is {error_exist} ")

    while process_exist is False and error_exist is False:
        logging.info(f"File exist in processed is  {process_exist} or error is {error_exist} Re-Checking after 30 sec")
        sleep(30)
        process_exist = blob_client_process.exists()
        error_exist = blob_client_error.exists()

    if process_exist:
        logging.info(f"\tFile moved to processed folder. Location {blob_file_process}")
        status = 0
    elif error_exist:
        logging.info(f"\tFile moved to error folder. Location {blob_file_error}")
        status = 1
    else:
        logging.error("File not found in processed or error folder")
        raise Exception("File not found in processed or error folder")

    details_dict = {"source_file": config.get("file"),
                    "azure_file": blob_client_raw,
                    "Status": "SUCCESS"}

    base_html_content = add_segment_content(tag="upload_to_azure",
                                            value_dict=details_dict,
                                            base_content=config.get("base_html_content", None))

    return status, {"base_html_content": base_html_content}


if __name__ == "__main__":
    logging.basicConfig(filename='logs/upload_to_azure.log',
                        filemode='w', encoding='utf-8', level=logging.INFO)
    logging.info("-------------------------Upload to Azure START----------------------------------")
    start_time = datetime.now()
    logging.info(f"Upload start_time {start_time}")
    upload_to_azure()
    time_taken = datetime.now()-start_time
    logging.info(f"Upload Time taken {time_taken}")
    logging.info("-------------------------Upload to Azure END----------------------------------")


# def get_blob_client_object(container, blob):
#     # Connect to Azure Storage account
#     print(f"Azure Container- {container}")
#     print(f"Azure blob path- {blob}")
#     try:
#         blob_service_client = BlobServiceClient.from_connection_string(connect_str)
#         blob_client = blob_service_client.get_blob_client(container=container,
#                                                           blob=blob)
#         return blob_client
#     except Exception as e:
#         print("An error occurred while creating client blob_client:", e)

# def upload_to_azure_old():
#     """Connect blob using connection string"""
#     config = read_config(base_folder=os.path.dirname(__file__),
#                          key="upload_config")
#
#     chunk_size = int(config.get("chunk_size_mb")) * 1024 * 1024  # chunk_size_mb MB
#     source_file_path = os.path.join(os.path.dirname(__file__),
#                                     config.get("source_folder"),
#                                     config.get("file"))
#     print("file_path ", source_file_path)
#     try:
#         blob_client = get_blob_client_object(container=config.get("container_name"),
#                                              blob=config.get("blob_file_path"))
#         blob_client.upload_blob(read_file_chunks(source_file_path, chunk_size))
#         print("File has been successfully uploaded to the landing zone folder.")
#     except Exception as e:
#         print("An error occurred while uploading the file:", e)
