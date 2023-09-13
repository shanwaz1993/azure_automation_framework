from pdp_framework import PdpAutomation
from utils.utils import *

file_prefix = "PRP_ExtraHoursAPI_ExtraHours"
existing_bronze_silver_run_id = '407daf08-6928-4ac2-9f53-6206fcf20c3d'


# Tested
@pytest.mark.smoke
def test_pdp_upload_processed():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with correct file\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0


# Tested
@pytest.mark.smoke
def test_pdp_upload_name_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file name\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    # wrong File name
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}s_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                             dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                             random_number=random_number)

    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 1


# Tested
@pytest.mark.smoke
def test_pdp_upload_extn_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file extension\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    # Wrong File Extension
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.txt".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)

    config["pdp_flow"] = ["data_generator", "upload_to_azure"]
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.smoke
def test_pdp_upload_header_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Header/Columns\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    dimension_data = [{"name": "location", "columns": ["locationUUID", "StoreNumber", "LocationInternalname"],
                       "renamed_map": {"locationUUID": "location_Uuid", "StoreNumber": "Store_Number",
                      "LocationInternalname": "Store_Name"}, "records": 50},
                      {"name": "skills", "columns": ["skillCode", "skillName"],
                       "renamed_map": {"skillCode": "department_Id", "skillName": "Department_Name"}, "records": 50},
                      {"name": "colleague", "columns": ["colleagueUUID"], "renamed_map": {"colleagueUUID": "updatedBy"}, "records": 50}
                      ]   # Renamed columns with wrong name

    output_schema = ["extraHourId", "status", "location_Uuid", "department_Id", "assignedTo",
                     "startDateTime", "endDateTime", "updatedTimestamp",
                     "updatedBy", "createdTimestamp", "publishedTimestamp"]
    kpi_calculation = None
    config["data_generator"]["dimension_data"] = dimension_data
    config["data_generator"]["output_schema"] = output_schema
    config["data_generator"]["kpi_calculation"] = kpi_calculation
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.smoke
def test_pdp_upload_content_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Content\n""")
    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure"]

    config["data_generator"]["dimension_data"] = [{"name": "location", "columns": ["locationUUID", "StoreNumber",
                                                                                   "LocationInternalname"],
                                                   "renamed_map": {"locationUUID": "location_Uuid",
                                                                   "StoreNumber": "Store_Number",
                                                                   "LocationInternalname": "Store_Name"},
                                                   "records": 50},
                                                  {"name": "skills", "columns": ["skillCode", "skillName"],
                                                   "renamed_map": {"skillCode": "department_Id",
                                                                   "skillName": "Department_Name"},
                                                   "records": 50},
                                                  {"name": "colleague", "columns": ["colleagueUUID"],
                                                   "renamed_map": {"colleagueUUID": "updatedBy"},
                                                   "records": 50}
                                                 ]

    config["data_generator"]["output_schema"] = ["extraHourId", "status", "location_Uuid", "department_Id",
                                                 "assignedTo", "startDateTime", "endDateTime", "updatedTimestamp",
                                                 "updatedBy", "createdTimestamp"]   # Removed one column from file

    config["data_generator"]["kpi_calculation"] = None
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1


# Tested
@pytest.mark.smoke
def test_pdp_upload_protected_error():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours Upload data to azure blob with incorrect file Header/Columns\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    random_number = random.randint(100, 999)
    config["file"] = "PRP_ExtraHoursAPI_ExtraHours_20230811-500.csv"
    config["pdp_flow"] = ["upload_to_azure"]

    dimension_data = [
        {"name": "location", "columns": ["locationUUID"], "renamed_map": {"locationUUID": "location_Uuid"},
         "records": 50},
        {"name": "skills", "columns": ["skillCode"], "renamed_map": {"skillCode": "department_Id"}, "records": 50},
        {"name": "colleague", "columns": ["colleague_uuid"], "renamed_map": {"colleague_uuid": "updatedBy"},
         "records": 50}]  # Renamed columns with wrong name

    output_schema = ["extraHourId", "status", "location_Uuid", "department_Id", "assignedTo",
                     "startDateTime", "endDateTime", "updatedTimestamp",
                     "updatedBy", "createdTimestamp", "publishedTimestamp"]

    config["data_generator"]["dimension_data"] = dimension_data
    config["data_generator"]["output_schema"] = output_schema
    config["data_generator"]["kpi_calculation"] = None
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 1
