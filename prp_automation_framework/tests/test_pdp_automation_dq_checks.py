from pdp_framework import PdpAutomation
from utils.utils import *


file_prefix = "PRP_ExtraHoursAPI_ExtraHours"

existing_bronze_silver_run_id = 'notrequired'  # Define if running withrunid mark
existing_data_file = 'notrequired'             # Define if running withrunid mark


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_skills_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Skills_ID not in (select distinct skillCode 
                                                                              from delta_silver.skills)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_skills_id_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Skills_ID is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_skills_id():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for skills_id/department_id\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(Skills_ID)!=7""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_department_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """department_name not in (select distinct skillName 
                                                                              from delta_silver.skills)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_department_name_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """department_name is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_department_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for department_name\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(department_name)<1""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_extra_hours_date_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for extra_hours_date\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """Extra_Hours_date is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_extra_hours_date():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for extra_hours_date\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """date(Extra_Hours_date) is null""",
                                      "type": [{"name": "count", "query": None}]}
                                     ]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_location_uuid():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """location_uuid not in (select distinct locationUuid  
                                                  from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_location_uuid_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """location_uuid is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_location_uuid():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_id/location_uuid\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(location_uuid)!=36""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_store_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_name not in (select 
                                       distinct LocationInternalname  
                                       from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_store_name_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_name is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_uniqueness_store_name():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table uniqueness check for store_name/LocationInternalname\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """cast(store_name as string) is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_integrity_store_number():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table integrity check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_number not in (select 
                                       distinct StoreNumber  
                                       from delta_silver.location)""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_store_number_null():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table null check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """store_number is null""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0


@pytest.mark.dqcheck
@pytest.mark.validation
@pytest.mark.gold
def test_pdp_gold_table_validity_store_number():
    logging.info("""\t---------------Test Description---------------
    \n\tValidate ExtraHours gold table Validity check for store_number/StoreNumber\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)

    config["file"] = existing_data_file

    config["pdp_flow"] = ["validations"]
    config["validations"]["input_count"] = 0
    config["validations"]["validation_flow"] = ["gold"]
    config["validations"]["bronze_silver_run_id"] = existing_bronze_silver_run_id

    config["validations"]["gold"] = [{"db_table": "delta_gold.extrahours",
                                      "where": """length(store_number)!=4""",
                                      "type": [{"name": "count", "query": None}]}]

    status = PdpAutomation(config=config).run_pdp_flow()

    assert status == 0
