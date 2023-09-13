from pdp_framework import PdpAutomation
from utils.utils import *


file_prefix = "PRP_ExtraHoursAPI_ExtraHours"


# Tested
@pytest.mark.endtoend
# @pytest.mark.regression
def test_pdp_flow_all():
    logging.info("""\n\t---------------Test Description---------------
    \n\tValidate ExtraHours PDP Flow, data_generator,upload_to_azure,trigger_adf_pipeline,validations\n""")

    config_file = "{file_prefix}_config.json".format(file_prefix=file_prefix)
    config = read_config(base_folder=os.path.dirname(__file__)+"..\\..\\",
                         config_file=config_file)
    random_number = random.randint(100, 999)
    config["file"] = "{file_prefix}_{dt_nodash}-{random_number}.csv".format(file_prefix=file_prefix,
                                                                            dt_nodash=datetime.now().strftime("%Y%m%d"),
                                                                            random_number=random_number)
    config["pdp_flow"] = ["data_generator", "upload_to_azure",
                          "trigger_adf_pipeline", "validations"]
    config["validations"]["validation_flow"] = ["bronze", "silver", "gold"]
    status = PdpAutomation(config=config).run_pdp_flow()
    assert status == 0
