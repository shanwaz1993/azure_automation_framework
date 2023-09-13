from data_generator import data_generator
from upload_to_azure import upload_to_azure
from trigger_adf_pipeline import trigger_adf_pipeline
from validations import validations
from utils.utils import *
from datetime import datetime


class PdpAutomation:
    def __init__(self, config=None, pdp_flow=None):
        if config is None:
            logging.info("Reading config from file")
            self.config = read_config(base_folder=os.path.dirname(__file__))
            if self.config is None:
                logging.critical("No config Defined from file or parameter")
                raise Exception(f"No config Defined from file or parameter")
            else:
                logging.info(f"\tInput Config {self.config}")
        else:
            self.config = config

        if pdp_flow is None:
            self.pdp_flow = self.config["pdp_flow"]
        else:
            self.pdp_flow = pdp_flow

        # logging.info(f"Full Config  {self.config}")

    def run_task(self, t, config):
        try:
            status, x_comm = t(config)
            if status != 0:
                logging.error(f"Non zero status returned from task {t} ")
        except Exception as e:
            logging.error(f"Raised error in task {t} - {e}", exc_info=True)
            raise Exception(f"task {t} got some error")
        return status, x_comm

    def run_pdp_flow(self):
        x_comm_all = {}
        for task in self.pdp_flow:
            config = self.config[task]

            if self.config.get("file", None) is not None:
                config["file"] = self.config["file"]
                file_name = self.config["file"]
            elif config.get("file", None) is not None:
                file_name = config["file"]
                logging.info(f"\tTaking file_name from Defined Config File for task {task} is {config['file']}")
            else:
                logging.error("--file_name is neither passed in parameter nor in config json", exc_info=True)
                raise Exception("\t--file_name is neither passed in parameter nor in config json")
            try:
                config.update(x_comm_all)
                logging.info("\n\t------------------------------------------")
                logging.info(f"\tTASK {task.upper()} STARTED RUNNING")
                status, x_comm = self.run_task(eval(task), config)
                logging.info(f"\tTASK {task.upper()}  COMPLETED")
                x_comm_all.update(x_comm)
            except Exception as e:
                raise Exception(f"\tError while Running task {task} {e}")

        report_path = get_file_path(os.path.dirname(__file__),
                                    "output_data",
                                    "report_"+file_name.replace("csv", "html"))

        if config.get("html_report",None) is not None:
            write_to_html(report_path, x_comm_all.get("base_html_content"))
            logging.info(f"\tHTML Report Generated at {report_path}")

        return status


if __name__ == "__main__":
    logging.basicConfig(filename='logs/pdp_framework.log',
                        filemode='w', encoding='utf-8', level=logging.INFO)
    logging.info("-------------------------PDP Framework START----------------------------------")
    start_time = datetime.now()
    PdpAutomation().run_pdp_flow()
    logging.info(f"PDP flow- Total Time taken {datetime.now()-start_time}")
    logging.info("-------------------------PDP Framework END----------------------------------")
