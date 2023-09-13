import pytest
from pytest_metadata.plugin import metadata_key
import pytest_html
from datetime import datetime
import os


def pytest_html_report_title(report):
    report.title = "PDP ExtraHours Report"


def pytest_html_results_summary(prefix, summary, postfix):
    prefix.extend([f"This report is generated as part of pdp automation ExtraHours on {datetime.now()}"])


# @pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    # to remove environment section
    # config._metadata = None
    if not os.path.exists('../test_report'):
        os.makedirs('../test_report')
    if config.option.htmlpath is not None and config.option.markexpr is not None:
        config.option.htmlpath = 'test_report/' + (config.option.htmlpath).replace(".html", "") + "_" + (config.option.markexpr).replace(" ", "_") + "_" + datetime.now().strftime("%d-%m-%YT%H-%M-%S") + ".html"

