import logging

import pandas as pd

from anyway.data_adapters.cbs import parse_data
from anyway.data_adapters.cbs.config import paths
from anyway.data_adapters.cbs.utils.logger import CustomFormatter


# TODO: add many logs(some only for debbug mode)
# TODO: add encoding='ISO-8859-8' to config


# create logger with 'spam_application'
logger = logging.getLogger("Anyway")
logger.setLevel(logging.DEBUG)
# create console handler with a higher log level
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def read_data(excel_path, rows=None):
    df = pd.read_csv(excel_path, encoding='ISO-8859-8', nrows=rows)
    logger.debug("extracted data from {}".format(excel_path))
    return df


column_mapping_df = read_data(paths.CBS_FIELDS_CODES_PATH)
data_mapping_df = read_data(paths.CBS_CODES_PATH)


def load_data(df, path):
    df.to_csv(path, encoding='ISO-8859-8', index=False)
    logger.debug("data loaded to {}".format(path))


def update_cbs_general():
    raw_df = read_data(paths.CBS_GENERAL_DATA_PATH, 200)
    street_mapping_df = read_data(paths.CBS_STREET_CODES_PATH)
    non_urban_junction_mapping_df = read_data(paths.CBS_NON_URBAN_CODES_PATH)
    parsed_df = parse_data.parse_general_cbs_data(raw_df, column_mapping_df, data_mapping_df,
                                                  non_urban_junction_mapping_df, street_mapping_df)
    load_data(parsed_df, paths.CBS_OUTPUT_GENERAL_DATA_PATH)


# TODO: combine 2 functions below (diffrent params for input)
def update_cbs_involved():
    raw_df = read_data(paths.CBS_INVOLVED_DATA_PATH, 200)
    parsed_df = parse_data.parse_involved_cbs_data(raw_df, column_mapping_df, data_mapping_df)
    load_data(parsed_df, paths.CBS_OUTPUT_INVOLVED_DATA_PATH)


def update_cbs_vehicles():
    raw_df = read_data(paths.CBS_VEHICLES_DATA_PATH, 200)
    parsed_df = parse_data.parse_vehicles_cbs_data(raw_df, column_mapping_df, data_mapping_df)
    load_data(parsed_df, paths.CBS_OUTPUT_VEHICLES_DATA_PATH)


def main():
    update_cbs_general()
    update_cbs_involved()
    update_cbs_vehicles()


if __name__ == "__main__":
    main()
