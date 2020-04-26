import logging

import pandas as pd

from anyway.data_adapters.cbs import parse_data
from anyway.data_adapters.cbs.config import paths


def read_data(excel_path, rows=None):
    df = pd.read_csv(excel_path, encoding='ISO-8859-8', nrows=rows)
    logging.info(excel_path + " has been loaded")
    return df


def load_data(df, path):
    df.to_csv(path, encoding='ISO-8859-8', index=False)


def update_cbs_general():
    # extract
    raw_df = read_data(paths.CBS_GENERAL_DATA_PATH, 200)
    column_mapping_df = read_data(paths.CBS_FIELDS_CODES_PATH)
    street_mapping_df = read_data(paths.CBS_STREET_CODES_PATH)
    non_urban_junction_mapping_df = read_data(paths.NON_URBAN_CODES_PATH)
    data_mapping_df = read_data(paths.CBS_CODES_PATH)
    # transform
    parsed_df = parse_data.parse_general_cbs_data(raw_df, column_mapping_df, data_mapping_df,
                                                  non_urban_junction_mapping_df, street_mapping_df)
    # load
    load_data(parsed_df, paths.CBS_OUTPUT_GENERAL_DATA_PATH)


def update_cbs_involved():
    # extract
    raw_df = read_data(paths.CBS_INVOLVED_DATA_PATH, 200)
    column_mapping_df = read_data(paths.CBS_FIELDS_CODES_PATH)
    data_mapping_df = read_data(paths.CBS_CODES_PATH)
    # transform
    parsed_df = parse_data.parse_involved_cbs_data(raw_df, column_mapping_df, data_mapping_df)
    # load
    load_data(parsed_df, paths.CBS_OUTPUT_INVOLVED_DATA_PATH)


def update_cbs_vehicles():
    # extract
    raw_df = read_data(paths.CBS_VEHICLES_DATA_PATH, 200)
    column_mapping_df = read_data(paths.CBS_FIELDS_CODES_PATH)
    data_mapping_df = read_data(paths.CBS_CODES_PATH)
    # transform
    parsed_df = parse_data.parse_vehicles_cbs_data(raw_df, column_mapping_df, data_mapping_df)
    # load
    load_data(parsed_df, paths.CBS_OUTPUT_VEHICLES_DATA_PATH)


def main():
    update_cbs_general()
    update_cbs_involved()
    update_cbs_vehicles()


if __name__ == "__main__":
    main()
