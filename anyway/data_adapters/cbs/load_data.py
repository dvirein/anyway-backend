import logging

import pandas as pd

from anyway.data_adapters.cbs import parse_data
from anyway.data_adapters.cbs.config import paths
from anyway.data_adapters.cbs.config.paths import cbs_tables_config
from anyway.data_adapters.cbs.models.CbsTable import CbsTable
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


def read_data(excel_path, rows=None, table_name='The'):
    df = pd.read_csv(excel_path, encoding='ISO-8859-8', nrows=rows)
    logger.debug("extracted {} data from {}".format(table_name, excel_path))
    return df


column_mapping_df = read_data(paths.CBS_FIELDS_CODES_PATH)
data_mapping_df = read_data(paths.CBS_CODES_PATH)


def load_data(df, path, table_name='The'):
    df.to_csv(path, encoding='ISO-8859-8', index=False)
    logger.debug("{} data loaded to {}".format(table_name, path))


def update_cbs_general():
    raw_df = read_data(paths.CBS_GENERAL_DATA_PATH, 200)
    street_mapping_df = read_data(paths.CBS_STREET_CODES_PATH)
    non_urban_junction_mapping_df = read_data(paths.CBS_NON_URBAN_CODES_PATH)
    parsed_df = parse_data.parse_general_cbs_data(raw_df, column_mapping_df, data_mapping_df,
                                                  non_urban_junction_mapping_df, street_mapping_df)
    load_data(parsed_df, paths.CBS_OUTPUT_GENERAL_DATA_PATH)


def update_cbs_table_data(table_config):
    raw_df = read_data(table_config['input_path'], 200, table_config['table_type'])
    cbs_table = CbsTable(raw_df, column_mapping_df, data_mapping_df)
    cbs_table.parsed_df = parse_data.parse_cbs_data(cbs_table)
    load_data(cbs_table.parsed_df, table_config['output_path'], table_config['table_type'])


def update_all_tables(cbs_tables_config):
    for table_config in cbs_tables_config:
        if table_config['table_type'] == 'general':
            update_cbs_general()
        else:
            update_cbs_table_data(table_config)


def main():
    update_all_tables(cbs_tables_config)


if __name__ == "__main__":
    main()
