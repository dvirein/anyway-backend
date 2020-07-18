import logging

import pandas as pd

from anyway.data_adapters.cbs import parse_data
from anyway.data_adapters.cbs.config import tables_config
from anyway.data_adapters.cbs.config.tables_config import cbs_tables_config
from anyway.data_adapters.cbs.models.CbsTable import CbsTable
from anyway.data_adapters.cbs.utils.logger import CustomFormatter

# TODO: add many logs(some only for debbug mode)
# TODO: add encoding='ISO-8859-8' to config

# TODO: fix handler double rows
logger = logging.getLogger("Anyway")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)


def read_data(path, rows_number=None, table_name='undefined'):
    df = pd.read_csv(path, encoding='ISO-8859-8', nrows=rows_number)
    logger.debug("extracted data from {} {}".format(table_name, path))
    return df


# TODO: add to the relevant functions
column_mapping_df = read_data(tables_config.CBS_FIELDS_CODES_PATH)
data_mapping_df = read_data(tables_config.CBS_CODES_PATH)


def load_data(df, path, table_name='The'):
    df.to_csv(path, encoding='ISO-8859-8', index=False)
    logger.debug("{} data loaded to {}".format(table_name, path))


def update_cbs_general(table_config):
    raw_df = read_data(table_config['raw_input_path'], 200, table_config['table_type'])
    cbs_table = CbsTable(raw_df, column_mapping_df, data_mapping_df)
    street_mapping_df = read_data(tables_config.CBS_STREET_CODES_PATH)
    non_urban_junction_mapping_df = read_data(tables_config.CBS_NON_URBAN_CODES_PATH)
    parsed_df = parse_data.parse_general_cbs_data(cbs_table, table_config, non_urban_junction_mapping_df,
                                                  street_mapping_df)
    load_data(parsed_df, table_config['output_path'], table_config['table_type'])


def update_cbs_table_data(table_config):
    raw_df = read_data(table_config['raw_input_path'], 200, table_config['table_type'])
    cbs_table = CbsTable(raw_df, column_mapping_df, data_mapping_df)
    cbs_table.parsed_df = parse_data.parse_cbs_data(cbs_table, table_config)
    load_data(cbs_table.parsed_df, table_config['output_path'], table_config['table_type'])


def update_all_tables(tables_conf):
    for table_conf in tables_conf:
        if table_conf['table_type'] == 'general':
            update_cbs_general(table_conf)
        else:
            update_cbs_table_data(table_conf)


def main():
    update_all_tables(cbs_tables_config)


if __name__ == "__main__":
    main()
