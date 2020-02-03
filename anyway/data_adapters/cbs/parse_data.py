import logging
import re


def parse_by_mapping(df, col_map, value_map, lang):
    for column in list(df.columns.values):
        idx = 0
        if column in col_map.keys():
            col_code = col_map[column]
            if col_code in value_map.keys():
                df[column] = df[column].fillna(-9998).astype(int)
                df[column] = df[column].astype(str)
                for row in df[column]:
                    df.at[idx, column] = value_map.get(col_map[column], {}).get(row, {}).get(lang)
                    idx += 1
    return df


def format_df(df, convert_fields, dt):
    df.columns = map(str.upper, df.columns)
    df = df.rename(columns=convert_fields)
    df = _drop_unnecessary_columns(df, convert_fields.values())
    df['dt'] = dt
    return df


def get_dt(path):
    dt_search = re.search('.*\\\H(\d{6})\d+\\\.*\.csv', path)
    return dt_search.group(1)

def generate_id_column(df, columns, new_column_name):
    # TODO: concat columns, add to DF, return
    pass


def _drop_unnecessary_columns(df, allowed_columns):
    drop_columns = [column for column in df.columns.values if column not in allowed_columns]
    if drop_columns:
        logging.warning(f"Unknown columns has been found: {drop_columns} consider add to mapping fields names")
    df = df.drop(columns=drop_columns)
    return df

