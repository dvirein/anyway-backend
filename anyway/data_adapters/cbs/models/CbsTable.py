class CbsTable:

    def __init__(self, raw_df, column_mapping_df, data_mapping_df, parsed_df=None):
        self.raw_df = raw_df
        self.column_mapping_df = column_mapping_df
        self.data_mapping_df = data_mapping_df
        self.parsed_df = parsed_df
