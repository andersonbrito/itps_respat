import pandas as pd
import numpy as np
import argparse

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Normalize data matrix, using another matrix or constant values",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input1", required=True, help="Main matrix, used as the numerator")
    parser.add_argument("--input2", required=False, type=str,  help="Secondary matrix, with values used as denominators")
    parser.add_argument("--index1", nargs="+", required=True, type=str, help="Columns with unique identifiers in the numerator file")
    parser.add_argument("--index2", nargs="+", required=False, type=str, help="Columns with unique identifiers in the denominator file, at least one match index1")
    parser.add_argument("--rolling-average", required=False, type=int,  help="Window for rolling average conversion")
    parser.add_argument("--norm-var", required=False, type=str,  help="Single column to be used for normalization of all columns (e.g. population)")
    parser.add_argument("--rate", required=False, type=int,  help="Rate factor for normalization (e.g. 100000 habitants)")
    parser.add_argument("--min-denominator", required=False, type=int, default=0, help="Value X of rolling average window (mean at every X data points in time series)")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--output", required=True, help="TSV matrix with normalized values")
    args = parser.parse_args()

    input1 = args.input1
    input2 = args.input2
    unique_id1 = args.index1
    unique_id2 = args.index2
    rolling_avg = args.rolling_average
    norm_variable = args.norm_var
    rate_factor = args.rate
    min_denominator = args.min_denominator
    filters = args.filter
    output = args.output


    # path = '/Users/anderson/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/20210113_relatório1/results/region/'
    # input1 = path + 'combined_matrix_region_posneg_weeks.tsv'
    # input2 = path + 'combined_matrix_region_totaltests.tsv'
    # unique_id1 = ['pathogen', 'region']
    # unique_id2 = ['pathogen', 'region']
    # norm_variable = ''
    # rate_factor = ''
    # rolling_avg = 3
    # filters = 'test_result:Positive'
    # output = path + 'posrates.tsv'


    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
            # print(file)
            separator = '\t'
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] == 'csv':
            separator = ','
            df = pd.read_csv(file, encoding='utf-8', sep=separator, dtype='str')
        elif str(file).split('.')[-1] in ['xls', 'xlsx']:
            df = pd.read_excel(file, index_col=None, header=0, sheet_name=0, dtype='str')
            df.fillna('', inplace=True)
        else:
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX')
            exit()
        return df

    # open dataframe
    df = load_table(input1)
    df.fillna('', inplace=True)


    for idx in unique_id1:
        df = df[~df[idx].isin([''])]

    # filter rows
    def filter_df(df, criteria):
        print('\nFiltering rows...')
        # print(criteria)
        new_df = pd.DataFrame()
        include = {}
        for filter_value in criteria.split(','):
            filter_value = filter_value.strip()
            if not filter_value.startswith('~'):
                col, val = filter_value.split(':')[0], filter_value.split(':')[1]
                if val == '\'\'':
                    val = ''
                if col not in include:
                    include[col] = [val]
                else:
                    include[col].append(val)
        # print('Include:', include)
        for filter_col, filter_val in include.items():
            print('\t- Including only rows with \'' + filter_col + '\' = \'' + ', '.join(filter_val) + '\'')
            # print(new_df.size)
            if new_df.empty:
                df_filtered = df[df[filter_col].isin(filter_val)]
                new_df = new_df.append(df_filtered)
            else:
                new_df = new_df[new_df[filter_col].isin(filter_val)]
            # print(new_df)#.head())

        exclude = {}
        for filter_value in criteria.split(','):
            filter_value = filter_value.strip()
            if filter_value.startswith('~'):
                # print('\t- Excluding all rows with \'' + col + '\' = \'' + val + '\'')
                filter_value = filter_value[1:]
                col, val = filter_value.split(':')[0], filter_value.split(':')[1]
                if val == '\'\'':
                    val = ''
                if col not in exclude:
                    exclude[col] = [val]
                else:
                    exclude[col].append(val)
        # print('Exclude:', exclude)
        for filter_col, filter_val in exclude.items():
            print('\t- Excluding all rows with \'' + filter_col + '\' = \'' + ', '.join(filter_val) + '\'')
            if new_df.empty:
                df = df[~df[filter_col].isin(filter_val)]
                new_df = new_df.append(df)
            else:
                new_df = new_df[~new_df[filter_col].isin(filter_val)]
            # print(new_df)#.head())
        return new_df

    # load data
    if filters not in ['', None]:
        df = filter_df(df, filters)

    if input2 not in ['', None]:
        df2 = load_table(input2)
        df2.fillna('', inplace=True)
    else:
        df2 = df[unique_id1]
        norm_variable = 'norm_variable'
        unique_id2 = unique_id1
        df2[norm_variable] = 1

    # print(df2.head)
    # print(df2.columns.tolist())

    # get columns
    date_columns = []
    for column in df.columns.to_list():
        if column[0].isdecimal():
            if norm_variable in ['', None]:
                if column in df2.columns.tolist():
                    date_columns.append(column)
            else:
                date_columns.append(column)


    # set new indices
    df.insert(0, 'unique_id1', '')
    df['unique_id1'] = df[unique_id1].astype(str).sum(axis=1)
    df.insert(1, 'unique_id2', '')
    df['unique_id2'] = df[unique_id2].astype(str).sum(axis=1)

    df2.insert(0, 'unique_id2', '')
    df2['unique_id2'] = df2[unique_id2].astype(str).sum(axis=1)

    # create empty dataframes
    nondate_columns = [column for column in df.columns.to_list() if column not in date_columns]
    # print(date_columns)
    # print(nondate_columns)

    df3 = df.filter(nondate_columns, axis=1)

    # set new index
    # df.set_index(unique_id1, inplace=True)
    df2.set_index('unique_id2', inplace=True)
    df3.set_index('unique_id1', inplace=True)

    # print(df)
    # print(df2)
    # print(df3)


    # perform normalization
    for idx, row in df.iterrows():
        if rolling_avg not in [None, '']:
            rolling_window_obj = row.rolling(int(rolling_avg))
            rolling_average = rolling_window_obj.mean()
            df.loc[idx] = rolling_average

        # print('\n' + str(idx))
        id1 = str(df.loc[idx, 'unique_id1'])
        id2 = str(df.loc[idx, 'unique_id2'])
        # print(id1, id2)
        # print(df[date_columns].loc[idx])

        for time_col in date_columns:
            # print(time_col, df.loc[idx, time_col])
            numerator = float(df.loc[idx, time_col])
            # numerator = df.loc[(df[unique_id1] == id1), time_col]
            # print(id1, numerator)

            if rate_factor in ['', None]:
                rate_factor = 1
                # print('\nNo rate factor provided. Using "1" instead.')

            if norm_variable in ['', None]:
                # print(df2.loc[id2, time_col])
                denominator = float(df2.loc[id2, time_col])
                # denominator = int(df2.loc[(df2[unique_id2] == id2), time_col])
            else:
                denominator = float(df2.loc[id2, norm_variable])
                # denominator = int(df2.loc[(df2[unique_id2] == id2), norm_variable])

            if denominator > min_denominator: # prevent division by zero
                normalized = '%.5f' % ((numerator * rate_factor) / denominator)
            else:
                normalized = np.nan
                # if norm_variable in ['', None]:
                #     normalized = '%.5f' % (numerator / denominator)
                # else:
                #     # if rate_factor in ['', None]:
                #     #     rate_factor = 1
                #     normalized = '%.5f' % ((numerator * rate_factor) / denominator)

            # print(numerator, denominator)
            # print(normalized)
            df3.at[id1, time_col] = normalized
            # print(df3.loc[(df3[unique_id1] == id1), time_col])
            # df3.loc[(df3[unique_id1] == id1), time_col] = normalized

    df3 = df3.reset_index()
    df3 = df3.drop(columns=['unique_id1', 'unique_id2'])

    # output converted dataframes
    df3.to_csv(output, sep='\t', index=False)
    print('\nNormalization successfully completed.\n')
