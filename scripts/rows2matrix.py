#!/usr/bin/python

# Created by: Anderson Brito
#
# row2matrix.py -> It converts stacked rows of values in two columns into a matrix
#
#
# Release date: 2021-08-22
# Last update: 2021-12-21

import pandas as pd
import argparse
import time
import itertools

# pd.set_option('max_columns', 100)
# print(pd.show_versions())

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

import platform
# print('Python version:', platform.python_version())
# print('Pandas version:', pd.__version__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate matrix of occurrences at the intersection of two or more columns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="TSV file with data to be aggregated as two-dimensional matrix")
    parser.add_argument("--xvar", required=True, type=str, help="Data that goes in the X axis of the matrix")
    parser.add_argument("--xtype", required=False, type=str, help="Is the x variable a time variable (date)? If so, enter 'time'")
    parser.add_argument("--target", required=False, type=str, help="Target column, when variable is already aggregated")
    parser.add_argument("--sum-target",required=False, nargs=1, type=str, default='no',
                        choices=['no', 'yes'], help="Should values in target column be summed up?")
    parser.add_argument("--format",required=False, nargs=1, type=str, default='float',
                        choices=['float', 'integer'], help="What is the format of the data points (float/integer)?")
    parser.add_argument("--yvar", nargs="+", required=True, type=str, help="One or more columns to be used as index")
    parser.add_argument("--unique-id", required=True, type=str, help="Column including the unique ids to be displayed in the Y axis")
    parser.add_argument("--extra-columns", nargs="*", required=False, type=str, help="Extra columns to export")
    parser.add_argument("--new-columns", required=False, help="New columns to be added with standard identifiers, e.g. 'country:Brazil'")
    parser.add_argument("--filters", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--time-var", required=False, type=str, help="Time variable, when x variable is not temporal data")
    parser.add_argument("--start-date", required=False, type=str,  help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=False, type=str,  help="End date in YYYY-MM-DD format")
    parser.add_argument("--output", required=True, help="TSV matrix")
    args = parser.parse_args()
    # print(args)

    input = args.input
    x_var = args.xvar
    x_type = args.xtype
    target_variable = args.target
    sum_target = args.sum_target[0]
    data_format = args.format[0]
    y_var = args.yvar
    y_unique_id = args.unique_id
    extra_cols = args.extra_columns
    add_id_cols = args.new_columns
    filters = args.filters
    timevar = args.time_var
    start_date = args.start_date
    end_date = args.end_date
    output = args.output

    # path = '/Users/anderson/google_drive/ITpS/projetos_colaboracoes/phyloDF/data/epidemiology/brasil/tsv/'
    # input = path + 'painel_short.tsv'
    # x_var = 'data'
    # x_type = 'time'
    # y_var = ['codmun']
    # y_unique_id = 'codmun'
    # target_variable = 'casosNovos'
    # sum_target = 'no'
    # data_format = 'integer'
    # add_id_cols = 'pais:Brasil'
    # extra_cols = ['regiao', 'estado', 'municipio', 'coduf']
    # filters = "~codmun:"
    # timevar = ''
    # start_date = '2020-03-01' # start date above this limit
    # end_date = '2021-12-31' # end date below this limit
    # output = path + 'matrix.tsv'

    def load_table(file):
        df = ''
        if str(file).split('.')[-1] == 'tsv':
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

    df = load_table(input)
    df.fillna('', inplace=True)
    # print(df.columns.tolist())

    for idx in y_var:
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
    # print(df[y_var])

    # filter by time
    if x_type == 'time':
        if timevar in ['', None]:
            timevar = x_var

    if timevar not in ['', None]:
        today = time.strftime('%Y-%m-%d', time.gmtime())

        df[timevar] = df[timevar].str.replace('/', '-', regex=False)

        # assess date completeness
        df = df[df[timevar].apply(lambda x: len(x.split('-')) == 3)]  # accept only full dates
        df = df[df[timevar].apply(lambda x: 'X' not in x)] # exclude -XX-XX missing dates

        # print(timevar)
        # print(df[timevar])

        df[timevar] = pd.to_datetime(df[timevar])  # converting to datetime format
        if start_date in [None, '']:
            start_date = df[timevar].min().strftime('%Y-%m-%d')
        if end_date in [None, '']:
            end_date = today
        print('\n\t- Filtering data by ' + '\"' + timevar + '\": ' + start_date + ' > ' + end_date)
        mask = (df[timevar] >= start_date) & (df[timevar] <= end_date)  # mask any lines with dates outside the start/end dates
        df = df.loc[mask]  # apply mask
        df[timevar] = df[timevar].dt.strftime('%Y-%m-%d')

    if x_type == 'time':
        time_range = [day.strftime('%Y-%m-%d') for day in list(pd.date_range(pd.to_datetime(start_date), pd.to_datetime(end_date), freq='d'))]
        data_cols = time_range
    else:
        data_cols = sorted(df[x_var].unique().tolist())


    list_ids = []
    for col_index in y_var:
        # ids = []
        ids = list(set(df[col_index].tolist()))
        list_ids.append(ids)
    # print(list_ids)

    print('\nA total of ' + str(len(df.index) + 1) + ' rows were included after filtering (by values and time period).')

    # print(data_cols)
    # print(rows)

    # set new indices
    df.insert(0, 'unique_id1', '')
    df['unique_id1'] = df[y_var].astype(str).sum(axis=1)
    df['unique_id1'] = df['unique_id1'].astype(str)
    df.insert(1, 'unique_id2', '')
    df['unique_id2'] = df[y_unique_id]#.astype(str).sum(axis=1)
    df2 = pd.DataFrame(columns=data_cols)
    # df2['unique_id1'] = df2[y_var].astype(str).sum(axis=1)

    # indexing
    df2.insert(0, 'unique_id1', '')
    for y_col in y_var:
        df2.insert(0, y_col, '')


    rows = list(itertools.product(*list_ids))
    for idx, id_names in enumerate(rows):
        unique_id1 = ''.join(id_names)
        pos = y_var.index(y_unique_id)
        unique_id2 = id_names[pos]
        # print(unique_id1, unique_id2)
        df2.loc[idx, 'unique_id1'] = unique_id1
        df2.loc[idx, 'unique_id2'] = unique_id2
        for num, col_name in enumerate(y_var):
            value = id_names[num]
            df2.loc[idx, col_name] = value


    df2 = df2.fillna(0) # with 0s rather than NaN
    df2.set_index('unique_id1', inplace=True)

    if extra_cols not in [None, '', ['']]:
        for column in extra_cols:
            if column in df.columns.to_list():
                df2.insert(0, column, '')
    else:
        extra_cols = []

    if target_variable in ['', None]:
        y_var = list(set(y_var))
        df1 = df.groupby([x_var] + ['unique_id1']).size().to_frame(name='count').reset_index() # group and count occorrences
    else:
        if sum_target == 'yes':
            if data_format == 'float':
                df[target_variable] = df[target_variable].astype(float)
            else:
                df[target_variable] = df[target_variable].astype(int)

            df1 = df.groupby([x_var] + y_var, sort=False)[target_variable].sum().reset_index(name='count')

            if data_format == 'float':
                df1['count'] = df1['count'].round(2)
        else:
            df1 = df.rename(columns={target_variable: 'count'})

    # print(df)
    # if len(y_var) > 0:
    # df[y_unique_id] = df[y_unique_id].astype(str)
    # df1[y_var] = df1[y_var].astype(str)

    df.set_index('unique_id1', inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    # print(df.head)
    # print(df2.head)
    # print(df2)

    # fill extra columns with their original content
    if extra_cols not in [None, '', ['']]:
        for column in extra_cols:
            if column in df.columns.to_list():
                for idx, row in df2.iterrows():
                    unique_id2 = df2.loc[idx, 'unique_id2']
                    value = df.loc[df['unique_id2'] == unique_id2, column].iloc[0]
                    # print(idx, column, value)
                    df2.at[idx, column] = value
    else:
        extra_cols = []

    # populate output dataframe
    for idx, row in df1.iterrows():
        x = df1.loc[idx, x_var]
        y = df1.loc[idx, 'unique_id1']
        count = int(df1.loc[idx, 'count'])
        if count < 0:
            count = 0
        df2.at[y, x] = count


    # save
    df2 = df2.reset_index()
    df2 = df2.drop(columns=['unique_id1', 'unique_id2'])
    if len(extra_cols) > 0:
        df2 = df2[y_var + extra_cols + data_cols]
    else:
        df2 = df2[y_var + data_cols]

    # add new columns with identifiers
    if add_id_cols not in ['', None]:
        new_cols = [c.strip() for c in add_id_cols.split(',')]
        for col in new_cols:
            col_name, col_value = col.split(':')
            df2.insert(0, col_name, '')
            df2[col_name] = col_value

    df2 = df2.sort_values(by=y_unique_id)
    df2.to_csv(output, sep='\t', index=False)
    print('\nConversion successfully completed.\n')

