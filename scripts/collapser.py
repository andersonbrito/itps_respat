#!/usr/bin/python

# Created by: Anderson Brito
#
# row2matrix.py -> It converts stacked rows of values in two columns into a matrix
#
#
# Release date: 2021-08-22
# Last update: 2021-09-22

import pandas as pd
import argparse
import itertools

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# pd.set_option('max_columns', 100)
# pd.options.mode.chained_assignment = None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Colapse groups of two or more rows, summing up corresponding values in matrix",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Matrix of daily case counts per location")
    parser.add_argument("--index", required=True, nargs='+', type=str, help="Column with category breakdowns, which will be collapsed and sum up values")
    parser.add_argument("--unique-id", required=True, type=str, help="Unique id column to guide inclusion of extra columns")
    parser.add_argument("--extra-columns", required=False, nargs='+', type=str, help="Extra columns to export")
    parser.add_argument("--new-columns", required=False, help="New columns with standard identifiers, applicable to all rows, e.g. 'country:Brazil'")
    # parser.add_argument("--target", required=True, type=str, help="Column with category breakdowns, which will be collapsed")
    parser.add_argument("--ignore", required=False, nargs='+', type=str, help="Columns to be ignored, not added in output")
    parser.add_argument("--format",required=False, nargs=1, type=str, default='float',
                        choices=['float', 'integer'], help="What is the format of the data points (float/integer)?")
    parser.add_argument("--sortby", required=False, nargs='+', type=str, help="Columns to be used to sort the output file")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--output", required=True, help="Final output in TSV format")
    args = parser.parse_args()

    input = args.input
    id_cols = args.index
    unique_id = args.unique_id
    extra_cols= args.extra_columns
    add_cols = args.new_columns
    # target = args.target
    ignore_cols = args.ignore
    data_format = args.format[0]
    sortby = args.sortby
    filters = args.filter
    output = args.output


    # path = '/Users/anderson/google_drive/ITpS/projetos_colaboracoes/origins/analyses/nextstrain/run2_20220205_alpha/subsampler/data/'
    # input = path + 'matrix_lineages_months.tsv'
    # id_cols = 'variant'
    # unique_id = 'variant'
    # extra_cols = ''
    # add_cols = ''
    # # target = 'test_result'
    # ignore_cols = ['pango_lineage']
    # data_format = 'integer'
    # sortby = ['variant']
    # filters = ''
    # output = path + 'collapsed_matrix.tsv'


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

    # if target in id_cols + extra_cols:
    #     print('\nThe `target` column should not be among the `index columns` or `extra columns`. Please fix it, and restart.')
    #     exit()

    # original dataframe
    df = load_table(input)
    df.fillna('', inplace=True)
    # df.set_index(groupby, inplace=True)
    
    added_cols = []
    # add new columns with identifiers
    if add_cols not in ['', None]:
        new_cols = [c.strip() for c in add_cols.split(',')]
        for col in new_cols:
            col_name, col_value = col.split(':')
            added_cols.append(col_name)
            df.insert(0, col_name, '')
            df[col_name] = col_value

    # filter rows
    def filter_df(df, criteria):
        print('\nFiltering rows...')
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


    # drop columns
    if ignore_cols not in ['', None]:
        for col in ignore_cols:
            df = df.drop(columns=col.strip())

    # print(ignore_cols)

    if extra_cols in [None, '', ['']]:
        extra_cols = []

    if not type(id_cols) == list:
        id_cols = [id_cols]

    # add data columns
    all_cols = df.columns.tolist()
    # datacols = [col for col in all_cols if col not in [unique_id] + [target] + extra_cols + id_cols + added_cols]
    datacols = [col for col in all_cols if col not in [unique_id] + extra_cols + id_cols + added_cols]
    # print(datacols)

    df[datacols] = df[datacols].apply(pd.to_numeric, errors='coerce')


    list_ids = []
    for col_index in id_cols:
        ids = list(set(df[col_index].tolist()))
        list_ids.append(ids)
    # print(list_ids)
    #
    # print(datacols)
    # print(rows)

    # set new indices
    df.insert(0, 'unique_id1', '')
    df['unique_id1'] = df[id_cols].astype(str).sum(axis=1)
    df.insert(1, 'unique_id2', '')
    df['unique_id2'] = df[unique_id]

    # print(df.head())

    df2 = pd.DataFrame(columns=datacols)
    # print(df2)

    # indexing
    df2.insert(0, 'unique_id1', '')
    for y_col in id_cols:
        df2.insert(0, y_col, '')

    rows = list(itertools.product(*list_ids))
    # print(rows)
    for idx, id_names in enumerate(rows):
        unique_id1 = ''.join(id_names)
        pos = id_cols.index(unique_id)
        unique_id2 = id_names[pos]
        # print(unique_id1, unique_id2)
        df2.loc[idx, 'unique_id1'] = unique_id1
        df2.loc[idx, 'unique_id2'] = unique_id2
        for num, col_name in enumerate(id_cols):
            value = id_names[num]
            df2.loc[idx, col_name] = value

    # print(df2)
    df2 = df2.fillna(0) # with 0s rather than NaN
    df2.set_index('unique_id1', inplace=True)

    if len(added_cols) > 0:
        extra_cols += added_cols

    if extra_cols not in [None, '', ['']]:
        for column in extra_cols:
            if column in df.columns.to_list():
                df2.insert(0, column, '')

    # if extra_cols not in [None, '', ['']]:
    #     for column in extra_cols:
    #         if column in df.columns.to_list():
                for idx, row in df2.iterrows():
                    unique_id2 = df2.loc[idx, 'unique_id2']
                    value = df.loc[df['unique_id2'] == unique_id2, column].iloc[0]
                    # print(idx, column, value)
                    df2.at[idx, column] = value

    # print(df.head())

    for name, dfG in df.groupby('unique_id1', as_index=False):
        for col in datacols:
            y, x = name, col
            count = dfG[col].sum()
            # print(y, x, count)
            # print('\t*', df2.loc[y, x])
            df2.loc[y, x] = count

    # print(df2)

    # save
    df2 = df2.reset_index()
    df2 = df2.drop(columns=['unique_id1', 'unique_id2'])
    if len(extra_cols) > 0:
        df2 = df2[id_cols + extra_cols + datacols]
    else:
        df2 = df2[id_cols + datacols]


    if sortby not in ['', None]:
        df2 = df2.sort_values(by=sortby)

    # save
    df2.to_csv(output, sep='\t', index=False)
    print('\nConversion successfully completed.\n')