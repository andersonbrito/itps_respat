# -*- coding: utf-8 -*-
#!/usr/bin/python

# Created by: Anderson Brito
# Email: andersonfbrito@gmail.com
# Release date: 2022-02-03
# Last update: 2022-02-03

import pandas as pd
import os
import argparse
from pathlib import Path

pd.set_option('display.max_columns', 500)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Merge dataframes in diretorories according to regular expression",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--path", required=False, help="Metadata file 1")
    parser.add_argument("--regex", required=False, default='*.*', help="Regular expression that identifies input files")
    parser.add_argument("--filters", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--fillna", required=False, default='\'\'', help="Filler to replace NA data points")
    parser.add_argument("--output", required=True, help="Merged file")
    args = parser.parse_args()

    path = args.path
    regex = args.regex
    filters = args.filters
    filler = args.fillna
    output = args.output

    # path = '/Users/anderson/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/20210113_relatório1/results/country/'
    # regex = '*.tsv'
    # fillna = None
    # output = path + 'merged.tsv'

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

    if path in ['', None]:
        path = os.getcwd()


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
    ldf = []
    for file in Path(path).rglob(regex):
        # print(file.resolve())
        subdf = load_table(file.resolve())
        if filters not in ['', None]:
            subdf = filter_df(subdf, filters)
        ldf.append(subdf)

    df = pd.concat(ldf)

    if filler in ['', None]:
        filler = ''
    df.fillna(filler, inplace=True)
    # print(df)

    # # nextstrain metadata
    # df1 = load_table(metadata1)
    # df1.fillna('', inplace=True)
    #
    # # Extra metadata
    # df2 = load_table(metadata2)
    # df2.fillna('', inplace=True)
    #
    # # merge frames
    # frames = [df1, df2]
    # result = pd.concat(frames)
    # result.fillna('', inplace=True)
    df.to_csv(output, sep='\t', index=False)

    print('\nTSV metadata files successfully merged.\n')
