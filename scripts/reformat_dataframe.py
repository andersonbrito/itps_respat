import pandas as pd
import argparse
import os
import numpy as np
import unidecode
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Modify dataframe by adding, removing or modifying columns and rows",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input1", required=True, help="Original dataframe file")
    parser.add_argument("--input2", required=False, help="Files with extra columns to be added or modified, including an index column")
    parser.add_argument("--index", required=False, type=str, help="Column with unique identifiers")
    parser.add_argument("--action", required=False, type=str,
                        choices=['add', 'modify', 'reorder'], help="Action to be executed to filter target taxa")
    parser.add_argument("--mode", required=False, type=str,
                        choices=['columns', 'rows'], help="Elements to be processed: columns or rows?")
    parser.add_argument("--targets", required=False,  help="List of columns or rows to be added, remove or modified."
                                                           "It can be provided as a file, one target per line, or as a comma-separated list of targets.")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--start-date", required=False, type=str,  help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=False, type=str,  help="End date in YYYY-MM-DD format")
    parser.add_argument("--sortby", nargs='+', required=False, type=str, help="List of columns to be used to sort the dataframe")
    parser.add_argument("--output", required=True, help="TSV file with modified datraframe")
    args = parser.parse_args()

    input1 = args.input1
    input2 = args.input2
    index = args.index
    action = args.action
    mode = args.mode
    list_targets = args.targets
    filters = args.filter
    start_date = args.start_date
    end_date = args.end_date
    sortby = args.sortby
    output = args.output


    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/vigilancia/nextstrain/run12_20220104_report7/pre-analyses/'
    # input1 = path + 'metadata_2021-12-31_BRonly.tsv' # target file
    # input2 = path + '' # new data file
    # index = '' # index in common between both dataframes
    # action = 'filter'
    # mode = 'rows'
    # # list_targets = path + 'columns.tsv' # list of columns
    # list_targets = ''
    # sortby = 'date'
    # filters = 'country_exposure:Brazil'
    # start_date = 'date:2020-03-01' # start date above this limit
    # end_date = 'date:2021-12-31' # end date below this limit
    # output = path + 'filtered.tsv'

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
            df = df[~df[index].isin([''])]  # drop row with empty index
        else:
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX')
            exit()
        return df

    # original
    df1 = load_table(input1)
    df1.fillna('', inplace=True)

    if list_targets != None:
        if os.path.isfile(list_targets):
            targets = [item.strip() for item in open(list_targets).readlines()]
        else:
            if ',' in list_targets:
                # print(list_targets)
                targets = [x.strip() for x in list_targets.split(',')]
            else:
                targets = [list_targets]

    # filter by time
    def time_filter(df, time_var, start_date, end_date):
        print('\nFiltering by date: ' + start_date + ' > ' + end_date)
        df[time_var] = pd.to_datetime(df[time_var])  # converting to datetime format
        if start_date in [None, '']:
            start_date = df[time_var].min()
        if end_date in [None, '']:
            today = time.strftime('%Y-%m-%d', time.gmtime())
            end_date = today

        mask = (df[time_var] >= start_date) & (df[time_var] <= end_date)  # mask any lines with dates outside the start/end dates
        df = df.loc[mask]  # apply mask
        df[time_var] = df[time_var].dt.strftime('%Y-%m-%d')
        return df

    found = {}
    notfound = {}
    if action == 'add' and mode == 'columns':
        match = []
        def add_values(query, df, column):
            value = ''
            if query not in [None, '', np.nan]:
                lower_query = unidecode.unidecode(query).lower()
                dindexes = {unidecode.unidecode(x).lower(): x for x in df.index.tolist()}
                # print(dindexes)
                if lower_query in dindexes:
                    value = df.loc[dindexes[lower_query], column]
                    # print(query, lower_query, dindexes[lower_query], value)
                    # print(query, value)
                    if query not in match:
                        # print('\t- ' + query)
                        match.append(query)
                else:
                    # print(query, lower_query)
                    if column not in notfound:
                        notfound[column] = [query]
                    else:
                        if query not in notfound[column]:
                            notfound[column] += [query]
            else:
                pass
            return value

        # source of new columns
        if input2 != None:
            df2 = load_table(input2)
            df2.fillna('', inplace=True)
            df2 = df2.set_index(index)

        print('\n# Adding new columns')
        if len(targets) > 0:
            for col in targets:
                print('\t- ' + col)

        df1 = df1.sort_values(index)
        # dindexes = {unidecode.unidecode(x).lower(): x for x in df2.index.tolist()}
        for new_column in targets:
            df1[new_column] = df1[index].apply(lambda x: add_values(x, df2, new_column))
            # df1[new_column] = df1[index].str.lower().replace(dindexes, inplace=True)

    if action == 'modify' and mode == 'rows':
        # source of new data
        df2 = ''
        if input2 != None:
            df2 = load_table(input2)
            # df2 = pd.read_csv(path + input2, encoding='utf-8', sep='\t', dtype=str)
            df2.fillna('', inplace=True)
        else:
            print('File with reference values to be modified is missing. Provide \'--input2\'')
            exit()

        for id2, row2 in df2.iterrows():
            anchor_col, anchor_val, target_col, new_val = df2.loc[id2, 'anchor_col'], df2.loc[id2, 'anchor_val'], df2.loc[id2, 'target_col'], df2.loc[id2, 'new_val']
            # for id1, row1 in df1.iterrows():
            if anchor_val in df1[anchor_col].tolist():
                df1.loc[df1[anchor_col] == anchor_val, target_col] = new_val
                if target_col not in found:
                    found[target_col] = [new_val]
                else:
                    if new_val not in found[target_col]:
                        found[target_col] += [new_val]
            else:
                if anchor_col not in notfound:
                    if new_val not in found[target_col]:
                        notfound[anchor_col] = [anchor_val]
                else:
                    if anchor_val not in notfound[anchor_col]:
                        if new_val not in found[target_col]:
                            notfound[anchor_col] += [anchor_val]

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

    if filters not in [None, '']:
        print('\nFiltering rows based on user defined filters...')
        if filters not in ['', None]:
            df1 = filter_df(df1, filters)

    # Filter by date
    if start_date or end_date not in [None, '']:
        start, end = '', ''
        date_col = ''
        if start_date not in [None, '']:
            start = start_date.split(':')[1].strip()
            if date_col == '':
                date_col = start_date.split(':')[0].strip()
        if end_date not in [None, '']:
            end = end_date.split(':')[1].strip()
            if date_col == '':
                date_col = end_date.split(':')[0].strip()

        df1 = time_filter(df1, date_col, start, end)


    if action == 'reorder' and mode == 'columns':
        pass


    if len(found.keys()) > 0:
        print('\n# Fixed data points')
        for col, vals in found.items():
            print('  > ' + col)
            for v in vals:
                print('\t- ' + v)
    if len(notfound.keys()) > 0:
        print('\n# These reference data points where not found, and their actions were not implemented:')
        for col, vals in notfound.items():
            print('  > ' + col)
            for v in vals:
                print('\t- ' + v)

    # sort values
    if sortby != None:
        df1 = df1.sort_values(by=sortby)

    df1.to_csv(output, sep='\t', index=False)

