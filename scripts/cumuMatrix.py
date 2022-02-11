import pandas as pd
import argparse


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate matrix with increasing, cumulative counts from time series",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Time series counts")
    parser.add_argument("--index", required=True, type=str,  help="Unique identifier in Y axis")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--output", required=True, help="TSV matrix with cumulative counts")
    args = parser.parse_args()


    input = args.input
    unique_id = args.index
    filters = args.filter
    output = args.output

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/dashboard/bimap/pipeline/data/'
    # input = path + 'matrix_brazil-states_cases.tsv'
    # output = path + 'matrix_brazil-states_cases_cumu.tsv'
    # unique_id = 'state'


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
    # print(df)

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


    # get total genomes and cases
    date_columns = []
    for column in df.columns.to_list():
        if column[-1].isdecimal():
            date_columns.append(column)
    # print(date_columns)

    # create empty dataframes
    nondate_columns = [column for column in df.columns.to_list() if column not in date_columns]
    df2 = df.filter(nondate_columns, axis=1)
    # print(df2)

    # set new index
    df.set_index(unique_id, inplace=True)
    df2.set_index(unique_id, inplace=True)


    # get cumulative values
    for idx, row in df.iterrows():
        # print('- ' + idx)
        current_count = 0
        for point in date_columns:
            metric = int(df.loc[idx, point])
            # print(idx, current_count, metric)
            current_count = current_count + metric
            df2.loc[idx, point] = current_count

    df2 = df2.reset_index()
    # print(df2)

    # output converted dataframes
    df2.to_csv(output, sep='\t', index=False)
    print('\nCumulative counts successfully calculated.\n')
