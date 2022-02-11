import pandas as pd
import argparse
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Aggregate daily counts as epiweeks, months or year",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Matrix with numeric data to be grouped based on specific ranges")
    parser.add_argument("--column", required=True, type=str,  help="Target columns with numeric data")
    parser.add_argument("--bins", required=True, type=str,  help="Comma-separated string or file with numbers (one per line) representing the upper limits of each group")
    parser.add_argument("--group", required=True, type=str,  help="Name of the new column, where groups will be added")
    parser.add_argument("--lowest", required=False, type=str,  help="Lowest value among all values")
    parser.add_argument("--highest", required=False, type=str,  help="Highest value among all values")
    parser.add_argument("--filter", required=False, type=str, help="Format: '~column_name:value'. Remove '~' to keep only that data category")
    parser.add_argument("--sortby", nargs='+', required=False, type=str, help="List of columns to be used to sort the dataframe")
    parser.add_argument("--output", required=True, help="TSV matrix with extra column with grouped categories")
    args = parser.parse_args()

    input = args.input
    target_col = args.column
    range = args.bins
    group_name = args.group
    lowest = args.lowest
    highest = args.highest
    filters = args.filter
    sortby = args.sortby
    output = args.output

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/sgtf_omicron/analyses/run4_20220111_sgtf/results/'
    # input = path + 'combined_testdata_geo.tsv'
    # target_col = 'age'
    # # ranges = ['100+','95-99','90-94','85-89','80-84','75-79','70-74','65-69','60-64','55-59','50-54','45-49','40-44','35-39','30-34','25-29','20-24','15-19','10-14','5-9','0-4']
    # range = ['4', '9', '14', '19', '24', '29', '34', '39', '44', '49', '54', '59', '64', '69', '74', '79', '84', '89', '94', '99']
    # lowest = -1
    # highest = 200
    # filters = '~test_result:Negative, ~age:'''
    # output = input.split('.')[0] + '2.tsv'


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

    # Load metadata
    df = load_table(input)
    df.fillna('', inplace=True)

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

    if os.path.isfile(range):
        range = [item.strip() for item in open(range).readlines()]
    else:
        if ',' in range:
            range = [x.strip() for x in range.split(',')]
        else:
            range = [range]

    origin = 0
    if lowest not in [''] and str(lowest)[-1].isdigit():
        origin = float(lowest)
    bins = [origin] + range
    if highest not in [''] and str(highest)[-1].isdigit():
        if float(bins[-1]) < float(highest):
            bins.append(highest)

    def get_group(value):
        tick_label = 'NA'
        if value not in [None, '']:
            if str(value)[-1].isdigit():
                value = float(value.strip())
            for num, varbin in enumerate(bins):
                if num < len(bins) - 1:
                    start, end = int(bins[num]), int(bins[num + 1])
                    if start < value <= end:
                        tick_label = str(start) + '-' + str(end)
                        if highest not in [''] and float(end) == float(highest):
                            tick_label = str(int(bins[-2]) + 1) + '+'
                        # print(start, '<', value, '<=', end , '... ' , tick_label)
        return tick_label

    # group data by range
    df[group_name] = df[target_col].apply(lambda x: get_group(x)).str.replace('-1-4', '0-4')

    # for idx, row in df[[target_col, 'category']].iterrows():
    #     print(df.loc[idx, 'sample_id'], df.loc[idx, target_col], df.loc[idx, 'category'])

    # sort values
    if sortby != None:
        df = df.sort_values(by=sortby)

    df.to_csv(output, sep='\t', index=False)
    print('\nNew column successfully created.\n\t- Output was saved in:%s\n' % output)
