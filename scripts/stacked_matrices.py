import pandas as pd
import argparse
import os

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Pairwise association between corresponding columns and rows in two matrices, generating stacked rows with data",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input1", required=True, help="Matrix with data to be displayed in the X axis")
    parser.add_argument("--input2", required=False, help="Matrix with data to be displayed in the Y axis")
    parser.add_argument("--index", required=True, help="Column with unique identifier, common to both matrices")
    parser.add_argument("--xvar", required=True, type=str,  help="Name of the variable X")
    parser.add_argument("--yvar", required=False, type=str,  help="Name of the variable Y")
    parser.add_argument("--extra-columns", required=False, help="Extra columns with extra info to export")
    parser.add_argument("--output", required=True, help="TSV file showing corrected genome counts per epiweek")
    args = parser.parse_args()


    input1 = args.input1
    input2 = args.input2
    unique_id = args.index
    var_name1 = args.xvar
    var_name2 = args.yvar
    extra_cols = args.extra_columns
    output = args.output

    # print(extra_cols)

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/dashboard/nextstrain/run8_20211029_itps5/data/'
    # input1 = path + 'matrix_macroregions_year.tsv'
    # input2 = ''
    # output = path + 'stacked_matrix.tsv'
    #
    # unique_id = 'cd_hlt_'
    # var_name1 = 'total_genomes'
    # var_name2 = ''
    # extra_cols = []


    pd.set_option('display.max_columns', 500)

    # input genome and case counts per epiweek
    separator = '\t'
    df1 = pd.read_csv(input1, encoding='utf-8', sep='\t', dtype=str)
    if input2 not in ['', None]:
        df2 = pd.read_csv(input2, encoding='utf-8', sep='\t', dtype=str)

    # get values corresponding columns
    date_columns = []
    for column in df1.columns.to_list():
        if column[-1].isdecimal():
            if input2 not in ['', None]:
                if column in df2.columns.to_list():
                    date_columns.append(column)
            else:
                date_columns.append(column)

    # keep only common rows
    if input2 not in ['', None]:
        common_rows = [id for id in df1[unique_id].to_list() if id in df2[unique_id].to_list()]
    else:
        common_rows = [id for id in df1[unique_id].to_list()]


    df1 = df1[df1[unique_id].isin(common_rows)]
    df1.set_index(unique_id, inplace=True)

    if input2 not in ['', None]:
        df2 = df2[df2[unique_id].isin(common_rows)]
        df2.set_index(unique_id, inplace=True)

    # add other columns, if available
    if extra_cols == None:
        extra_cols = []
    else:
        if os.path.isfile(extra_cols):
            extra_cols = [item.strip() for item in open(extra_cols).readlines()]
        else:
            if ',' in extra_cols:
                extra_cols = [x.strip() for x in extra_cols.split(',')]
            else:
                extra_cols = [extra_cols]

    print(extra_cols)

    # get stacked merged dataframe
    data = {}
    for idx, row in df1.iterrows():
        if len(data) == 0:
            data['id'] = []
            data['group_id'] = []
            data[unique_id] = []
            for col in extra_cols:
                data[col] = []
            data[var_name1] = []
            if input2 not in ['', None]:
                data[var_name2] = []

        for time_point in date_columns:
            id = idx + '.' + time_point
            variable1 = df1.loc[idx, time_point]

            if input2 not in ['', None]:
                variable2 = df2.loc[idx, time_point]

                if 'X' not in variable1 and 'X' not in variable2:
                    if float(variable1) > 0 and float(variable2) > 0:
                        data['id'].append(id)
                        data['group_id'].append(time_point)
                        data[unique_id].append(idx)
                        # data[var_name1].append(np.log10(float(variable1)*10000))
                        # data[var_name2].append(np.log10(float(variable2)*100))
                        data[var_name1].append(float(variable1))
                        data[var_name2].append(float(variable2))
                        # print(variable2, float(variable2)*1000000, np.log10(float(variable2)*1000000))
                        for col in extra_cols:
                            value = ''
                            try:
                                value = df1.loc[idx, col]
                            except:
                                value = df2.loc[idx, col]
                            data[col].append(value)
            else:
                if 'X' not in variable1:
                    if float(variable1) > 0:
                        data['id'].append(id)
                        data['group_id'].append(time_point)
                        data[unique_id].append(idx)
                        # data[var_name1].append(np.log10(float(variable1)*10000))
                        data[var_name1].append(float(variable1))
                        # print(variable2, float(variable2)*1000000, np.log10(float(variable2)*1000000))
                        for col in extra_cols:
                            value = ''
                            try:
                                value = df1.loc[idx, col]
                            except:
                                value = ''
                            data[col].append(value)

    # print(data)
    # Create DataFrame
    df3 = pd.DataFrame(data)
    df3.to_csv(output, sep='\t', index=False)
