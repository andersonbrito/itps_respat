#!/usr/bin/python

# Created by: Anderson Brito
#
# clean_data.py -> Scritps to fix data columns with values that match user defined patterns
#
#
# Release date: 2021-11-13
# Last update: 2021-11-13

import pandas as pd
import argparse
from difflib import SequenceMatcher


pd.set_option('max_columns', 100)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Fix data columns with values that match user defined patterns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="TSV file with the original data")
    parser.add_argument("--patterns", required=True, type=str, help="TSV files with patterns and new standards")
    parser.add_argument("--purge",required=False, nargs=1, type=str, default='no',
                        choices=['no', 'yes'], help="Should original columns be purged after standardization?")
    parser.add_argument("--replacements",required=False, nargs=1, type=str, default='yes',
                        choices=['no', 'yes'], help="Should an extra file be generated, showing the data conversions?")
    parser.add_argument("--similarity", required=True, type=float, help="Similarity of comparisons to be accepted as a true match")
    parser.add_argument("--output", required=True, help="TSV file with corrected data pints")
    args = parser.parse_args()

    input = args.input
    patterns = args.patterns
    purge = args.purge
    replacements = args.replacements
    output = args.output
    output2 = 'replacements.tsv'
    similarity = args.similarity


    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/metasurvBR/data/metadata_genomes/'
    # input = path + 'metadata_2021-11-05_upto2021-08-31_BRAonly_seqtech.tsv'
    # patterns = path + 'patterns_labs.tsv'
    # purge = 'no'
    # replacements = 'yes'
    # output = path + 'metadata_2021-11-05_upto2021-08-31_BRAonly_seqtech_labs.tsv'
    # output2 = path + 'replacements.tsv'

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

    # load original data file
    dfM = load_table(input)
    dfM.fillna('', inplace=True)
    # print(dfM.head)

    # load file with patterns to be found and corrected
    dfP = load_table(patterns)
    dfM.fillna('', inplace=True)

    def similar(a, b):
        return SequenceMatcher(None, a, b).ratio()

    list_cols = list(set(dfP['column'].tolist()))

    found = {}
    notfound = {}
    def fix_data(column, value):
        threshold = similarity
        correct_value = ''
        if column not in found:
            found[column] = {}
            notfound[column] = []

        for idx, row in dfP.iterrows():
            old_str = dfP.loc[idx, 'pattern']
            new_str = dfP.loc[idx, 'standard']

            if value not in found[column]:
                # print(value, old_str, new_str)
                if similar(value, old_str) > threshold or similar(value, new_str) > threshold:
                    correct_value = new_str
                    print('\t- ' + value + '  →  ' + new_str)
                    found[column][value] = new_str
            else:
                correct_value = found[column][value]
        if correct_value == '':
            if value not in notfound[column]:
                notfound[column].append(value)

        return correct_value

    print('\nPerforming data conversions...\n')
    for column in list_cols:
        dfM = dfM.sort_values(by=column)
        dfM[column + '_fixed'] = dfM[column].apply(lambda x: fix_data(column, x))


    if len(notfound) > 0:
        print('\n\nThese data points did not match any pattern...\n')
        for column, list_values in notfound.items():
            for value in list_values:
                print('\t- ' + column + ': ' + value)

    if purge == 'yes':
        dfM.drop(columns=list_cols)
        for column in list_cols:
            dfM = dfM.rename(columns={column + '_fixed': column})

    # save
    dfM.to_csv(output, sep='\t', index=False)

    if replacements != 'no':
        data = []
        for column in found:
            for old, new in found[column].items():
                data.append([column, old, new])

        for column, old in notfound.items():
            if len(old) > 0:
                for entry in old:
                    data.append([column, entry, 'NotFound'])

        list_cols = dfP.columns.tolist()
        dfR = pd.DataFrame(data, columns=list_cols)
        dfR.to_csv(output2, sep='\t', index=False)

    print('\nData successfully converted.\n')