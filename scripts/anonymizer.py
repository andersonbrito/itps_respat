import pandas as pd
import argparse
import hashlib

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Add a new column with an index generated out of data columns",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--input", required=True, help="Original dataframe file")
    parser.add_argument("--columns", nargs='+', required=True, type=str, help="List of columns to be used to sort the dataframe")
    parser.add_argument("--remove", required=False, default='no', type=str,
                        choices=['yes', 'no'], help="Should rows with duplicated indexes be removed, keeping only one?")
    parser.add_argument("--output", required=True, help="TSV file with modified datraframe")
    args = parser.parse_args()

    input = args.input
    columns = args.columns
    remove = args.remove
    output = args.output


    # path = '/Users/anderson/google_drive/ITpS/documents/hiring/PD0121/challenge/'
    # input = path + 'metadata_2021-12-31_BRonly_geo_variants.tsv'
    # columns = ['strain', 'gisaid_epi_isl']
    # remove = 'yes'
    # output = path + 'matrix.tsv'
    # output2 = path + 'duplicates.tsv'


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

    # load renaming patterns
    df = load_table(input)
    df.fillna('', inplace=True)

    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
        return id

    df.insert(0, 'identifier', '')
    df['identifier'] = df[columns].astype(str).sum(axis=1).apply(lambda x: generate_id(x))

    # output duplicates rows
    duplicates = df['identifier'].duplicated().sum()
    if duplicates > 0:
        if remove == 'yes':
            mask = df['identifier'].duplicated(keep=False) # find duplicates
            dfD = df[mask]
            output2 = 'duplicates.tsv'
            dfD.to_csv(output2, sep='\t', index=False)
            print('\nWARNING!\nFile with %s duplicate entries saved in:\n%s' % (str(duplicates), output2))

            # drop duplicates
            df = df.drop_duplicates(['identifier'], keep='last')
        else:
            print('\nWARNING!\n%s duplicated entries were found.' % (str(duplicates)))


    # output combined dataframe
    df.to_csv(output, sep='\t', index=False)
    print('\nData successfully anonymized and saved in:\n\t- %s\n' % output)
