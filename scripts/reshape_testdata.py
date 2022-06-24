# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: anderson.brito@itps.org.br
# Release date: 2021-12-05
# Last update: 2022-04-26

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


today = time.strftime('%Y-%m-%d', time.gmtime())
import platform
print('Python version:', platform.python_version())
print('Pandas version:', pd.__version__)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Combine and reformat data tables from multiple sources and formats into a single TSV file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--datadir", required=True, help="Name of the folder containing independent folders for each lab")
    parser.add_argument("--rename", required=False, help="TSV, CSV, or excel file containing new standards for column names")
    parser.add_argument("--correction", required=False, help="TSV, CSV, or excel file containing new standards for column values")
    parser.add_argument("--output", required=True, help="TSV file aggregating all columns listed in the 'rename file'")
    args = parser.parse_args()

    path = os.path.abspath(os.getcwd())
    input_folder = path + '/' + args.datadir + '/'
    rename_file = args.rename
    correction_file = args.correction
    output = args.output

    # path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/sgtf_omicron/analyses/run4_20220111_sgtf/'
    # input_folder = path + 'data/'
    # rename_file = input_folder + 'rename_columns.xlsx'
    # correction_file = input_folder + 'fix_values.xlsx'
    # output = input_folder + today + '_combined_data.tsv'

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
    dfR = load_table(rename_file)
    dfR.fillna('', inplace=True)

    dict_rename = {}
    # dict_corrections = {}
    for idx, row in dfR.iterrows():
        id = dfR.loc[idx, 'lab_id']
        if id not in dict_rename:
            dict_rename[id] = {}
        old_colname = dfR.loc[idx, 'column_name']
        new_colname = dfR.loc[idx, 'new_name']
        rename_entry = {old_colname: new_colname}
        dict_rename[id].update(rename_entry)

    # load value corrections
    dfC = load_table(correction_file)
    dfC.fillna('', inplace=True)

    dict_corrections = {}
    all_ids = list(set(dfC['lab_id'].tolist()))
    for idx, row in dfC.iterrows():
        lab_id = dfC.loc[idx, 'lab_id']
        colname = dfC.loc[idx, 'column_name']

        old_data = dfC.loc[idx, 'old_data']
        new_data = dfC.loc[idx, 'new_data']
        if old_data + new_data not in ['']:
            labs = []
            if colname == 'any':
                labs = all_ids
            else:
                labs = [lab_id]
            for id in labs:
                if id not in dict_corrections:
                    dict_corrections[id] = {}
                if colname not in dict_corrections[id]:
                    dict_corrections[id][colname] = {}
                data_entry = {old_data: new_data}
                dict_corrections[id][colname].update(data_entry)


    # Fix datatables
    print('\nFixing datatables...')
    def fix_datatable(dfL, lab):
        dfN = dfL
        if lab == 'Diagnósticos do Brasil':
            dfN = pd.DataFrame()
            failures = {}
            dfL['Exame'] = dfL['Exame'].apply(lambda x: 'MS2' if x == 'MS2CI' else x)

            for i, (code, dfG) in enumerate(dfL.groupby('NumeroPedido')):
                # print('>' + str(i))
                data = {}
                unique_cols = [col for col in dfG.columns.tolist() if len(list(set(dfG[col].tolist()))) == 1]
                for col in unique_cols:
                    data[col] = dfG[col].tolist()[0]
                if code not in failures:
                    failures[code] = []
                else:
                    print('\t\tWarning: ' + code + ' is likely a duplicate')

                # fix Ct values
                genes = ['NGENE', 'ORF1AB', 'SGENE', 'MS2']
                found = []
                for idx, row in dfG.iterrows():
                    gene = dfG.loc[idx, 'Exame']
                    if gene in genes:
                        found.append(gene)
                        ct_value = dfG.loc[idx, 'ResultadoLIS']
                        if '.' in ct_value:
                            ct_value = ct_value.replace('.', '')
                            if len(ct_value) < 5:
                                ct_value = ct_value + '0'*(5-len(ct_value))

                        ct_value = float(ct_value)/1000
                        if ct_value > 50:
                            ct_value = ct_value/10
                        data[gene] = str(np.round(ct_value, 2))
                    else:
                        found.append(gene)

                # check if gene was detected
                for g in genes:
                    if g in found:
                        found.remove(g)
                    else:
                        failures[code] += [g]
                if len(found) > 0:
                    for g in found:
                        if g not in genes:
                            print('\t\t- Gene ' + g + ' is so far unknown as a target. Check for inconsistencies in the file named above.')

                # print(failures[code])
                if 'MS2' in failures[code]:
                    print('\t\tWarning: ' + code + ' has no internal control')
                dfN = dfN.append(data, ignore_index=True)
            # print(dfN)
        elif lab == 'Diagnósticos do Brasil_2':
            # print(dfL)
            for idx, row in dfL.iterrows():
                values = dfL.loc[idx, 'Resultado do Teste COVID']
                # print(idx, values)
                for entry in values.split("/"):
                    if 'negativo' in entry.lower():
                        entry = entry.split(':')[0] + ':' + '0'
                    gene, ct = [i.strip() for i in entry.split(':')]
                    if gene == 'N':
                        dfL.loc[idx, 'Ct_N'] = str(np.round(float(ct.replace(',', '.')), 2))
                    elif gene == 'ORF':
                        dfL.loc[idx, 'Ct_ORF1ab'] = str(np.round(float(ct.replace(',', '.')), 2))
                    elif gene == 'S':
                        dfL.loc[idx, 'Ct_S'] = str(np.round(float(ct.replace(',', '.')), 2))
                dfL.loc[idx, 'test_result'] = 'Positive'
            dfN = dfL

        elif lab == 'DASA':
            def not_assigned(geo_data):
                empty = ['', 'SEM CIDADE', 'MUDOU', 'NAO_INFORMADO', 'NAOINFORMADO']
                if geo_data in empty:
                    geo_data = ''
                return geo_data

            dfL['birthdate'] = ''
            # dfL['sex'] = ''
            dfL['cidade_norm'] = dfL['cidade_norm'].apply(lambda x: not_assigned(x))
            dfL['uf_norm'] = dfL['uf_norm'].apply(lambda x: not_assigned(x))
            dfN = dfL

        elif lab == 'IMT-CDL':
            ignored = ['', 'sem amostra', 'Negativo']
            dfL = dfL[~dfL['GENOTIPAGEM'].isin(ignored)]

            def categories(genotype):
                mock_ct = '999'
                sgtf = ['Possível Omicron']
                nonsgtf = ['Ancestral', 'Indeterminado']
                if genotype in sgtf:
                    mock_ct = ''
                return mock_ct

            dfL['IDADE'] = dfL['IDADE'].str.split(' ').str[0]
            dfL['birthdate'] = ''
            dfL['state'] = 'AM'
            dfL['location'] = 'Manaus'
            dfL['Ct_S'] = dfL['GENOTIPAGEM'].apply(lambda x: categories(x.strip()))
            dfN = dfL
        return dfN

    def rename_columns(id, df):
        # print(df.columns.tolist())
        # print(dict_rename[id])
        df = df.rename(columns=dict_rename[id])
        # print(df.columns.tolist())
        return df

    # open data files
    dfT = pd.DataFrame()
    for element in os.listdir(input_folder):
        if not element.startswith('_'):
            id = element
            element = element + '/'
            if os.path.isdir(input_folder + element) == True:
                print('\n# Processing datatables from: ' + id)
                for filename in os.listdir(input_folder + element):
                    if filename.split('.')[-1] in ['tsv', 'csv', 'xls', 'xlsx'] and filename[0] not in ['~', '_']:
                        print('\t- File: ' + filename)
                        df = load_table(input_folder + element + filename)
                        df.fillna('', inplace=True)
                        df.reset_index(drop=True)
                        df = fix_datatable(df, id) # reformat datatable
                        df.insert(0, 'lab_id', id)
                        df = rename_columns(id, df) # fix data points
                        frames = [dfT, df]
                        df2 = pd.concat(frames)
                        dfT = df2

    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)

    # fix data points
    def fix_data_points(id, col_name, value):
        new_value = value
        if value in dict_corrections[id][col_name]:
            new_value = dict_corrections[id][col_name][value]
        return new_value


    print('\n# Fixing data points...')
    for lab_id, columns in dict_corrections.items():
        print('\t- Fixing data from: ' + lab_id)
        for column, values in columns.items():
            # print('\t- ' + column + ' (' + column + ' → ' + str(values) + ')')
            dfT[column] = dfT[column].apply(lambda x: fix_data_points(lab_id, column, x))

    # print(dfT)

    # reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    # create epiweek column
    def get_epiweeks(date):
        date = pd.to_datetime(date, errors='coerce')
        epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
        epiweek = epiweek[:4] + '_' + 'EW' + epiweek[-2:]
        return epiweek

    dfT['epiweek'] = dfT['date_testing'].apply(lambda x: get_epiweeks(x))

    for idx, row in dfT.iterrows():
        birth = dfT.loc[idx, 'birthdate']
        test = dfT.loc[idx, 'date_testing']
        if birth not in [np.nan, '', None]:
            birth = pd.to_datetime(birth)
            age = (test - birth) / np.timedelta64(1, 'Y')
            dfT.loc[idx, 'age'] = np.round(age, 1)

    # fix sex information
    dfT['sex'] = dfT['sex'].apply(lambda x: x[0] if x != '' else x)

    # Add gene detection results
    def check_detection(ctValue):
        result = "Not detected"
        try:
            if ctValue[0].isdigit() and float(ctValue) > 0:
                result = "Detected"
        except:
            pass
        return result

    # Ct value columns
    targets = []
    for col in dfT.columns.tolist():
        if col.startswith('Ct_'):
            new_col = col.split('_')[1] + '_detection'
            if new_col not in targets:
                targets.append(new_col)
            dfT[new_col] = dfT[col].apply(lambda x: check_detection(x))

    # reset index
    dfT = dfT.reset_index(drop=True)

    # generate sample id
    dfT.insert(1, 'sample_id', '')
    dfT.fillna('', inplace=True)
    dfT['unique_id'] = dfT.astype(str).sum(axis=1) # combine values in rows as a long string

    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
        return id
    # print(dfT.columns.tolist())
    # print(dfT[['lab_id', 'date_testing']])

    dfT['sample_id'] = dfT['unique_id'].apply(lambda x: generate_id(x))
    key_cols = ['lab_id', 'test_id', 'sample_id', 'Ct_S', 'Ct_N', 'Ct_ORF1ab', 'state', 'location', 'test_result',
                'sex', 'date_testing', 'epiweek', 'age', 'S_detection', 'N_detection', 'ORF1ab_detection']
    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])

    dfT['date_testing'] = dfT['date_testing'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # fix test results with empty data
    dfT['test_result'] = dfT['test_result'].apply(lambda x: 'Negative' if x not in ['Negative', 'Positive'] else x)

    # output duplicates rows
    duplicates = dfT.duplicated().sum()
    if duplicates > 0:
        mask = dfT.duplicated(keep=False) # find duplicates
        dfD = dfT[mask]
        output2 = input_folder + 'duplicates.tsv'
        dfD.to_csv(output2, sep='\t', index=False)
        print('\nWARNING!\nFile with %s duplicate entries saved in:\n%s' % (str(duplicates), output2))

    # drop duplicates
    dfT = dfT.drop_duplicates(keep='last')

    # output combined dataframe
    dfT.to_csv(output, sep='\t', index=False)
    print('\nData successfully aggregated and saved in:\n%s\n' % output)

