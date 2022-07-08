# -*- coding: utf-8 -*-

# Created by: Anderson Brito
# Email: anderson.brito@itps.org.br
# Release date: 2022-01-19
# Last update: 2022-01-19

import pandas as pd
import os
import numpy as np
import hashlib
import time
import argparse
from epiweeks import Week

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

pd.set_option('display.max_columns', 500)
pd.options.mode.chained_assignment = None

today = time.strftime('%Y-%m-%d', time.gmtime())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Combine and reformat data tables from multiple sources and output a single TSV file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--datadir", required=True, help="Name of the folder containing independent folders for each lab")
    parser.add_argument("--rename", required=False, help="TSV, CSV, or excel file containing new standards for column names")
    parser.add_argument("--correction", required=False, help="TSV, CSV, or excel file containing data points requiring corrections")
    parser.add_argument("--cache", required=False, help="Previously processed data files")
    parser.add_argument("--output", required=True, help="TSV file aggregating all columns listed in the 'rename file'")
    args = parser.parse_args()

    path = os.path.abspath(os.getcwd())
    input_folder = path + '/' + args.datadir + '/'
    rename_file = args.rename
    correction_file = args.correction
    cache_file = args.cache

    output = args.output

    # path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/itps_respat_test/'
    # input_folder = path + 'data/'
    # rename_file = input_folder + 'rename_columns.xlsx'
    # correction_file = input_folder + 'fix_values.xlsx'
    # cache_file = input_folder + 'cache.tsv'
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


    # load cache file
    if cache_file not in [np.nan, '', None]:
        dfP = load_table(cache_file)
        dfP.fillna('', inplace=True)
    else:
        dfP = pd.DataFrame()


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



    def generate_id(column_id):
        id = hashlib.sha1(str(column_id).encode('utf-8')).hexdigest()
        return id



    # Fix datatables
    print('\nFixing datatables...')
    def fix_datatable(dfL, file):
        dfN = dfL
        # if lab == 'DASA':

        if 'codigo' in dfL.columns.tolist():
            id_columns = ['codigorequisicao', 'idade', 'sexo', 'data_exame', 'cidade', 'uf']
            for column in id_columns:
                if column not in dfL.columns.tolist():
                    dfL[column] = ''
                    print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

            # generate sample id
            dfL.insert(1, 'sample_id', '')
            dfL.insert(1, 'test_kit', 'DASA4')
            dfL.fillna('', inplace=True)
            dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1) # combine values in rows as a long string
            dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)) # generate alphanumeric sample id

            # prevent reprocessing of previously processed samples
            if cache_file not in [np.nan, '', None]:
                duplicates = dfL[dfL['sample_id'].isin(dfP['sample_id'].tolist())]['sample_id'].tolist()
                if len(duplicates) > 0:
                    print('\t\t * A total of ' + str(len(duplicates)) + ' samples were already processed previously. Skipping...')
                dfL = dfL[~dfL['sample_id'].isin(dfP['sample_id'].tolist())] # remove duplicates

            pathogens = {'FLUA': ['FLUA'], 'FLUB': ['FLUB'], 'VSR': ['VSR'], 'SC2': ['COVID'],
                         'META': [], 'RINO': [], 'PARA': [], 'ADENO': [], 'BOCA': [], 'COVS': [], 'ENTERO': [], 'BAC': []}
            dfN = pd.DataFrame()
            unique_cols = list(set(dfL.columns.tolist()))

            for i, (code, dfR) in enumerate(dfL.groupby('codigorequisicao')):
                data = {} # one data row for each request
                for col in unique_cols:
                    data[col] = dfR[col].tolist()[0]

                target_pathogen = {}
                for p, t in pathogens.items():
                    data[p + '_test_result'] = 'Not tested'
                    for g in t:
                        target_pathogen[g] = p
                dfR['pathogen'] = dfR['codigo'].apply(lambda x: target_pathogen[x])

                genes = {'FLUA': 1, 'FLUB': 1, 'VSR': 1, 'COVID': 1}
                found = []

                for virus, dfG in dfR.groupby('pathogen'):
                    # print('>>> Test for', virus)
                    for idx, row in dfG.iterrows():
                        gene = dfG.loc[idx, 'codigo']
                        ct_value = int(dfG.loc[idx, 'positivo'])
                        result = '' # to be determined
                        if gene in genes:
                            found.append(gene)
                            if gene not in data:
                                data[gene] = '' # assign target
                                if ct_value != '':
                                    data[gene] = str(ct_value)
                                    # print(gene, ct_value)

                                    if ct_value == genes[gene]:
                                        result = 'DETECTADO'
                                        data[virus + '_test_result'] = result
                                    else: # target not detected
                                        # print('Ct too high for gene', gene)
                                        result = 'NÃO DETECTADO'
                                        data[virus + '_test_result'] = result
                                    # print('\t * ' + gene + ' (' + str(ct_value) + ') = ' + data[virus + '_test_result'])

                                else: # if no Ct is reported
                                    result = 'NÃO DETECTADO'
                                    # print('\t - ' + gene + ' (' + str(ct_value) + ') = ' + data[virus + '_test_result'])
                                    if data[virus + '_test_result'] != 'DETECTADO':
                                        data[virus + '_test_result'] = result
                        else:
                            found.append(gene)

                    # check if gene was detected
                    for g in genes.keys():
                        if g in found:
                            found.remove(g)
                    if len(found) > 0:
                        for g in found:
                            if g not in genes:
                                print('Gene ' + g + ' in an anomaly. Check for inconsistencies')

                # print(data)
                dfN = dfN.append(data, ignore_index=True)

        # elif lab == 'DASA_2':
        elif 'Gene S' in dfL.columns.tolist():

            if 'resultado' not in dfL.columns.tolist():
                if 'resultado_norm' in dfL.columns.tolist():
                    dfL.rename(columns={'resultado_norm': 'resultado'}, inplace=True)
                    dfL['resultado'] = dfL['resultado'].apply(
                        lambda x: 'NAO DETECTADO' if x == 'NEGATIVO' else 'DETECTADO')
                else:
                    if 'resultado_original' in dfL.columns.tolist():
                        dfL.rename(columns={'resultado_original': 'resultado'}, inplace=True)
                        dfL['resultado'] = dfL['resultado'].apply(
                            lambda x: 'NAO DETECTADO' if x == 'NDT' else 'DETECTADO')
                    else:
                        print('No \'result\' column found.')
                        exit()

            if 'requisicao' not in dfL.columns.tolist():
                if 'codigo_externo_do_paciente' in dfL.columns.tolist():
                    dfL.rename(columns={'codigo_externo_do_paciente': 'requisicao'}, inplace=True)
                else:
                    dfL.insert(1, 'requisicao', '')
                    print('\t\t\t - No \'requisicao\' column found. Please check for inconsistencies. Meanwhile, an empty \'requisicao\' column was added.')

            # print(dfL.columns.tolist())
            id_columns = ['requisicao', 'data', 'idade', 'sexo', 'cidade_norm', 'uf_norm', 'Gene N', 'Gene ORF', 'Gene S']
            for column in id_columns:
                if column not in dfL.columns.tolist():
                    dfL[column] = ''
                    print('\t\t\t - No \'%s\' column found. Please check for inconsistencies. Meanwhile, an empty \'%s\' column was added.' % (column, column))

            # generate sample id
            dfL.insert(1, 'sample_id', '')
            dfL.insert(1, 'test_kit', 'TaqPath')
            dfL.fillna('', inplace=True)
            dfL['unique_id'] = dfL[id_columns].astype(str).sum(axis=1) # combine values in rows as a long string
            dfL['sample_id'] = dfL['unique_id'].apply(lambda x: generate_id(x)) # generate alphanumeric sample id

            # prevent reprocessing of previously processed samples
            if cache_file not in [np.nan, '', None]:
                duplicates = dfL[dfL['sample_id'].isin(dfP['sample_id'].tolist())]['sample_id'].tolist()
                if len(duplicates) > 0:
                    print('\t\t * A total of ' + str(len(duplicates)) + ' samples were already processed previously. Skipping...')
                dfL = dfL[~dfL['sample_id'].isin(dfP['sample_id'].tolist())] # remove duplicates

            def not_assigned(geo_data):
                empty = ['', 'SEM CIDADE', 'MUDOU', 'NAO_INFORMADO', 'NAOINFORMADO']
                if geo_data in empty:
                    geo_data = ''
                return geo_data

            pathogens = {'FLUA': [], 'FLUB': [], 'VSR': [], 'SC2': [], 'META': [], 'RINO': [],
                         'PARA': [], 'ADENO': [], 'BOCA': [], 'COVS': [], 'ENTERO': [], 'BAC': []}

            # target_pathogen = {}
            for p, t in pathogens.items():
                if p != 'SC2':
                    dfL[p + '_test_result'] = 'Not tested'

            dfL['birthdate'] = ''
            dfL['Ct_FluA'] = ''
            dfL['Ct_FluB'] = ''
            dfL['Ct_VSR'] = ''
            dfL['Ct_RDRP'] = ''

            dfL['cidade_norm'] = dfL['cidade_norm'].apply(lambda x: not_assigned(x))
            dfL['uf_norm'] = dfL['uf_norm'].apply(lambda x: not_assigned(x))
            dfN = dfL

        # print(dfN.head())
        return dfN


    def rename_columns(id, df):
        # print(df.columns.tolist())
        # print(dict_rename[id])
        if id in dict_rename:
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
                for filename in sorted(os.listdir(input_folder + element)):
                    if filename.split('.')[-1] in ['tsv', 'csv', 'xls', 'xlsx'] and filename[0] not in ['~', '_']:
                        print('\n\t- File: ' + filename)
                        df = load_table(input_folder + element + filename)
                        df.fillna('', inplace=True)
                        df.reset_index(drop=True)

                        df = fix_datatable(df, filename) # reformat datatable
                        df.insert(0, 'lab_id', id)
                        df = rename_columns(id, df) # fix data points
                        dfT = dfT.reset_index(drop=True)
                        df = df.reset_index(drop=True)
                        frames = [dfT, df]
                        df2 = pd.concat(frames)
                        dfT = df2

    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)

    # print(dfT)

    # fix data points
    def fix_data_points(id, col_name, value):
        new_value = value
        if value in dict_corrections[id][col_name]:
            new_value = dict_corrections[id][col_name][value]
        return new_value

    # print(dfT.head())
    print('\n# Fixing data points...')
    for lab_id, columns in dict_corrections.items():
        print('\t- Fixing data from: ' + lab_id)
        for column, values in columns.items():
            # print('\t- ' + column + ' (' + column + ' → ' + str(values) + ')')
            dfT[column] = dfT[column].apply(lambda x: fix_data_points(lab_id, column, x))


    # reformat dates and get ages
    dfT['date_testing'] = pd.to_datetime(dfT['date_testing'])

    # create epiweek column
    def get_epiweeks(date):
        try:
            date = pd.to_datetime(date)
            epiweek = str(Week.fromdate(date, system="cdc")) # get epiweeks
            year, week = epiweek[:4], epiweek[-2:]
            epiweek = str(Week(int(year), int(week)).enddate())
#             epiweek = str(Week.fromdate(date, system="cdc"))  # get epiweeks
#             epiweek = epiweek[:4] + '_' + 'EW' + epiweek[-2:]
        except:
            epiweek = ''
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

    # reset index
    dfT = dfT.reset_index(drop=True)

    dfT['sample_id'] = dfT['unique_id'].apply(lambda x: generate_id(x))
    key_cols = ['lab_id', 'test_id', 'test_kit', 'sample_id', 'state_code', 'location', 'sex', 'date_testing', 'epiweek', 'age',
                'FLUA_test_result', 'Ct_FluA', 'FLUB_test_result', 'Ct_FluB', 'VSR_test_result', 'Ct_VSR',
                'SC2_test_result', 'Ct_geneN', 'Ct_geneS', 'Ct_RDRP', 'META_test_result', 'RINO_test_result',
                'PARA_test_result', 'ADENO_test_result', 'BOCA_test_result', 'COVS_test_result', 'ENTERO_test_result', 'BAC_test_result']

    for col in dfT.columns.tolist():
        if col not in key_cols:
            dfT = dfT.drop(columns=[col])

    # print(dfT.columns.tolist)


    dfT = dfT[key_cols]
    dfT['date_testing'] = dfT['date_testing'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else 'XXXXX')

    # # fix test results with empty data
    # for p in pathogens.keys():
    #     dfT[p + '_test_result'] = dfT[p + '_test_result'].apply(lambda x: 'Negative' if x not in ['Negative', 'Positive'] else x)

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
