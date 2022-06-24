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
    parser.add_argument("--output", required=True, help="TSV file aggregating all columns listed in the 'rename file'")
    args = parser.parse_args()

    path = os.path.abspath(os.getcwd())
    input_folder = path + '/' + args.datadir + '/'
    rename_file = args.rename
    correction_file = args.correction
    output = args.output

    # path = '/Users/anderson/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/20220314_relatorio3/'
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
    def fix_datatable(dfL, lab, file):
        dfN = dfL
        if lab == 'DB Molecular':
            pathogens = {'FLUA': ['FLUARV'], 'FLUB': ['FLUBRV'], 'VSR': ['RSVRV'], 'SC2': ['NGRV', 'SGRV', 'RDRPGRV', 'EGENERV', 'NGENERV'],
                         'META': [], 'RINO': [], 'PARA': [], 'ADENO': [], 'BOCA': [], 'COVS': [], 'ENTERO': [], 'BAC': []}
            dfN = pd.DataFrame()
            unique_cols = list(set(dfL.columns.tolist()))
            # dfL['Parametro'] = dfL['Parametro'].apply(lambda x: 'MS2' if x == 'MS2CI' else x)

            controls = ['ZZFLUA', 'ZZFLUB', 'ZZRSV', 'ZZSARS']
            dfL = dfL[~dfL['Parametro'].isin(controls)]
            for i, (code, dfR) in enumerate(dfL.groupby('NumeroPedido')):
                data = {} # one data row for each request
                for col in unique_cols:
                    data[col] = dfR[col].tolist()[0]

                target_pathogen = {}
                for p, t in pathogens.items():
                    data[p + '_test_result'] = 'Not tested'
                    for g in t:
                        target_pathogen[g] = p
                dfR['Parametro'] = dfR['Parametro'].str.replace('NGENERV', 'NGRV')
                dfR['pathogen'] = dfR['Parametro'].apply(lambda x: target_pathogen[x])

                genes = {'FLUARV':40.0, 'FLUBRV':40.0, 'RSVRV':40.0, 'NGRV':40.0, 'SGRV':40.0, 'RDRPGRV':40.0,
                         'EGENERV':40.0}
                found = []
                # not_performed = [g for g in genes if g not in list(set(dfG['Parametro'].tolist()))]
                test_targets = dfR['Parametro'].tolist()
                # if len(test_targets) > 6:
                # print('\n\n>' + str(i) + '. ' + str(code) + ' >>> ' + str(len(dfG.index)))
                # print(not_performed)
                # line = '\n\n>' + str(i) + ') ' + str(code) + ' Number of targets: ' + str(len(dfR.index))
                # print(line)
                # outfile2.write(line)

                for virus, dfG in dfR.groupby('pathogen'):
                    # print('>>> Test for', virus)
                    for idx, row in dfG.iterrows():
                        gene = dfG.loc[idx, 'Parametro']
                        ct_value = dfG.loc[idx, 'ResultadoLIS']
                        result = '' # to be determined
                        if gene in genes:
                            found.append(gene)
                            if gene not in data:
                                data[gene] = '' # assign target
                                if ct_value != '': # Ct value exists, fix inconsistencies
                                    if '.' in ct_value:
                                        ct_value = ct_value.replace('.', '')
                                        if len(ct_value) < 5:
                                            ct_value = ct_value + '0'*(5-len(ct_value))

                                    ct_value = float(ct_value)/1000
                                    if ct_value > 50:
                                        ct_value = ct_value/10

                                    ct_value = np.round(ct_value, 2)
                                    data[gene] = str(ct_value) # assign corrected Ct value

                                    if ct_value < genes[gene]:
                                        result = 'DETECTADO'
                                        data[virus + '_test_result'] = result
                                    else: # if Ct is too high
                                        print('Ct too high for gene', gene)
                                        result = 'NÃO DETECTADO'
                                        data[virus + '_test_result'] = result
                                    # print('\t * ' + gene + ' (' + str(ct_value) + ') = ' + data[virus + '_test_result'])

                                else: # if no Ct is reported
                                    result = 'NÃO DETECTADO'
                                    # print('\t - ' + gene + ' (' + str(ct_value) + ') = ' + data[virus + '_test_result'])
                                    if data[virus + '_test_result'] != 'DETECTADO':
                                        data[virus + '_test_result'] = result

                                # fix multi-target result
                                if virus == 'SC2':
                                    if data[virus + '_test_result'] != 'DETECTADO':
                                        if result == 'DETECTADO':
                                            data[virus + '_test_result'] = result # fix wrong result, in case at least one target is detected
                                            # print('\t ** ' + gene + ' (' + str(ct_value) + ') = ' + data[virus + '_test_result'])
                            else:
                                line2 = str(code) + '\t' + gene + '\t' + str(ct_value) + '\t' + dfG.loc[idx, 'Resultado'] + '\t' + str(len(dfR.index)) + '\t' + file + '\n'
                                # print(line2)
                                # outfile2.write(line2)
                                if data[virus + '_test_result'] != 'DETECTADO':
                                    # print('duplicate?')
                                    if result == 'DETECTADO':
                                        for p, t in pathogens.items():
                                            if gene in t:
                                                data[virus + '_test_result'] = result # get result as shown in original file
                                                print('\t *** ' + gene + ', Ct = (' + str(ct_value) + ') = ' + data[virus + '_test_result'])
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


        elif lab == 'HLAGyn':
            pathogens = {'FLUA': ['VIRUS_Influenza A', 'VIRUS_Influenza H1N1', 'VIRUS_Influenza H3', 'Vírus Influenza A'], 'FLUB': ['VIRUS_Influenza B', 'Vírus Influenza B'],
                         'META': ['VIRUS_Metapneumovírus'], 'VSR': ['VIRUS_Sincicial A', 'VIRUS_Sincicial B', 'Vírus Sincicial Respiratório A/B'], 'RINO': ['VIRUS_Rinovírus'],
                         'PARA': ['VIRUS_Parainfluenza 1', 'VIRUS_Parainfluenza 2', 'VIRUS_Parainfluenza 3', 'VIRUS_Parainfluenza 4'],
                         'ADENO': ['VIRUS_Adenovirus'], 'BOCA': ['VIRUS_Bocavirus'],
                         'COVS': ['VIRUS_CoV-229E', 'VIRUS_CoV-HKU', 'VIRUS_CoV-NL63', 'VIRUS_CoV-OC43'],
                         'SC2': ['VIRUS_SARS_Like', 'VIRUS_SARS-CoV-2', 'Coronavírus SARS-CoV-2'], 'ENTERO': ['VIRUS_Enterovírus'],
                         'BAC': ['BACTE_Bordetella pertussis', 'BACTE_Bordetella parapertussis', 'BACTE_Mycoplasma pneumoniae']}
            dfN = pd.DataFrame()
            unique_cols = list(set(dfL.columns.tolist()))
            for idx, row in dfL.iterrows():
                data = {'Ct_FluA': '', 'Ct_FluB': '', 'Ct_VSR': '', 'Ct_RDRP': '', } # one data row for each request
                for col in unique_cols:
                    value = dfL.loc[idx, col]
                    # data[col] = dfL[col].tolist()[0]
                    data[col] = value

                target_pathogen = {}
                for pat, tests in pathogens.items():
                    # print('\n' + pat, tests)
                    # pedido = data['Pedido']
                    others = []
                    data[pat + '_test_result'] = 'Not tested'
                    # for gene in test:
                    #     target_pathogen[gene] = pat

                    for target in tests:
                        # print(pedido, target, data[target])
                        if target in data:
                            if data[target] == 'Detectado':
                                if data[pat + '_test_result'] in ['Not tested', 'Não Detectado']:
                                    # print('\t\t\t >>>' + target, data[target], '*', data[pat + '_test_result'])
                                    data[pat + '_test_result'] = 'Detectado'
                                    # print(pat + '_test_result', 'fixed >>>', data[pat + '_test_result'])
                            elif data[target] == 'Não Detectado':
                                if data[pat + '_test_result'] == 'Not tested':
                                    # print('\t\t\t >>>' + target, data[target], '*', data[pat + '_test_result'])
                                    data[pat + '_test_result'] = 'Não Detectado'
                                    # print(pat + '_test_result', 'fixed >>>', data[pat + '_test_result'])
                            else:
                                pass # if neither "Detectado" nor "Não Detectado", live it as is ("Not tested")

                dfN = dfN.append(data, ignore_index=True)


        elif lab == 'DB Molecular_2':
            dfN = pd.DataFrame()
            pathogens = {'FLUA': [], 'FLUB': [], 'VSR': [], 'SC2': ['NGENE', 'SGENE'], 'META': [], 'RINO': [],
                         'PARA': [], 'ADENO': [], 'BOCA': [], 'COVS': [], 'ENTERO': [], 'BAC': []}

            for i, (code, dfG) in enumerate(dfL.groupby('NumeroPedido')):
                # print('>' + str(i))
                data = {'Ct_FluA': '', 'Ct_FluB': '', 'Ct_VSR': '', 'Ct_RDRP': '', } # one data row for each request
                unique_cols = [col for col in dfG.columns.tolist() if len(list(set(dfG[col].tolist()))) == 1]
                for col in unique_cols:
                    data[col] = dfG[col].tolist()[0]

                # target_pathogen = {}
                for p, t in pathogens.items():
                    if p != 'SC2':
                        data[p + '_test_result'] = 'Not tested'

                # fix Ct values
                genes = pathogens['SC2']
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

                dfN = dfN.append(data, ignore_index=True)


        elif lab == 'DASA':
            pathogens = {'FLUA': ['FLUA'], 'FLUB': ['FLUB'], 'VSR': ['VSR'], 'SC2': ['COVID'],
                         'META': [], 'RINO': [], 'PARA': [], 'ADENO': [], 'BOCA': [], 'COVS': [], 'ENTERO': [], 'BAC': []}
            dfN = pd.DataFrame()
            unique_cols = list(set(dfL.columns.tolist()))

            for i, (code, dfR) in enumerate(dfL.groupby('codigorequisicao')):
                data = {'Ct_FluA': '', 'Ct_FluB': '', 'Ct_VSR': '', 'Ct_RDRP': '', } # one data row for each request

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

        elif lab == 'DASA_2':
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
                for filename in os.listdir(input_folder + element):
                    if filename.split('.')[-1] in ['tsv', 'csv', 'xls', 'xlsx'] and filename[0] not in ['~', '_']:
                        print('\t- File: ' + filename)
                        df = load_table(input_folder + element + filename)
                        df.fillna('', inplace=True)
                        df.reset_index(drop=True)
                        df = fix_datatable(df, id, filename) # reformat datatable
                        df.insert(0, 'lab_id', id)
                        df = rename_columns(id, df) # fix data points
                        frames = [dfT, df]
                        df2 = pd.concat(frames)
                        dfT = df2

    dfT = dfT.reset_index(drop=True)
    dfT.fillna('', inplace=True)

    # print(dfT)

    # print(dfT[['test_id', 'FLUA_test_result', 'FLUB_test_result', 'VSR_test_result', 'SC2_test_result',
    # 'META_test_result', 'RINO_test_result', 'PARA_test_result', 'ADENO_test_result', 'BOCA_test_result',
    # 'COVS_test_result', 'ENTERO_test_result', 'BAC_test_result']])

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

#     for idx, row in dfT.iterrows():
#         test = dfT.loc[idx, 'date_testing']
#         test_id = dfT.loc[idx, 'test_id']
#         lab_id = dfT.loc[idx, 'lab_id']
#         print(lab_id, test_id, test)
#     print('Done!')
    
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
        test_id = dfT.loc[idx, 'test_id']
        lab_id = dfT.loc[idx, 'lab_id']
#         age = dfT.loc[idx, 'age']
        try:
           if birth not in [np.nan, '', None, pd.NaT] and test not in [np.nan, '', None, pd.NaT]:
#                print(lab_id, test_id, birth, type(birth), test, type(test), age, type(age))
               birth = pd.to_datetime(birth)
               age = (test - birth) / np.timedelta64(1, 'Y')
               dfT.loc[idx, 'age'] = np.round(age, 1)
        except:
            print('\nAn issue was found in this sample:')
            print('\n\t- Lab ID = ' + lab_id)
            print('\t- Test ID = ' + test_id)
            print('\t- Test date = ' + test)
#             print('\t- Age = ' + age)
            print('\t- Birth date = ' + birth)


    # fix sex information
    dfT['sex'] = dfT['sex'].apply(lambda x: x[0] if x != '' else x)

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
    key_cols = ['lab_id', 'test_id', 'sample_id', 'state_code', 'location', 'sex', 'date_testing', 'epiweek', 'age',
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
