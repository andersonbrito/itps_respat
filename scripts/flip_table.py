import pandas as pd
from pylab import *

matplotlib.use('Qt5Agg')
plt.rcParams['font.family'] = 'Arial'

path = '/Users/anderson/GLab Dropbox/Anderson Brito/ITpS/projetos_itps/sgtf_omicron/analyses/run6_20220131_sgtf/figures/donnut/'
input = path + 'matrix_states_detection_week.tsv'
filter_c = 'DS_UF_SIGLA, 2022_EW03'
filter_r = ''

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


# load data
df1 = load_table(input)
df1.fillna('', inplace=True)

# filter
def filter_rows(df, criteria):
    new_df = pd.DataFrame()
    for filter_value in sorted(criteria.split(',')):
        filter_value = filter_value.strip()
        col, val = filter_value.split(':')[0], filter_value.split(':')[1]
        if not filter_value.startswith('~'):
            df_filtered = df[df[col].isin([val])]
            new_df = new_df.append(df_filtered)

    for filter_value in sorted(criteria.split(',')):
        filter_value = filter_value.strip()
        if filter_value.startswith('~'):
            filter_value = filter_value[1:]
            col, val = filter_value.split(':')[0], filter_value.split(':')[1]
            if new_df.empty:
                df = df[~df[col].isin([val])]
                new_df = new_df.append(df)
            else:
                new_df = new_df[~new_df[filter_value.split(':')[0]].isin([filter_value.split(':')[1]])]
    return new_df


def filter_cols(df, criteria):
    print(criteria)
    newdf = df
    keep_cols = []
    drop_cols = []
    for col in criteria.split(','):
        col = col.strip()
        # print(col)
        if col[0] == '~':
            drop_cols.append(col[1:])
        else:
            keep_cols.append(col)

    if len(keep_cols) > 0:
        print(vars, keep_cols)
        newdf = newdf[keep_cols]
    if len(drop_cols) > 0:
        newdf = newdf.drop(columns=drop_cols)
    return newdf

# load data
if filter_r not in ['', None]:
    df1 = filter_rows(df1, filter_r)

# drop columns
if filter_c not in ['', None]:
    df1 = filter_cols(df1, filter_c)

print(df1.head())


df1 = df1.transpose()

print(df1.head())
