import numpy as np
import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap, BoundaryNorm
from matplotlib.ticker import FuncFormatter
import matplotlib as mpl
import argparse

plt.rcParams['font.family'] = 'Arial'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate heatmaps",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", required=True, help="Configuration file in TSV format")
    args = parser.parse_args()

    config = args.config

    # path = '/Users/anderson/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/20220628_relatorio10/figures/heatmap/'
    # config = 'config_posrate_weeks_SC2demogfull.tsv'


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
            print(file)
            print('Wrong file format. Compatible file formats: TSV, CSV, XLS, XLSX')
            exit()
        return df

    params = load_table(config)
    params.fillna('', inplace=True)
    params = params.set_index('param')
    # print(params)

    backend = params.loc['backend', 'value']
    matplotlib.use(backend)

    # Load data
    input_file = params.loc['input', 'value']
    df = load_table(input_file)

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
    filters = params.loc['filter', 'value']
    if filters not in ['', None]:
        df = filter_df(df, filters)

    categories = params.loc['yvar', 'value']
    ordering = params.loc['category_order', 'value']
    # reorder
    if ordering not in ['', None]:
        order = [c.strip() for c in ordering.split(',')]
        df = df[df[categories].isin(order)]
        df[categories] = pd.Categorical(df[categories], order)
        df = df.sort_values(categories)

    df.set_index(categories, inplace=True)
    all_categories = df.index.tolist()

    # drop columns
    ignore_cols = params.loc['ignore_cols', 'value']
    if ignore_cols not in ['', None]:
        dropped = []
        for col in ignore_cols.split(','):
            if col.strip() in df.columns.tolist():
                dropped.append(col.strip())
        df = df.drop(columns=dropped)

    # convert to numeric
    df.fillna(-1, inplace=True)
    values = df.to_numpy(dtype=float)

    # plot dimensions
    plot_width = int(params.loc['figsize', 'value'].split(',')[0].strip())
    plot_heigth = int(params.loc['figsize', 'value'].split(',')[1].strip())


    # data = data.drop(columns=['order', 'region', 'division'])
    # data.set_index(index, inplace=True)
    # print(data)


    colours = params.loc['colours', 'value']
    bins = params.loc['bins', 'value']

    if colours.startswith('#'):
        cols = [c.strip() for c in colours.split(',')]
        bins = [float(b.strip()) for b in bins.split(',')]
        bounds = np.append(bins, bins[-1] + 1)
        cmap = ListedColormap(cols)
        norm = BoundaryNorm(bounds, ncolors=len(cols))
    else:
        bins = int(bins)
        cmap = sns.color_palette(sns.color_palette(colours, bins))#, as_cmap=True)


    fig, ax = plt.subplots(figsize=(plot_width, plot_heigth), facecolor='w')

    # ax = sns.heatmap(values, cmap=cmap, vmin=0, vmax=0.8, square=False, annot=False,
    #                  cbar_kws={"orientation": "horizontal"})
    # ax = sns.heatmap(data, cmap=cmap, vmin=0, vmax=0.05, square=True, cbar=False, mask=values < 0, annot=True,
    #                  cbar_kws={"orientation": "horizontal"})

    min_value = float(params.loc['min_value', 'value'])
    max_value = float(params.loc['max_value', 'value'])
    colorbar = True if params.loc['show_legend', 'value'] == 'yes' else False
    annot = True if params.loc['show_annotations', 'value'] == 'yes' else False
    label_format = params.loc['label_format', 'value']

    if colours.startswith('#'):
        cmap3 = mpl.colors.ListedColormap(['white']) # blank values
        # cmap3.set_bad("black")
        # sns.heatmap(values, mask=values > 0, cmap=cmap3, cbar=False, linewidths=0.1, linecolor=None)

        # cmap2 = mpl.colors.ListedColormap(['#808080']) # zero values
        # sns.heatmap(values, mask=values != 0, cmap=cmap2, cbar=False, linewidths=0.1, linecolor=None)

        if label_format == 'percentage':
            format = lambda x, pos: '{:.1%}'.format(x) # true data
        elif label_format == 'integer':
            format = lambda x, pos: '{:1}'.format(x) # true data
        else:
            format = lambda x, pos: '{:.1}'.format(x) # true data

        sns.heatmap(values, xticklabels=df.columns, yticklabels=df.index, cbar=colorbar,
                    annot_kws={"fontsize":10}, cmap=cmap, norm=norm,
                    cbar_kws={"orientation": "vertical", 'ticks': bins, 'format': FuncFormatter(format)},
                    mask=values < 1e-3, annot=annot, fmt='.0%', linewidths=0.05, linecolor="#CECECE")
    else:
        sns.heatmap(values, xticklabels=df.columns, yticklabels=df.index, vmin=min_value, vmax=max_value,
                    linewidths=0.05, linecolor="#CECECE", annot_kws={"fontsize":10}, cmap=cmap, cbar=colorbar,
                    cbar_kws={"orientation": "vertical"}, mask=values < 0, annot=annot, fmt='.0%')

        # cmap3 = mpl.colors.ListedColormap(['white']) # blank values
        # sns.heatmap(values, mask=values > 0, cmap=cmap3, cbar=False, linewidths=0.1, linecolor="#CECECE")

    plt.tick_params(axis='both', which='major', labelsize=10, labelbottom=True, labeltop=False, bottom=False, top=False)


    # ax = sns.heatmap(data, cmap=cmap, vmin=0, vmax=0.05, square=True, cbar=False, mask=values < 0, annot=False,
    #                  cbar_kws={"orientation": "horizontal"})

    # ax.set_facecolor('g')

    # turn the axis label
    for item in ax.get_yticklabels():
        item.set_rotation(0)

    for item in ax.get_xticklabels():
        item.set_rotation(90)

    if backend == 'pdf':
        plt.tight_layout()
        plt.savefig("heatmap_" + config.split('.')[0].split('_')[-1] + ".pdf", format="pdf", bbox_inches="tight")
    else:
        plt.show()





