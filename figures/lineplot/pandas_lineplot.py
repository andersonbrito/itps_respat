import matplotlib.pyplot as plt
import pandas as pd
import matplotlib
import matplotlib.ticker as mtick
from pylab import *
import os.path
from matplotlib.backends.backend_pdf import PdfPages
import argparse

# matplotlib.use('Qt5Agg')
plt.rcParams['font.family'] = 'Arial'

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate line plots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", required=True, help="Configuration file in TSV format")
    args = parser.parse_args()

    config = args.config

    # path = '/Users/Anderson/Library/CloudStorage/GoogleDrive-anderson\.brito@itps.org.br/Outros computadores/My Mac mini/google_drive/ITpS/projetos_itps/resp_pathogens/analyses/20220628_relatorio11/figures/lineplot/'
    # config = path + 'config_linepos.tsv'


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
            # df = df[~df[index].isin([''])]  # drop row with empty index
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

    categories = params.loc['categories', 'value']
    ordering = params.loc['category_order', 'value']
    # reorder
    if ordering not in ['', None]:
        order = [c.strip() for c in ordering.split(',')]
        df[categories] = pd.Categorical(df[categories], order)
        df = df.sort_values(categories)

    df.set_index(categories, inplace=True)
    all_categories = df.index.tolist()


    # drop columns
    ignore_cols = params.loc['ignore_cols', 'value']
    if ignore_cols not in ['', None]:
        for col in ignore_cols.split(','):
            df = df.drop(columns=col.strip())

    # stacked dataframe
    data = {categories: [], 'x': [], 'y': []}
    rolling_avg = params.loc['rolling_average', 'value']
    for idx, row in df.iterrows():
        if rolling_avg not in [None, '']:
            # window = int(params.loc['window', 'value'])
            rolling_window_obj = row.rolling(int(rolling_avg))
            rolling_average = rolling_window_obj.mean()
            df.loc[idx] = rolling_average

        for column in df.columns.tolist():
            valuey = df.loc[idx, column]
            valuex = column
            data[categories].append(idx)
            data['x'].append(valuex)
            data['y'].append(valuey)

    # print(data)
    df2 = pd.DataFrame(data)
    df2['y'] = df2['y'].astype(float)


    # Setup the figure size
    plot_width = float(params.loc['figsize', 'value'].split(',')[0].strip())
    plot_heigth = float(params.loc['figsize', 'value'].split(',')[1].strip())
    plt.rcParams['figure.figsize'] = (plot_width, plot_heigth)



    d_colours = {}
    colours = params.loc['colours', 'value']

    def generate_palette(list, scheme):
        cmap = cm.get_cmap(scheme, len(list))    # PiYG
        list_colour = []
        for name, colour in zip(list, range(cmap.N)):
            rgba = cmap(colour)
            # rgb2hex accepts rgb or rgba
            hex = matplotlib.colors.rgb2hex(rgba)
            list_colour.append((name, hex))
        # print(list_colour)
        return list_colour

    if os.path.isfile(colours):
        dfA = load_table(colours)
        dfA = dfA.rename(columns={'value': colours})
        dfA = dfA.set_index(colours)
        for c in df.index:
            if c not in d_colours:# and c in df2[categories]:
                d_colours[c] = dfA.loc[c, 'hex_color']
    else:
        for element in generate_palette(all_categories, colours):
            cat, hex = element
            if cat not in d_colours:# and c in df2[categories]:
                d_colours[cat] = hex

        # # colour scheme
        # colours = [c.strip() for c in params.loc['colours', 'value'].split(',')]
        # legend = df.index.tolist()
        # if len(colours) < len(legend):
        #     for n in range(len(legend) - len(colours)):
        #         colours.append('#CECECE')
        #
        # for label, hex in zip(legend, colours):
        #     d_colours[label] = hex

    # else:
    #     print('\nNo colour scheme provided. Please provide a TSV file or a string with HEX colours')

    # print(d_colours)

    # print(df2)
    # print(d_colours)
    # plot
    marker_size = float(params.loc['show_markers', 'value'])
    fig, ax = plt.subplots()
    for name in set(sorted(data[categories])):
        # print(name)
        ax.plot(df2[df2[categories] == name].x, df2[df2[categories] == name].y, label=name, marker='o', markersize=marker_size, color=d_colours[name])
        # df2.plot(x='x', y='y', label=name, marker='o', color=d_colours[name])
        # ax.plot(df[df.name == name].xvar, df[df.name == name].yvar, label=name)

    min_y = float(params.loc['min_y', 'value'])
    max_y = float(params.loc['max_y', 'value'])
    ax.set_ylim(min_y, max_y)
    # print()
    ax.set_xlim(-0.5, len(df.columns.tolist())-0.5)


    y_format = params.loc['y_format', 'value']
    if y_format == 'percentage':
        ax.yaxis.set_major_formatter(mtick.PercentFormatter(1))

    x_label = params.loc['xlabel', 'value'].replace('\\n', '\n')
    y_label = params.loc['ylabel', 'value'].replace('\\n', '\n')
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)

    # # print(data['xvar'][::3])
    ticklabels = data['x'][:int(len(data[categories]) / len(list(set(data[categories]))))]
    tick_freq = params.loc['tick_every', 'value']
    if tick_freq not in ['', None]:
        tick_every = int(tick_freq)
        ax.set_xticks(ticklabels[::tick_every])
        ax.set_xticklabels(ticklabels[::tick_every])
    plt.xticks(ticklabels, rotation=90)



    show_grid = params.loc['show_grid', 'value']
    if show_grid not in ['', None]:
        for axis in show_grid.split(','):
            ax.grid(axis=axis.strip(), zorder=0)

    show_legend = params.loc['show_legend', 'value']
    if show_legend == 'yes':
        ax.legend(loc='best')


    if backend == 'pdf':
        plt.tight_layout()
        plt.savefig("lineplot_" + config.split('.')[0].split('_')[-1] + ".pdf", format="pdf", bbox_inches="tight")
    else:
        plt.show()


