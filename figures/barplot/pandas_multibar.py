import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredText
import matplotlib
from matplotlib.ticker import MultipleLocator
import matplotlib.ticker as ticker
import matplotlib.ticker as mtick
import numpy as np
import os
import argparse

plt.rcParams['font.family'] = 'Arial'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate line plots",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", required=True, help="Configuration file in TSV format")
    args = parser.parse_args()

    config = args.config

    # path = '/Users/anderson/google_drive/ITpS/projetos_itps/sgtf_omicron/analyses/run15_20220621_sgtf/figures/barplot/'
    # os.chdir(path)
    # config = 'config_bardetection.tsv'


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

    params = load_table(config)
    params.fillna('', inplace=True)
    params = params.set_index('param')
    # print(params)

    backend = params.loc['backend', 'value']
    matplotlib.use(backend)

    input_file = params.loc['input', 'value']
    df1 = load_table(input_file)

    # filter rows
    def filter_df(df, criteria):
        print('\nFiltering rows...')
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


    filters = params.loc['filter', 'value']
    dfD = pd.DataFrame()
    if filters not in ['', None]:
        dfD = filter_df(df1, filters)
    else:
        dfD = df1
    # print(dfD.head())


    # drop columns
    ignore_cols = params.loc['ignore_cols', 'value']
    if ignore_cols not in ['', None]:
        for col in ignore_cols.split(','):
            dfD = dfD.drop(columns=col.strip())
    # print(dfD.columns.tolist())


    # define datacolumns
    y_var = params.loc['y_var', 'value']
    group = params.loc['groupby', 'value']
    datacols = sorted(list(set(dfD.columns.tolist()) - set([y_var, group])))
    dfD[datacols] = dfD[datacols].apply(pd.to_numeric, errors='coerce')

    # tick order
    tick_order = params.loc['tick_order', 'value']
    new_order = []
    if tick_order not in ['', None]:
        for label in tick_order.split(','):
            new_order.append(label.strip())
        # print(list(set([y_var, group])) + [c for c in new_order if c in dfD.columns.tolist()])
        new_order = list(set([y_var, group])) + [c for c in new_order if c in dfD.columns.tolist()]
        dfD = dfD[new_order]

    colour_scheme = params.loc['colour_scheme', 'value']
    colourmap = {}
    if '.' in colour_scheme:
        colour_by = params.loc['colour_by', 'value']
        dfA = load_table(colour_scheme)
        dfA = dfA.rename(columns={'value': colour_by})
        dfA = dfA.set_index(colour_by)
        for c in dfD[colour_by].tolist():
            if c not in colourmap and c in dfD[y_var].tolist():
                colourmap[c] = dfA.loc[c, 'hex_color']

    # plot specifications
    plot_width = float(params.loc['figsize', 'value'].split(',')[0].strip())
    plot_heigth = float(params.loc['figsize', 'value'].split(',')[1].strip())

    figsize = (plot_width, plot_heigth)

    # Share Y axis
    samey = params.loc['same_yscale', 'value']
    booly = True
    if samey.lower() == 'no':
        booly = False

    samex = params.loc['same_xscale', 'value']
    boolx = True
    if samex.lower() == 'no':
        boolx = False

    nrows = int(params.loc['nrows', 'value'])
    ncols = int(params.loc['ncols', 'value'])
    if nrows == 1 and ncols == 1:
        fig, ax = plt.subplots(figsize=figsize)
    else:
        fig, axes = plt.subplots(nrows, ncols, sharex=boolx, sharey=booly, figsize=figsize)


    # # sort values
    # sortby = params.loc['sort_by', 'value']
    # if sortby not in ['', None]:
    #     df1 = df1.sort_values(by=[group, sortby])
    # else:
    #     df1 = df1.sort_values(by=[group, y_var])
    #
    # print(df1[sortby].tolist())

    # dfD = dfD.sort_values(by=[group, y_var])
    for i, (name, df) in enumerate(dfD.groupby(group)):
        # absolute or relative scale?
        scale = params.loc['scale', 'value']
        if scale == 'relative':
            for col in datacols:
                df[col] = (df[col] / df[col].sum()) * 100

        # group scale summing up their scale
        df2 = df.groupby([y_var]).sum().sort_values(ascending=True, by=y_var)
        # print(df2)

        # transpose dataframe
        df2 = df2.transpose()
        categories = list([item for item in set(df[y_var].tolist())])
        df2 = df2[categories]
        # print(df[y_var].tolist())

        if nrows >= 2 and ncols >= 2:
            ax = axes[i // ncols][i % ncols]
        elif nrows == 1 and ncols > 1:
            ax = axes[i]
        elif nrows > 1 and ncols == 1:
            ax = axes[i]
        elif nrows == 1 and ncols == 1:
            ax = ax

        # plot
        plot_kind = params.loc['plot_kind', 'value']
        if '.' in colour_scheme:
            ax = df2.plot(ax=ax, kind=plot_kind, stacked=True, color=[colourmap[i] for i in df2.columns], edgecolor='white', lw=0.1,
                          figsize=(plot_width, plot_heigth), width=0.90, zorder=10)
        else:
            if colour_scheme in ['', None]:
                colour_scheme = 'Greys'
            ax = df2.plot(ax=ax, kind='bar', stacked=True, cmap=colour_scheme, figsize=(plot_width, plot_heigth), width=0.9, zorder=10)


        handles, labels = ax.get_legend_handles_labels()
        legend = params.loc['legend', 'value']
        if legend not in ['', None]:
            column_legend = int(params.loc['column_legend', 'value'])
            ax.legend(handles, labels, loc=legend, ncol=column_legend, fontsize=8)  # reverse both handles and labels
        else:
            ax.get_legend().remove()




        # labels
        x_label = params.loc['x_label', 'value'].replace('\\n', '\n')
        y_label = params.loc['y_label', 'value'].replace('\\n', '\n')
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        # if scale == 'absolute':
        #     tick_every = int(params.loc['tick_every', 'value'])
        #     df2 = df2.reset_index()
        #     labellist = df2['index'].tolist()[::tick_every]

        show_grid = params.loc['show_grid', 'value']
        if show_grid not in ['', None]:
            for axis in show_grid.split(','):
                ax.grid(axis=axis.strip(), zorder=0)

        tick_every = int(params.loc['tick_every', 'value'])
        # if plot_kind == 'bar':
        #     if scale == 'absolute':
        #         ax.xaxis.set_minor_locator(MultipleLocator())
        #         ax.xaxis.set_major_locator(MultipleLocator(tick_every))
        # if plot_kind == 'barh':
        if tick_every not in ['', None]:
            ax.xaxis.set_major_locator(MultipleLocator(tick_every))
            ax.xaxis.set_minor_locator(MultipleLocator(tick_every/2))


    # plt.xticks(ticklabels, rotation=90)
        # ax.set_ylim((10**-1, 10**4))

        min_y = params.loc['min_y', 'value']
        if min_y not in ['', None]:
            min_y = float(min_y)

        max_y = params.loc['max_y', 'value']
        if max_y not in ['', None]:
            max_y = float(max_y)

        log_scale = params.loc['log_scale', 'value']
        if log_scale == 'yes':
            ax.set_yscale('log')
            min_y = np.log(min_y)
            min_y = np.log(min_y)
            ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y,pos: ('{{:.{:1d}f}}'.format(int(np.maximum(-np.log10(y),0)))).format(y)))

        if str(min_y) + str(max_y) not in ['', None]:
            if min_y in ['', None]:
                ax.set_ylim(ymin=None, ymax=max_y)
            elif max_y in ['', None]:
                ax.set_ylim(ymin=min_y, ymax=None)
            else:
                ax.set_ylim(ymin=min_y, ymax=max_y)

        # if scale == 'relative':
        #     ax.set_xlim(-1, 101)
        #     ax.xaxis.set_major_formatter(mtick.PercentFormatter())

        ax.yaxis.set_tick_params(which='minor', bottom=False)

    # ax.axhline(y=np.log(1000), color='#ABABAB', linestyle='--', lw=0.5)

    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=None, hspace=None)

    if backend == 'pdf':
        plt.tight_layout()
        plt.savefig("barplot_" + config.split('.')[0].split('_')[-1] + ".pdf", format="pdf", bbox_inches="tight")
        print("\nDone!\nFile saved as: " + "barplot_" + config.split('.')[0].split('_')[-1] + ".pdf")
    else:
        plt.show()
