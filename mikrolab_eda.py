import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import logging
import os
import re
import sys


def desc_num_feature(dataframe, feature_name, bins=30, edgecolor='k', **kwargs):
    fig, ax = plt.subplots(figsize=(8, 4))
    dataframe[feature_name].hist(bins=bins, edgecolor=edgecolor, ax=ax, **kwargs)
    ax.set_title(feature_name, size=15)
    plt.figtext(1, 0.15, str(dataframe[feature_name].describe().round(2)), size=17)
    plt.savefig('./diagrams/' + feature_name + '.png', dpi=300, format='png')
    return True


if __name__ == "__main__":
    configuration = {
        'logfile': './logs/eda_log',
        'source_file': 'target_directory/2018-09-22T04:30:36_office_2018-10-02T10:30:25.csv',
        'attributes': ['time', 'co2_hum', 'co2_ppm', 'co2_tmp', 'ec', 'ph', 'rpi_t', 'rtd_t', 'tsl']
    }
    logging.basicConfig(filename=configuration['logfile'], level=logging.INFO)
    source_file = configuration['source_file']

    data = pd.read_csv(source_file)

    data['co2_hum'].fillna((data['co2_hum'].mean()), inplace=True)
    data.fillna(method='ffill', inplace=True)

    # data.where(
    #     data.replace(to_replace=0, value=np.nan),
    #     other=(data.fillna(method='ffill') + data.fillna(method='bfill')) / 2
    # )

    # completing data
    data.replace(to_replace=0, value=np.nan, inplace=True)
    data.fillna(method='ffill', inplace=True)
    data.fillna(method='bfill', inplace=True)

    print("Shape of the dataset: {}".format(data.shape))
    print("Descriptive statistics: {}".format(data.describe()))

    for attribute in configuration['attributes'][1:]:
        desc_num_feature(data, attribute)

    sns.pairplot(
        data=data,
        plot_kws={"s": 2},
        diag_kind='kde'
    )
    plt.show()



    sys.exit(0)


# TODO run exploratory data analysis (KDE - density plot)
