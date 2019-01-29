import boto3
import botocore
import logging
import os
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import mikrolab_source_data as mikrolab

logging.basicConfig(filename='./logs/aws-mikrolab', level=logging.INFO)


def get_data_from_s3(bucket, source_file_name, target_file_name):
    BUCKET_NAME = bucket
    KEY = source_file_name  # replace with your object key
    s3 = boto3.resource('s3')
    try:
        s3.Bucket(BUCKET_NAME).download_file(KEY, target_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    return True

def upload_converted_data(bucket, source_directory):
    print("uploading converted data to AWS")
    logging.info("uploading converted data to AWS")
    s3 = boto3.client('s3')
    files = os.listdir(source_directory)
    filename = files[0]
    source_file = os.path.join(source_directory, filename)
    target_file = filename
    s3.upload_file(source_file, bucket, target_file)
    return True

def upload_charts(bucket, charts_directory):
    print("uploading converted data to AWS")
    logging.info("uploading converted data to AWS")
    s3 = boto3.client('s3')
    for filename in os.listdir(charts_directory):
        source_file = os.path.join(charts_directory, filename)
        target_file = filename
        print("uploading file: " + filename + " data to AWS")
        logging.info("uploading file: " + filename + " data to AWS")
        s3.upload_file(source_file, bucket, target_file)
    return True

def build_dataset(source_directory):
    print("building dataset")
    logging.info("building dataset")
    files = os.listdir(source_directory)
    filename = files[0]
    source_file = os.path.join(source_directory, filename)
    data = pd.read_csv(source_file)
    data.drop(['ID'], axis=1, inplace=True)
    data.time = pd.to_datetime(data.time)
    data.set_index('time', inplace=True)
    data.replace(to_replace=0, value=np.nan, inplace=True)
    data.fillna(method='ffill', inplace=True)
    data.fillna(method='bfill', inplace=True)
    return data

def hourly_resampling(data, charts_directory):
    print("resampling data")
    logging.info("resampling data")
    hourly = data.resample('H').sum()
    # hourly.plot(['ec', 'ph', 'rpi_t'], style=[':', '--', '-'])
    hourly.plot(style=[':', '--', '-'])
    resampled_chart_file = os.path.join(charts_directory, 'eda_mikrolab_hourly_resampling.png')
    plt.savefig(resampled_chart_file, dpi=600, format='png')

def desc_num_feature(dataframe, feature_name, directory, bins=30, edgecolor='k', **kwargs):
    print("plotting " + feature_name)
    logging.info("plotting " + feature_name)
    fig, ax = plt.subplots(figsize=(8, 4))
    dataframe[feature_name].hist(bins=bins, edgecolor=edgecolor, ax=ax, **kwargs)
    ax.set_title(feature_name, size=15)
    plt.figtext(1, 0.15, str(dataframe[feature_name].describe().round(2)), size=17)
    filename = os.path.join(directory, feature_name + '.png')
    plt.savefig(filename, dpi=300, format='png')
    return True

def correlation_chart(dataframe, directory):
    print("plotting correlations")
    logging.info("plotting correlations")
    sns.pairplot(
        data=dataframe[['co2_hum', 'co2_ppm', 'co2_tmp', 'rpi_t', 'rtd_t', 'tsl']],
        plot_kws={"s": 2},
        diag_kind='kde'
    )
    filename = os.path.join(directory, 'eda_mikrolab_selective_kde.png')
    plt.savefig(filename, dpi=300, format='png')


# DONE - download and store data from S3
# DONE - upload file to S3
# DONE EDA and upload charts to AWS S3 bucket

if __name__ == '__main__':

    source_file_name = 'influx-export.csv'

    source_bucket       = 'ie-mikrolab-raw-data'
    converted_bucket    = 'ie-mikrolab-converted-data'
    eda_bucket          = 'ie-mikrolab-eda'

    target_directory = "./aws-converted"
    target_file_prefix = None
    data_directory = './aws-datasource'
    charts_directory = './aws-diagrams'
    # source_file = 'influx-export-short.csv'
    source_file = 'influx-export.csv'
    attributes = ['co2_hum', 'co2_ppm', 'co2_tmp', 'ec', 'ph', 'rpi_t', 'rtd_t', 'tsl']

    print("Downloading data from AWS S3")
    logging.info("Downloading data from AWS S3")
    get_data_from_s3(source_bucket, source_file_name, os.path.join(data_directory, source_file))

    source_file_path = os.path.join(data_directory, source_file)
    source_data = mikrolab.read_raw_file(source_file_path)

    for index in range(10):
        print("{}".format(source_data[index]))

    sd_location, sd_feature_names, sd_column_lengths, sd_dataset = mikrolab.process_data(source_data)

    print("Location: {}".format(sd_location))
    print("Feature names: {}".format(",".join(sd_feature_names)))
    print("Column lengths: {}".format(",".join(str(l) for l in sd_column_lengths)))
    print("number of data points: {}".format(sd_dataset.__len__()))
    print("last datapoint: {}".format(sd_dataset[-20:-1]))

    mikrolab.write_csv_file(target_directory, target_file_prefix, sd_location, sd_feature_names, sd_dataset)
    upload_converted_data(converted_bucket, target_directory)

    # read dataset
    dataset = build_dataset(target_directory)
    # create charts
    hourly_resampling(dataset, charts_directory)
    for attribute in attributes:
        desc_num_feature(dataset, attribute, charts_directory)
    correlation_chart(dataset, charts_directory)

    print(dataset.corr())
    logging.info(dataset.corr())

    # upload charts
    upload_charts(eda_bucket, charts_directory)

    sys.exit(0)