import numpy as np
import pandas as pd
import matplotlib as plt
import seaborn as sb

import logging
import os
import re
import sys

logging.basicConfig(filename='./logs/source_data_log', level=logging.DEBUG)

# data example

# time      2018-09-22T04:30:36.258478152Z
# co2_hum   78.3966064453125        float               humidified
# co2_ppm   385.881591796875        float               parts per million
# co2_tmp   12.946136474609375      float               total particulate matter
# ec        0.6602                  float
# ph        6.429                   float
# rpi_t     47.24                   float
# rtd_t     12.842                  float
# tsl       4.08                    float

# DONE read data into raw dataset
# DONE logger for the project
# DONE FileNotFoundException
# DONE read_raw_file + tests (type, size, file missing, file too short, look for separator)
# DONE detect header
# DONE read_raw_file test
# DONE adjust data format + tests
# DONE extract feature names from the header
# DONE extract data from file based on format definition from the header
# DONE produce raw dataset
# DONE unit tests (process_data)
# DONE write csv
# TODO handle cannot write file exception and write test for it
# TODO write function to read file and transform it into csv
# TODO merge records with rpi_t with all other records


def read_raw_file(path, minimal_length=4):
    raw_data = []
    try:
        fh = open(path, 'rt')
        for line in fh.readlines():
            raw_data.append(line.strip())
        if raw_data.__len__() < minimal_length:
            raise FileTooShort(minimal_length, "The file is too short to be processed")
    except FileNotFoundError as e:
        logging.error(e)
        raise FileNotFoundError
    if 'fh' in locals():
        fh.close()
    return _add_trailing_spaces(raw_data)


def process_data(raw_data):
    location, \
    feature_names, \
    dataset, \
    column_lengths, \
    indexes \
        = "", [], [], [], []

    # validate header
    if not _validate_header(raw_data[0:3]):
        raise HeaderBroken(raw_data[0:3], "Header Broken, don't know what to do!")

    # extract location
    location = _read_location(raw_data[0])

    # extract column lengths and feature names from header
    for token in _split_header(raw_data[1]):
        feature_names.append(token[0])
        column_lengths.append(''.join(token).__len__())

    # get list of indexes
    indexes = [sum(column_lengths[:i+1]) for i in range(len(column_lengths))]
    indexes.insert(0, 0)
    del indexes[-1]

    # process add data_points split by indexes, strip spaces, add to dataset
    for line in (raw_data[3:]):
        data_point = list(map(lambda x: line[slice(*x)], zip(indexes, indexes[1:] + [None])))
        dataset.append([feature.strip(' ') for feature in data_point])
    return location, feature_names, column_lengths, dataset


def write_csv_file(directory, file_prefix, location, feature_names, dataset):
    # create_filename
    filename = "_".join([dataset[0][0][:19], location, dataset[-1][0][:19]]) + ".csv"
    if file_prefix is not None:
        filename = file_prefix + "_" + filename
    path = os.path.join(directory, filename)

    # prepare_data
    current_id = 1
    final_dataset = ['ID,' + ','.join(feature_names) + '\n']
    for data_point in dataset:
        final_dataset.append(str(current_id) + ',' + ','.join(data_point) + '\n')
        current_id = current_id + 1

    # open file and write
    try:
        fh = open(path, 'w')
        for line in final_dataset:
            fh.write(line)
    except Exception as E:
        logging.error("Could not write to csv file: {}".format(str(E)))
    if 'fh' in locals():
        fh.close()
    return path

# - helper functions ---------------------------------------------------------------------------------------------------


# header validation functions
def _detect_location(line, search=re.compile('name:\s+(\S+)').search):
    return bool(search(line))


def _detect_header(line, search=re.compile('^[a-z0-9_ ]+$').search):
    return bool(search(line))


def _detect_separator(line, search=re.compile('^[- ]+$').search):
    return bool(search(line))


def _split_header(line, findall=re.compile('([a-z0-9_]+)(\s+)').findall):
    return findall(line)


def _read_location(line, findall=re.compile('name:\s+(\S+)').findall):
    return findall(line)[0]


def _validate_header(header_data):
    location_line, header_line, separator_line = header_data
    if _detect_location(location_line) & _detect_header(header_line) & _detect_separator(separator_line):
        logging.info("header ok, can proceed")
        result = True
    else:
        logging.error("Header broken, don't know what to do")
        result = False
    return result


# ammend original data
def _add_trailing_spaces(raw_data):
    max_line_lenght = max(raw_data, key=len).__len__()
    return list(map(lambda x: x.ljust(max_line_lenght), raw_data))


# - helper functions ---------------------------------------------------------------------------------------------------


if __name__ == "__main__":
    target_directory = "./target_directory"
    target_file_prefix = None
    data_directory = './datasource'
    source_file = 'influx-export.csv'
    source_file_path = os.path.join(data_directory, source_file)

    source_data = read_raw_file(source_file_path)

    for index in range(10):
        print("{}".format(source_data[index]))

    sd_location, sd_feature_names, sd_column_lengths, sd_dataset = process_data(source_data)

    print("Location: {}".format(sd_location))
    print("Feature names: {}".format(",".join(sd_feature_names)))
    print("Column lengths: {}".format(",".join(str(l) for l in sd_column_lengths)))
    print("number of data points: {}".format(sd_dataset.__len__()))
    print("last datapoint: {}".format(sd_dataset[-20:-1]))


    write_csv_file(target_directory, target_file_prefix, sd_location, sd_feature_names, sd_dataset)

    sys.exit(0)

# - exceptions ---------------------------------------------------------------------------------------------------------


class Error(Exception):
    pass


class FileTooShort(Error):
    def __init__(self, minimal_length, message):
        self.minimal_length = minimal_length
        self.message = message


class HeaderBroken(Error):
    def __init__(self, header, message):
        self.header = header
        self.message = message
