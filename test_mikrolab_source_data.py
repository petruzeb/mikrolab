import mikrolab_source_data as mk

import pytest
import os



# test reading of the source data

def test_read_raw_file_FileNotFoundError():
    path = 'nonsense'
    with pytest.raises(FileNotFoundError):
        mk.read_raw_file(path, minimal_length=4)

def test_read_raw_file_FileTooShort():
    path = './datasource/influx-export_testfile_header_only.csv'
    with pytest.raises(mk.FileTooShort):
        mk.read_raw_file(path, minimal_length=4)

def test_read_raw_file_type():
    path = './datasource/influx-export.csv'
    data = mk.read_raw_file(path, minimal_length=4)
    assert type(data) is list

def test_read_raw_file_number_of_lines():
    number_of_records = 69934
    path = './datasource/influx-export.csv'
    data = mk.read_raw_file(path, minimal_length=4)
    assert data.__len__() == number_of_records




# test separator detection
def test__detect_separator_happy_path():
    separator_line = "----                           -------           -------            -------            --                 --    ----- -----  ---"
    assert mk._detect_separator(separator_line) == True

def test__detect_separator_short_happy_path():
    separator_line = "- "
    assert mk._detect_separator(separator_line) == True

def test__detect_separator_wrong_characters():
    separator_line = "- fail -"
    assert mk._detect_separator(separator_line) == False



# test header validation functions
def test__detect_header_happy_path():
    header_line = "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl"
    assert mk._detect_header(header_line) == True

def test__detect_header_data_point():
    data_point = "2018-09-22T04:33:03.93403931Z                                                                                   47.24        "
    assert mk._detect_header(data_point) == False

def test__detect_header_tabs():
    data_point = "time      co2_hum\tco2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl"
    assert mk._detect_header(data_point) == False

def test__detect_location():
    data_point = "name: office     "
    assert mk._detect_location(data_point) == True

def test__read_location():
    line = "name: office     "
    location = "office"
    assert mk._read_location(line) == location


# test header validation
def test_validate_header_happy():
    data = [
        "name: office     ",
        "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl",
        "----                           -------           -------            -------            --                 --    ----- -----  ---"
    ]
    assert mk._validate_header(data) == True

def test_validate_header_unhappy():
    data = [
        "",
        "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl",
        "----                           -------           -------            -------            --                 --    ----- -----  ---"
    ]
    assert mk._validate_header(data) == False







# test data detector
# "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl"
# "2018-09-22T04:33:03.93403931Z                                                                                   47.24        "

# test data adjustor
def test__add_trailing_spaces_all_lines_are_the_same():
    lines = ['aaaaa', 'b', '  ', '     ssss ', ' lkjasd klj lkj    klj', 'llllll    llllll aa   ']
    adjusted_lines = mk._add_trailing_spaces(lines)
    assert max(adjusted_lines, key=len).__len__() == min(adjusted_lines, key=len).__len__()


# test features extraction
def test_extract_features_total_length():
    header_line = "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl                "
    recomposed = ""
    tokens = mk._split_header(header_line)
    for token in tokens:
        recomposed = recomposed + ''.join(token)
    assert recomposed == header_line


# test process_data
def test_process_data_broken_header():
    data = [
        "",
        "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl",
        "----                           -------           -------            -------            --                 --    ----- -----  ---"
    ]
    with pytest.raises(mk.HeaderBroken):
        mk.process_data(data)

def test_process_data_check_header_data():
    correct_header = [
        "name: nowhere",
        "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl",
        "----                           -------           -------            -------            --                 --    ----- -----  ---"
    ]
    location, feature_names, column_lengths, dataset = mk.process_data(correct_header)

    assert feature_names.__len__() == column_lengths.__len__()
    assert any(length == 0 for length in column_lengths) == False
    assert location == "nowhere"

def test_process_data_check_data_extraction():
    correct_header = [
        "name: nowhere                                                                                                                                   ",
        "time                           co2_hum           co2_ppm            co2_tmp            ec                 ph    rpi_t rtd_t  tsl                ",
        "----                           -------           -------            -------            --                 --    ----- -----  ---                ",
        "2018-09-22T04:30:36.258478152Z 78.3966064453125  385.881591796875   12.946136474609375 0.6602             6.429       12.843 0                  ",
        "2018-09-22T04:30:52.140187073Z 78.36761474609375 383.2767333984375  12.946136474609375 0.6604             6.43        12.842 0                  ",
        "2018-09-22T04:31:03.339845297Z                                                                                  47.24                           ",
        "2018-09-22T04:31:07.972184149Z 78.387451171875   380.22943115234375 12.903411865234375 0.6602             6.43        12.841 0                  ",
        "2018-09-22T04:31:23.963548844Z 78.4088134765625  378.1479187011719  12.91943359375     0.6601             6.425       12.841 0                  ",
        "2018-09-22T04:31:39.820769828Z 78.38287353515625 376.2395324707031  12.932785034179688 0.6603             6.424       12.84  0                  ",
        "2018-09-22T04:31:55.6603789Z   78.363037109375   374.60919189453125 12.932785034179688 0.6601             6.43        12.84  0                  "
    ]
    location, feature_names, column_lengths, dataset = mk.process_data(correct_header)
    assert dataset.__len__() == 7
    # check if all elements have the same size
    for data_point in dataset:
        print(data_point)
        assert len(data_point) == 9

# writing result file

def test_write_csv_file_happy():
    file_prefix = "test"
    target_directory = './target_directory'
    sd_location = 'office'
    sd_feature_names = ['time', 'co2_hum', 'co2_ppm' ,'co2_tmp' , 'ec', 'ph', 'rpi_t', 'rtd_t','tsl']
    sd_dataset = [
        ['2018-10-02T10:25:03.545966883Z', '', '', '', '', '', '49.39', '', ''],
        ['2018-10-02T10:25:23.91387797Z', '75.76904296875', '346.5413513183594', '13.536277770996094', '0.7062', '', '', '11.685', '50.265600000000006'],
        ['2018-10-02T10:25:38.345693689Z', '75.77362060546875', '344.627685546875', '13.509574890136719', '0.7065', '', '', '11.686', '52.8768'],
        ['2018-10-02T10:25:52.789748318Z', '75.81024169921875', '345.4836120605469', '13.493553161621094', '0.7062999999999999', '', '', '11.688', '55.48799999999998'],
        ['2018-10-02T10:26:03.308554274Z', '', '', '', '', '', '49.39', '', ''],
        ['2018-10-02T10:26:07.108125486Z', '75.80413818359375', '343.6539611816406', '13.509574890136719', '0.7062', '', '', '11.688', '54.59039999999998'],
        ['2018-10-02T10:26:21.426398949Z', '75.7965087890625', '343.8226623535156', '13.480201721191406', '0.7062999999999999', '', '', '11.689', '57.201599999999985'],
        ['2018-10-02T10:26:35.79439478Z', '75.8148193359375', '342.4741516113281', '13.466850280761719', '0.7062999999999999', '', '', '11.69', '59.81279999999999'],
        ['2018-10-02T10:26:50.117619042Z', '75.8331298828125', '343.575439453125', '13.466850280761719', '0.7062999999999999', '', '', '11.691', '58.91519999999999'],
        ['2018-10-02T10:27:03.026856123Z', '', '', '', '', '', '49.39', '', ''],
        ['2018-10-02T10:27:04.474746339Z', '75.81939697265625', '342.6052551269531', '13.453498840332031', '0.7062999999999999', '', '', '11.693', '61.526399999999995'],
        ['2018-10-02T10:27:33.226286822Z', '75.84686279296875', '338.2898254394531', '13.437477111816406', '0.7061000000000001', '', '', '11.696', '64.34159999999999'],
        ['2018-10-02T10:28:02.00079631Z', '75.8544921875', '339.5597229003906', '13.424125671386719', '0.7062', '', '', '11.701', '66.0552'],
        ['2018-10-02T10:28:03.906960762Z', '', '', '', '', '', '49.39', '', ''],
        ['2018-10-02T10:28:16.314565716Z', '75.909423828125', '338.3568420410156', '13.410774230957031', '0.706', '', '', '11.702', '68.6664'],
        ['2018-10-02T10:28:30.705294724Z', '75.88653564453125', '340.33258056640625', '13.394752502441406', '0.7062', '', '', '11.704', '70.17599999999996'],
        ['2018-10-02T10:28:45.118231121Z', '75.88653564453125', '338.07379150390625', '13.394752502441406', '0.7062', '', '', '11.707', '71.68559999999998'],
        ['2018-10-02T10:29:03.706146919Z', '', '', '', '', '', '48.85', '', ''], ['2018-10-02T10:30:03.330479583Z', '', '', '', '', '', '48.31', '', '']
    ]
    file = mk.write_csv_file(target_directory, file_prefix, sd_location, sd_feature_names, sd_dataset)
    assert file == './target_directory/test_2018-10-02T10:25:03_office_2018-10-02T10:30:03.csv'
    os.remove(file)



