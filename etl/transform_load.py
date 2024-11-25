import os
import pandas as pd
from functools import reduce

def create_separate_dataframes(directory) -> dict:
    dataframes = {}
    dataframes_room = {}
    columns = ['co2', 'humidity', 'light', 'pir', 'temperature']

    count2 = 0
    for filename in os.listdir(directory):
        new_directory = directory + '/' + filename
        print(new_directory)
        count = 0
        for new_files in os.listdir(new_directory):
            f = os.path.join(new_directory, new_files)
            my_path = new_directory.split('/')[-1] + '_' + new_files.split('.')[0] # e.g. 656_co2
            dataframes[my_path] = pd.read_csv(f, names=['ts_min_bignt', columns[count]]) 
            count += 1
            count2 += 1

        dataframes_room[filename] = reduce(lambda left, right:
                         pd.merge(left, right, on='ts_min_bignt', how='inner'),
                         [dataframes[f'{filename}_co2'], dataframes[f'{filename}_humidity'], \
                          dataframes[f'{filename}_light'], dataframes[f'{filename}_pir'],\
                          dataframes[f'{filename}_temperature']])
        dataframes_room[filename]['room'] = filename

    return dataframes_room


def create_main_dataframe(separate_dataframes:dict):
    """
    Concats all per-room dataframes vertically. Creates final dataframe.
    """
    dataframes_to_concat = []

    for i in separate_dataframes.values():
        dataframes_to_concat.append(i)

    df = pd.concat(dataframes_to_concat, ignore_index=True)
    df = df.sort_values('ts_min_bignt') # All data is sorted according to ts_min_bignt. We want it to stream according to timestamp.
   
    df.dropna(inplace=True)
    df["event_ts_min"] = pd.to_datetime(df["ts_min_bignt"], unit='s') # Create datetime column\
    df.drop(columns = ['ts_min_bignt'])
    df['if_movement'] = df["pir"].apply(lambda x: 'movement' if x > 0 else 'no_movement')
    return df


def write_main_dataframe(df, data_generator_path):
    if not os.path.exists(data_generator_path):
        os.makedirs(os.path.dirname(data_generator_path), exist_ok=True)

    # Ensure the output file has write permissions
    df = df[['event_ts_min',
            'co2',
            'humidity',
            'light',
            'pir',
            'temperature',
            'room',
            'if_movement']]

    # Write the dataframe to CSV
    df.to_csv(data_generator_path + "/sensors.csv", index=False)

def transform_load(directory, data_generator_path):
    all_dataframes = create_separate_dataframes(directory)
    main_dataframe = create_main_dataframe(all_dataframes)
    write_main_dataframe(main_dataframe, data_generator_path)