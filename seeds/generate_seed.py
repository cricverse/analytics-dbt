import csv
import os


series = ['IPL', 'BPL', 'BBL', 'PSL', 'WBB', 'T20 World Cup']
season = ['2024', '2023', '2022', '2021', '2020']
id = os.getenv('ID')

def generate_seed(file_path):
    with open(file_path, 'w') as file:
        for current_series, current_season in zip(series, season):
            csv.writer(file).writerow([id, current_series, current_season])

generate_seed('seeds/automated.csv')