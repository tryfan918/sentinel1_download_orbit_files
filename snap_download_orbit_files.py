#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Dr. Huw T. Mithan
"""

import argparse
import os
import glob
import zipfile
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
import re
import requests
import logging
import pprint

# Define the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create a file handler to save log messages to a file
file_handler = logging.FileHandler('SentinelOrbitDownloader.log')

# Create a console handler to display log messages in the console
console_handler = logging.StreamHandler()

# Define a log message format
log_format_long = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_format_short = logging.Formatter('%(message)s')
file_handler.setFormatter(log_format_long)
#console_handler.setFormatter(log_format)

# Add the handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)


class SentinelOrbitDownloader:

    def __init__(self, data_directory, orbit_pass_name, insar_processor):

        self.slc_directory = os.path.join(data_directory, orbit_pass_name, "slc")
        self.orbit_files_webpage = "https://s1qc.asf.alaska.edu/aux_poeorb/"
        self.target_directory = os.path.expanduser("~/.snap/auxdata/Orbits/Sentinel-1/POEORB")
        self.NUM_TIMESTAMPS = 2
        self.CHUNK_DIVISOR = 4
        self.EOF_DATE_FORMAT = "%Y%m%dT%H%M%S"
        self.MAX_WORKERS = 10
        self.isce_orbits_directory = os.path.join(data_directory, orbit_pass_name,'orbits')
        self.insar_processor = insar_processor

    def find_satellite_names(self):
        zip_files = glob.glob(os.path.join(self.slc_directory, "*.zip"))
        satellite_names = set()
        for file in zip_files:
            if "S1A" in file:
                satellite_names.add("S1A")
            if "S1B" in file:
                satellite_names.add("S1B")
        logger.info("Satellite names found: %s", satellite_names)

        return list(satellite_names)

    def download_and_zip_snap(self, eof, acquisition_date):
        eof_url = f"{self.orbit_files_webpage}/{eof}"
        response = requests.get(eof_url)

        if response.status_code == 200:
            # Read the file content into a BytesIO object
            file_content = BytesIO(response.content)

            satellite_name = eof.split('_')[0]
            year, month = acquisition_date[:4], acquisition_date[4:6]
            dir_path = os.path.join(self.target_directory, satellite_name, year, month)
            os.makedirs(dir_path, exist_ok=True)

            zip_file_path = os.path.join(dir_path, f"{os.path.basename(eof)}.zip")
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(os.path.basename(eof), file_content.read())

            logger.info(f"Created ZIP file:\n {zip_file_path}")
        else:
            logger.error(f"Failed to download {eof_url}. Status code: {response.status_code}")
            logger.exception("Exception details:")

    def download_and_zip_isce(self, eof, acquisition_date):
        eof_url = f"{self.orbit_files_webpage}/{eof}"
        response = requests.get(eof_url)

        if response.status_code == 200:
            # Read the file content into a BytesIO object
            file_content = BytesIO(response.content)
            dir_path = self.isce_orbits_directory
            os.makedirs(dir_path, exist_ok=True)

            # Save the file content to a file in the local system
            orbit_file_path = os.path.join(dir_path, os.path.basename(eof))
            with open(orbit_file_path, 'wb') as f:
                f.write(file_content.read())

            logger.info(f"Created ZIP file:\n {orbit_file_path}")
        else:
            logger.error(f"Failed to download {eof_url}. Status code: {response.status_code}")
            logger.exception("Exception details:")

    def process_filtered_eof_chunk(self, chunk, acquisition_datetimes, date_format, v):
        matching_dict = {}

        for s in chunk:
            timestamps = s.split('_')[-v:]
            timestamps = [ts.lstrip('V').replace('.EOF', '') for ts in timestamps]

            try:
                timestamp_start = datetime.strptime(timestamps[-2], date_format)
                timestamp_end = datetime.strptime(timestamps[-1], date_format)

                matching_dates = [acq_date for acq_date in acquisition_datetimes if
                                 acq_date >= timestamp_start and acq_date <= timestamp_end]

                if matching_dates:
                    matching_dict[s] = [date.strftime("%Y%m%dT%H%M%S") for date in matching_dates]

            except ValueError:
                logger.exception(f"Error processing '{s}'")

        return matching_dict

    def process_orbit_data(self):

        
        satellite_names = self.find_satellite_names()

        if not satellite_names:
            logger.exception("No satellite names (S1A or S1B) found in the slc_directory.")
            return

        for SATELLITE_NAME in satellite_names:
            # Load and preprocess acquisition dates
            zip_files = glob.glob(os.path.join(self.slc_directory, "*.zip"))
            sentinel1_slc_files = [file for file in zip_files if SATELLITE_NAME in file]
            acquisition_dates = [re.search(r"(\d{8}T\d{6})", filename).group(1) for filename in sentinel1_slc_files]
            acquisition_datetimes = [datetime.strptime(date, self.EOF_DATE_FORMAT) for date in acquisition_dates]
            acquisition_datetimes.sort()

            # Fetch EOF files
            response = requests.get(self.orbit_files_webpage)
            if response.status_code != 200:
                logger.exception("Failed to retrieve the page. Status code:", response.status_code)
                return

            eof_list = re.findall(r'href="([^"]+)"', response.text)
            sentinel1_eof_files = [item for item in eof_list if item.startswith(SATELLITE_NAME)]

            # Process EOF files in parallel
            chunk_size = len(sentinel1_eof_files) // self.CHUNK_DIVISOR
            with ProcessPoolExecutor() as executor:
                chunks = [sentinel1_eof_files[i:i + chunk_size] for i in range(0, len(sentinel1_eof_files), chunk_size)]
                results = executor.map(self.process_filtered_eof_chunk, chunks, [acquisition_datetimes] * len(chunks),
                                        [self.EOF_DATE_FORMAT] * len(chunks), [self.NUM_TIMESTAMPS] * len(chunks))

            # Collect matching EOFs and acquisition dates
            matching_dict = {}
            for matching_strings_chunk in results:
                for filtered_eof, matching_dates in matching_strings_chunk.items():
                    matching_dict.setdefault(filtered_eof, []).extend(matching_dates)

            # Download and zip matching EOF files
            if matching_dict:
                logger.info("Matched orbit files with slc acquisition dates:\n%s", pprint.pformat(matching_dict))

                matching_eof_files = list(matching_dict.keys())
                matched_slc_acquisition_dates = [dates[0] for dates in matching_dict.values()]

                with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                    for eof, acquisition_date in zip(matching_eof_files, matched_slc_acquisition_dates):
                        if self.insar_processor == 'isce':
                            executor.submit(self.download_and_zip_isce, eof, acquisition_date)

                        elif self.insar_processor == 'snap':
                            executor.submit(self.download_and_zip_snap, eof, acquisition_date)

            else:
                print("No matching strings found.")


def main():
    parser = argparse.ArgumentParser(
        description='Downloads sentinel-1 POEORB orbit files',
                    formatter_class=argparse.RawTextHelpFormatter,
                    )
    parser.add_argument('--data_directory',
                        type=str,
                        help='Path to the data directory'
                        )
    parser.add_argument('--orbit_pass_name',
                        type=str,
                        help='Name of the orbit pass. Should be either "ascending" or "descending."')

    parser.add_argument('--insar_processor',
                        type=str,
                        help='Name of the software used to process slcs. This determines how the orbit files are saved for use in snap or isce. Should be either "isce" or "snap".')

    args = parser.parse_args()

    # Create an instance of the OrbitDataProcessor class
    processor = SentinelOrbitDownloader(args.data_directory, args.orbit_pass_name, args.insar_processor)

    # Process orbit data

    logger.info('***Starting SentinelOrbitDownloader***')
    processor.process_orbit_data()
    logger.info('***Finished SentinelOrbitDownloader***\n')

if __name__ == "__main__":
    main()


