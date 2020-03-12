"""
NEM12 module, everything related to NEM12 parsing, handling of NEM12 blob events
and NEM12 calculations.
"""

from datetime import date
from datetime import datetime
from pathlib import Path
import csv
import getopt
import os
import sys

from google.cloud import firestore
from google.cloud.storage import Blob
import pandas as pd

from . import init_firestore_client
from . import NEM12_STORAGE_PATH_IN, NEM12_STORAGE_PATH_MERGED


def handle_nem12_blob_in(data, context, storage_client, bucket, blob_name, logger):
    """
    Handle blob events in path NEM12_STORAGE_PATH_IN, merges all NEM12 files in this path
    together and places in NEM12_STORAGE_PATH_MERGED path, one NMI per file.
    """

    logger.info(f"handle_nem12_blob_in()")
    nem12_blobs = [blob for blob in (storage_client.list_blobs(
        bucket_or_name=bucket, prefix=NEM12_STORAGE_PATH_IN)) if (blob.name.endswith('.csv'))]

    Path(
        f"/tmp/{NEM12_STORAGE_PATH_IN}").mkdir(parents=True, exist_ok=True)
    Path(
        f"/tmp/{NEM12_STORAGE_PATH_MERGED}").mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Merging blobs [{str.join(',', [n12.name for n12 in nem12_blobs])}]")
    nem12_files = []
    for n12 in nem12_blobs:
        file_name = f"/tmp/{n12.name}"
        with open(file_name, 'wb') as file_obj:
            n12.download_to_file(file_obj)
            nem12_files.append(file_name)

    merger = Nem12Merger(nem12_files)
    nmi_meter_registers = merger.nmi_meter_registers

    if (len(nmi_meter_registers) > 0):
        nmis = set([nmr.nmi for nmr in nmi_meter_registers])

        for nmi in nmis:
            tmp_file_name = f"/tmp/{NEM12_STORAGE_PATH_MERGED}/nem12_{nmi}.csv"
            logger.info(f"Writing to file_name={tmp_file_name}")

            with open(tmp_file_name, mode="w") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',')
                nmrs = [nmr for nmr in nmi_meter_registers if nmr.nmi == nmi]
                for nmr in nmrs:
                    csv_writer.writerow(nmr.line_items)
                    for iday in nmr.interval_days:
                        csv_writer.writerow(iday.line_items)
                        for vq in iday.variable_qualities:
                            csv_writer.writerow(vq.line_items)

            new_blob_name = f"{NEM12_STORAGE_PATH_MERGED}/nem12_{nmi}.csv"
            logger.info(f"Writing to new_blob_name={new_blob_name}")
            new_blob = bucket.blob(new_blob_name)
            with open(tmp_file_name, mode="rb") as tmp_file_obj:
                new_blob.upload_from_file(
                    file_obj=tmp_file_obj, content_type='text/csv')

            os.remove(tmp_file_name)


def handle_nem12_blob_merged(data, context, storage_client, bucket, blob_name, root_collection_name, logger):
    """
    Handle blob events in path NEM12_STORAGE_PATH_MERGED, parses the NEM12 blob (from the blob event),
    groups consumption and generation data into dates and loads into Firestore.
    This function only handles one NMI per NEM12 file, pre-processed by handle_nem12_blob_in()
    """

    logger.info(f"handle_nem12_blob_merged()")

    Path(
        f"/tmp/{NEM12_STORAGE_PATH_MERGED}").mkdir(parents=True, exist_ok=True)

    blob = Blob(blob_name, bucket)

    nem12_files = []
    with open(f"/tmp/{blob_name}", 'wb') as file_obj:
        blob.download_to_file(file_obj)
        nem12_files.append(f"/tmp/{blob_name}")

    nem12_parser = Nem12Merger(nem12_files)
    nmi_meter_registers = nem12_parser.nmi_meter_registers

    if (len(nmi_meter_registers) > 0):
        nmis = set([nmr.nmi for nmr in nmi_meter_registers])
        assert len(nmis) == 1, f"Expected only one NMI but found [{nmis}]"

        nmi = list(nmis)[0]

        flattened = nem12_parser.flatten_data()
        df = pd.DataFrame(flattened)

        df_agged = df.groupby(['nmi', 'interval_date', 'interval']).agg({'nmi': ['first'], 'interval_date': ['first'], 'interval': [
            'first'], 'consumption': ['sum'], 'generation': ['sum'], 'quality': ['first'], 'interval_length': ['first'], 'uom': ['first']})
        df_agged.columns = ["_".join(x) for x in df_agged.columns.ravel()]
        df_agged.rename(columns={
            'nmi_first': 'nmi',
            'interval_date_first': 'interval_date',
            'interval_first': 'interval',
            'consumption_sum': 'consumption',
            'generation_sum': 'generation',
            'quality_first': 'quality',
            'interval_length_first': 'interval_length',
            'uom_first': 'uom',
        }, inplace=True)

    _merged_nem12_to_db(nmi, df_agged, root_collection_name, logger)

    for tmp_n12_file in nem12_files:
        os.remove(tmp_n12_file)


def _merged_nem12_to_db(nmi, df_agged, root_collection_name, logger):
    db = init_firestore_client()

    site_doc = db.collection(root_collection_name).document(nmi)
    site_doc.set({
        'nmi': nmi,
        'name': 'Home',
        'interval_length': 30,
        'uom': 'KWH',
    })

    # There is a limit of 500 on the number of batch writes
    # Write one year of data at a time
    first_year = df_agged['interval_date'].iloc[0].year
    last_year = df_agged['interval_date'].iloc[-1].year

    for year in range(first_year, last_year + 1):
        df_year = df_agged[df_agged['interval_date'].dt.year == year]
        nmi_date = ('-1', date(1990, 1, 1))
        dailies = []
        consumptions = []
        generations = []

        for row in df_year.iterrows():
            key, values = row
            nmi, interval_date, interval = key
            curr_nmi_date = (nmi, interval_date)
            daily_doc = site_doc.collection(
                'dailies').document(interval_date.strftime('%Y%m%d'))

            if (curr_nmi_date != nmi_date):
                nmi_date = curr_nmi_date
                consumptions = []
                generations = []
                dailies.append({'nmi': nmi, 'interval_date': interval_date,
                                'consumptions': consumptions, 'generations': generations})

            consumptions.append(values.consumption)
            generations.append(values.generation)

        batch = db.batch()

        for daily in dailies:
            interval_date = daily.get('interval_date')
            daily_doc = site_doc.collection(
                'dailies').document(interval_date.strftime('%Y%m%d'))
            batch.set(daily_doc, {
                'interval_date': datetime.combine(interval_date, datetime.min.time()),
                'meter_consumptions': daily.get('consumptions'),
                'meter_generations': daily.get('generations'),
                'solar_generations': [],
                'battery_charges': [],
                'battery_discharges': [],
            })

        batch.commit()


class Nem12Merger():
    """Naive implementation, only handles record 200, 300 and 400.
    For record 400 (VariableDayQuality), only the whole CSV line is parsed.
    All other record types are skipped.
    NEM12 file spec can be found here:
    https://www.aemo.com.au/consultations/current-and-closed-consultations/meter-data-file-format-specification-nem12-and-nem13/
    """

    def __init__(self, nem12_files):
        self.nem12_files = nem12_files
        self.nmi_meter_registers = []
        self.current_nmr = None
        self.current_iday = None
        self._parse()

    def _parse(self):
        for f in self.nem12_files:
            with open(f) as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                for row in csv_reader:
                    if (row[0] == '200'):
                        nmi = row[1]
                        register = row[3]
                        meter = row[6]
                        register_config = row[2]
                        uom = row[7]
                        interval_length = int(row[8])
                        row_nmr = NmiMeterRegister(
                            nmi, meter, register, register_config, uom, interval_length, row)
                        existing_nmr = next(
                            (nmr for nmr in self.nmi_meter_registers if (nmr == row_nmr)), None)
                        if (existing_nmr == None):
                            existing_nmr = row_nmr
                            self.nmi_meter_registers.append(existing_nmr)
                        self.current_nmr = existing_nmr

                    elif (row[0] == '300'):
                        interval_count = 1440 / self.current_nmr.interval_length
                        interval_date = datetime.strptime(
                            row[1], '%Y%m%d')
                        quality = row[int(interval_count + 2)]
                        row_iday = IntervalDay(
                            self.current_nmr, interval_date, quality, row)
                        existing_iday = next(
                            (iday for iday in self.current_nmr.interval_days if (iday == row_iday)), None)
                        if (existing_iday == None):
                            existing_iday = row_iday
                            existing_iday.interval_values = [
                                float(iv) for iv in row[2:int(interval_count + 2)]]
                            self.current_nmr.interval_days.append(
                                existing_iday)
                        self.current_iday = existing_iday

                    elif (row[0] == '400'):
                        row_var_quality = VariableDayQuality(
                            self.current_iday, "".join(row), row)
                        existing_var_quality = next(
                            (vq for vq in self.current_iday.variable_qualities if (vq == row_var_quality)), None)
                        if (existing_var_quality == None):
                            existing_var_quality = row_var_quality
                            self.current_iday.variable_qualities.append(
                                existing_var_quality)

                    else:
                        print(f"skipping record type {row[0]}")

    def flatten_data(self):
        """
        This implementation assumes the following:
        * input and output interval_length = 30
        * input and output UOM is KWH
        * input registers starts with either E or B to represent consumption or generation respectively, all other streams (e.g. K, Q) result in 0.0 value

        Returns:
        * Flattenned representation of interval values with parent keys (e.g. nmi, meter, register, interval_date) associated.
        * This representation is intended for convenient Pandas DataFrame creation.
        """
        result = []

        for nmr in self.nmi_meter_registers:
            uom = nmr.uom
            interval_length = nmr.interval_length
            assert uom == 'KWH', f"Current implementation only supports KWH but got uom={uom}"
            assert interval_length == 30, f"Current implementation only supports interval length of 30 minutes but got interval_length={interval_length}"

            for iday in nmr.interval_days:
                quality = iday.quality

                for i, value in enumerate(iday.interval_values):

                    result.append({
                        'nmi': nmr.nmi,
                        'meter': nmr.meter,
                        'register': nmr.register,
                        'interval_date': iday.interval_date,
                        'quality': iday.quality,
                        'interval_length': nmr.interval_length,
                        'uom': nmr.uom,
                        'interval': i+1,
                        'consumption': value if nmr.register.startswith('E') else 0.0,
                        'generation': value if nmr.register.startswith('B') else 0.0,
                    })

        return result


class NmiMeterRegister():
    def __init__(self, nmi, meter, register, register_config, uom, interval_length, line_items):
        self.nmi = nmi
        self.meter = meter
        self.register = register
        self.register_config = register_config
        self.uom = uom
        self.interval_length = interval_length
        self.interval_days = []
        self.line_items = line_items

    def __eq__(self, other):
        if not isinstance(other, NmiMeterRegister):
            return False
        else:
            return self.nmi == other.nmi and self.meter == other.meter and self.register == other.register and self.register_config == other.register_config

    def __repr__(self):
        return f"nmi={self.nmi},meter={self.meter},register={self.register},register_config={self.register_config},uom={self.uom},len(interval_days)={len(self.interval_days)}"

    def __str__(self):
        return self.__repr__()


class IntervalDay():
    def __init__(self, nmi_meter_register, interval_date, quality, line_items):
        self.nmi_meter_register = nmi_meter_register
        self.interval_date = interval_date
        self.quality = quality
        self.interval_values = []
        self.variable_qualities = []
        self.line_items = line_items

    def get_interval_length(self):
        return self.nmi_meter_register.interval_length

    def __eq__(self, other):
        if not isinstance(other, IntervalDay):
            return False
        else:
            return self.nmi_meter_register == other.nmi_meter_register and self.interval_date == other.interval_date

    def __repr__(self):
        return f"nmi_meter_register={self.nmi_meter_register},interval_date={self.interval_date},len(interval_values)={len(self.interval_values)},len(variable_qualities)={len(self.variable_qualities)}"

    def __str__(self):
        return self.__repr__()


class VariableDayQuality():
    def __init__(self, interval_day, line_str, line_items):
        self.interval_day = interval_day
        self.line_str = line_str
        self.line_items = line_items

    def __eq__(self, other):
        if not isinstance(other, VariableDayQuality):
            return False
        else:
            return self.interval_day == other.interval_day and self.line_str == other.line_str

    def __repr__(self):
        return f"interval_day={self.interval_day},line_str={self.line_str}"

    def __str__(self):
        return self.__repr__()
