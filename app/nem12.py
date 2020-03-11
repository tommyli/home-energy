"""Take input NEM12 CSV files and merges them into one
python merge_nem12.py -o <output_path> <nem12_1.csv> [nem12_2.csv...]
output: multiples of nem12_{nmi}.csv depending on how many NMIs in input files
"""

from datetime import datetime
from pathlib import Path
import csv
import getopt
import sys

from . import NEM12_STORAGE_PATH_IN, NEM12_STORAGE_PATH_MERGED


def main(argv=None):
    if argv is None:
        argv = sys.argv

    nem12_files = []
    output_path = ''

    try:
        opts, args = getopt.getopt(
            argv[1:], "ho:", ["help", "output path"])
    except getopt.error as msg:
        print(msg)
        print("for help use --help")
        sys.exit(2)
    for option, arg in opts:
        if option in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        if option in ("-o", "--out"):
            if not arg:
                print(__doc__)
                sys.exit(0)
            output_path = arg if arg[-1] == '/' else f"{arg}/"

    if (not len(argv) > 3):
        print(__doc__)
        sys.exit(0)

    nem12_files = argv[3:]
    print(output_path)

    merger = Nem12Merger(nem12_files)
    nmi_meter_registers = merger.nmi_meter_registers

    if (len(nmi_meter_registers) == 0):
        sys.exit(0)
    else:
        nmis = set([nmr.nmi for nmr in nmi_meter_registers])

        for nmi in nmis:
            file_name = f"{output_path}nem12_{nmi}.csv"
            print(f"Writing to {file_name}")
            with open(file_name, mode="w") as csv_file:
                csv_writer = csv.writer(csv_file, delimiter=',')
                nmrs = [nmr for nmr in nmi_meter_registers if nmr.nmi == nmi]
                for nmr in nmrs:
                    csv_writer.writerow(nmr.line_items)
                    for iday in nmr.interval_days:
                        csv_writer.writerow(iday.line_items)
                        for vq in iday.variable_qualities:
                            csv_writer.writerow(vq.line_items)

    print(nmi_meter_registers[0].interval_days[0])

    print(nmi_meter_registers)


def handle_nem12_blob_in(data, context, storage_client, bucket, blob_name, logger):
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


def handle_nem12_blob_merged(data, context, storage_client, bucket, blob_name, logger):
    logger.info(f"handle_nem12_blob_merged()")
    merged_blobs = [blob for blob in (storage_client.list_blobs(
        bucket_or_name=bucket, prefix=NEM12_STORAGE_PATH_MERGED)) if (blob.name.endswith('.csv'))]

    Path(
        f"/tmp/{NEM12_STORAGE_PATH_MERGED}").mkdir(parents=True, exist_ok=True)

    nem12_files = []
    for n12 in merged_blobs:
        with open(f"/tmp/{n12.name}", 'wb') as file_obj:
            n12.download_to_file(file_obj)
            nem12_files.append(f"/tmp/{n12.name}")

    nem12_parser = Nem12Merger(nem12_files)
    nmi_meter_registers = nem12_parser.nmi_meter_registers

    if (len(nmi_meter_registers) > 0):
        nmis = set([nmr.nmi for nmr in nmi_meter_registers])
        logger.info(f"nmis={nmis}")

        # TODO - Load into Firestore
        # for nmi in nmis:
        #     nmrs = [nmr for nmr in nmi_meter_registers if nmr.nmi == nmi]
        #     for nmr in nmrs:
        #         for iday in nmr.interval_days:
        #             logger.info(
        #                 f"nmi={nmi}, nmr={nmr}, iday={iday}, len(iday.interval_values)={len(iday.interval_values)}")


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
                            self.current_nmr, row[1], quality, row)
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


if __name__ == "__main__":
    sys.exit(main())
