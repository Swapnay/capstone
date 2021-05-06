from sparktasks.covid.extract import Extract
from  sparktasks.covid.transform_load import TransformLoad


@staticmethod
def extract():
    extract = Extract()
    extract.extract_from_source()
    extract.store_raw_in_db()

@staticmethod
def transform_load():
    transform_load = TransformLoad()
    transform_load.transform_load_data()

