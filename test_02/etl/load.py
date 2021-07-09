import pandas as pd
import numpy as np
from sqlalchemy import create_engine, sql
from sqlalchemy.dialects.postgresql import psycopg2
import sys

# NOTES:
# NEED TO add  openpyxl in dependency


from carta_interview import Datasets, get_data_file


class DataLoader(object):
    """Load data into postgres"""

    def load_data(self):
        patient_extract1 = get_data_file(Datasets.PATIENT_EXTRACT1)
        patient_extract2 = get_data_file(Datasets.PATIENT_EXTRACT2)
        df = self.get_data_frame_from_excel(patient_extract1, patient_extract2)
        self.write_to_DB(df)

    def get_data_frame_from_excel(self, patient_extract1, patient_extract2):
        try:
            df1 = pd.read_excel(patient_extract1, engine='openpyxl')
            df2 = pd.read_excel(patient_extract2, engine='openpyxl')

            df3 = pd.concat([df1, df2])

            df3.sort_values(
                by="Update D/T", inplace=True, ascending=False
            )
            df3.drop_duplicates(subset="MRN", inplace=True,
                                keep="first", ignore_index=True)
            return df3.dropna()
        except:
            print("Unexpected error in get_data_frame_from_excel:", sys.exc_info()[0])
            return None

    def write_to_DB(self, df):
        try:
            conn_string = 'postgresql://localhost:5432'
            db = create_engine(conn_string)
            conn = db.connect()

            df.to_sql('patients_extract', con=conn, if_exists='replace', index=False)
            # close connection
            conn.close()
        except:
            print("Unexpected error in write_to_DB:", sys.exc_info()[0])
