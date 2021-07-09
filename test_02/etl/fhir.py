import pandas as pd


# from fhir.resources.patient import Patient

class FHIRDataTransformer(object):
    """Transform data in postgres into Patient/Encounter resources"""

    def get_patient_resources(self):
        df = pd.read_sql_table('patients_extract', 'postgresql://localhost:5432')
        # patient json from https://www.hl7.org/fhir/patient.html

        patientRows = []

        for index, row in df.iterrows():
            json_obj = {"resourceType": "Patient",
                        "id": row["MRN"],
                        "birthDate": row["Birth Date"],
                        "name": [
                            {
                                "family": row["Last Name"],
                                "given": row["First Name"]
                            }
                        ]}

            patientRows.append(json_obj)

        ## Query data in postgres, produce array of Patient FHIR resources
        return patientRows

    def get_encounter_resources(self):
        df = pd.read_sql_table('patients_extract', 'postgresql://localhost:5432')
        # patient json from https://www.hl7.org/fhir/encounter.html

        encounterRows = []
        for index, row in df.iterrows():
            json_obj = {"resourceType": "Encounter",
                        "id": row["Encounter ID"]}
            encounterRows.append(json_obj)

        return encounterRows
