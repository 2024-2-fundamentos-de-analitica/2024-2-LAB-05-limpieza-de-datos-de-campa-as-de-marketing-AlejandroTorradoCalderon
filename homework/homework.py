"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import os
import zipfile
from pathlib import Path

INPUT_FOLDER = "files/input/"
OUTPUT_FOLDER = "files/output/"
Path(OUTPUT_FOLDER).mkdir(parents=True, exist_ok=True)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    
# DataFrame inicial vacío
df = pd.DataFrame()

# Iterar sobre los archivos .zip en la carpeta de entrada
for zip_file in os.listdir(INPUT_FOLDER):
    if zip_file.endswith(".zip"):
        zip_path = os.path.join(INPUT_FOLDER, zip_file)

        # Abrir el archivo .zip
        with zipfile.ZipFile(zip_path, 'r') as z:
            
            # Iterar sobre los archivos dentro del .zip
            for csv_file in z.namelist():
                if csv_file.endswith(".csv"):  # Asegurarse de que sea un archivo CSV
                    with z.open(csv_file) as f:
                        # Leer el CSV y concatenarlo con df
                        temp_df = pd.read_csv(f)
                        df = pd.concat([df, temp_df], ignore_index=True)

                        # client.csv
                        client_df = df[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']]
                        client_df['job'] = client_df['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
                        client_df["education"] = client_df["education"].str.replace(".", "_", regex=False).replace("unknown", pd.NA)
                        client_df['credit_default'] = client_df['credit_default'].apply(lambda x: 1 if x == "yes" else 0)
                        client_df['mortgage'] = client_df['mortgage'].apply(lambda x: 1 if x == "yes" else 0)
                        client_output_path = os.path.join(OUTPUT_FOLDER, "client.csv")
                        client_df.to_csv(client_output_path, index=False)

                        #campaign.csv
                        campaign_df = df[['client_id', 'number_contacts', 'contact_duration',
                                          'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome',
                                          'day', 'month']]
                        campaign_df['previous_outcome'] = campaign_df['previous_outcome'].apply(lambda x: 1 if x == "success" else 0)
                        campaign_df['campaign_outcome'] = campaign_df['campaign_outcome'].apply(lambda x: 1 if x == "yes" else 0)
                        # Convertir a datetime
                        campaign_df['last_contact_date'] = pd.to_datetime(
                            campaign_df['day'].astype(str) + "-" + campaign_df['month'] + "-2022", format="%d-%b-%Y",errors='coerce'
                        )
                        # Eliminar las columnas originales 'day' y 'month'
                        campaign_df = campaign_df.drop(columns=['day', 'month'])
                       
                        campaign_output_path = os.path.join(OUTPUT_FOLDER, "campaign.csv")
                        campaign_df.to_csv(campaign_output_path, index=False)

                        # economics.csv
                        economics_df = df[['client_id', 'cons_price_idx', 'euribor_three_months']]
                        economics_output_path = os.path.join(OUTPUT_FOLDER, "economics.csv")
                        economics_df.to_csv(economics_output_path, index=False)


if __name__ == "__main__":
    clean_campaign_data()
