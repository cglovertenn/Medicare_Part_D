from google.cloud import bigquery
import pandas as pd
import os
from sqlalchemy import create_engine
import psycopg2
import json
def connect(json):
    """
    Creates the connection to BigQuery; json shoule be your credentials.
    """
    client = bigquery.Client.from_service_account_json(json)
    return client
def get_database(client, data_name = "cms_medicare"):
    """
    Connects BigQuery to the correct database; cms_medicare is default.
    """
    dref = client.dataset(data_name, project = "bigquery-public-data")
    database = client.get_dataset(dref)
    return database
def get_table_names(client, database):
    """
    Gets the table names for easy viewing/looping.
    """
    table_names = [x.table_id for x in client.list_tables(database)]
    return table_names
def get_table(table, client, database):
    """
    Gets the data from an individual table as a pandas dataframe.
    """
    table = client.get_table(database.table(table))
    df = client.list_rows(table).to_dataframe()
    return df


json = "/Users/christinathip/VCB/visual_project/Medicare-8c5f8a5f8788.json"
client = connect(json)
db = get_database(client)
table_names = get_table_names(client, db)
table_names
table_name = 'part_d_prescriber_2014'

#Could not load this specific table, but able to load other
# table = get_table(table_name, client, db)
# table.head()
# table = db.table(table_name)
# tb = client.get_table(table)
# df = client.list_rows(tb)
# part_d_df = df.to_dataframe()

# Create a reference to the CSV and import a Pandas DataFrame
csv_path = "/Users/christinathip/VCB/visual_project/part_d_prescriber_2014.csv"
part_d_df = pd.read_csv(csv_path)
part_d_df.head()
# Rename the column headers
part_d_df = part_d_df.rename(columns={"Nppes Provider State": "nppes_provider_state",
                                        "Specialty Description": "specialty_description",
                                        "Drug Name": "drug_name",
                                        "Generic Name": "generic_name",
                                        "Bene Count": "bene_count",
                                        "Total Claim Count": "total_claim_count",
                                        "Total Day Supply":"total_day_supply",
                                        "Total Drug Cost": "total_drug_cost",
                                        "Total 30 Day Fill Count": "total_30_day_fill_count"})

part_d_df
part_d_df.columns

connection_string = "christinathip:lamppost15@localhost/project2"
engine = create_engine(f'postgresql+psycopg2://{connection_string}')
conn = engine.connect()
print("sql connect")
print(engine.table_names())
part_d_df.to_sql(name='part_d', con = engine, if_exists='append', index=False)

part_d_df.to_json("part_d.json", orient='records')
