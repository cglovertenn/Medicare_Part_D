import pandas as pd

data = pd.read_json("part_d.json", orient='records')
print(data.head())

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Integer, String, Float
Base = declarative_base()


# Create a class for data based on drug_name
class drug_db(Base):
    __tablename__ = "drug_db"
    id = Column(Integer, primary_key=True)
    nppes_provider_state = Column(String(255))
    specialty_description = Column(String(255))
    drug_name = Column(String(255))
    generic_name = Column(String(255))
    bene_count = Column(Integer)
    total_claim_count = Column(Integer)
    total_day_supply = Column(Integer)
    total_drug_cost = Column(Float(25))
    total_30_day_fill_count = Column(Float(25))

    def __repr__(self):
        return f"part_d_id={self.drug_db}, name={self.name}"


# Create a class for data based on state
class state_db(Base):
    __tablename__ = "state_db"
    id = Column(Integer, primary_key=True)
    nppes_provider_state = Column(String(255))
    drug_name = Column(String(255))
    generic_name = Column(String(255))
    bene_count = Column(Integer)
    total_claim_count = Column(Integer)
    total_day_supply = Column(Integer)
    total_drug_cost = Column(Float(25))
    total_30_day_fill_count = Column(Float(25))

    def __repr__(self):
        return f"part_d_id={self.state_db}, name={self.name}"
engine = create_engine('sqlite:///part_d_db.sqlite')
conn=engine.connect()
Base.metadata.create_all(engine)

print(engine.table_names())
from sqlalchemy.orm import Session
session = Session(bind=engine)

drug_data = data.to_dict(orient='records')
state_data = data.to_dict(orient='records')

metadata = MetaData(bind=engine)
import sqlalchemy
drug_table = sqlalchemy.Table('drug_db', metadata, autoload=True)
state_table = sqlalchemy.Table('state_db', metadata, autoload=True)

#Run to clear data
conn.execute(drug_table.delete())
conn.execute(state_table.delete())

conn.execute(drug_table.insert(), drug_data)
conn.execute(state_table.insert(), state_data)

conn.execute('SELECT * FROM drug_db').first()
