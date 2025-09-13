import os
import zipfile
import tempfile
import pandas as pd
from sqlalchemy import create_engine, Column, String, Float, Integer, text
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import Dict, Tuple, Set
import importlib.resources

Base = declarative_base()

_engine = None
_SessionLocal = None
_db_path = os.path.join(os.path.dirname(importlib.resources.files('hccinfhir.data')), "hcc.sqlite")

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(f'sqlite:///{_db_path}')
    return _engine

def get_db_session():
    """Returns a new database session."""
    global _SessionLocal
    if not os.path.exists(_db_path):
        rebuild_database()

    if _SessionLocal is None:
        engine = get_engine()
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return _SessionLocal()

class HccIsChronic(Base):
    __tablename__ = 'hcc_is_chronic'
    id = Column(Integer, primary_key=True)
    hcc = Column(String)
    is_chronic = Column(String)
    model_version = Column(String)
    model_domain = Column(String)

class RACoefficients(Base):
    __tablename__ = 'ra_coefficients'
    id = Column(Integer, primary_key=True)
    coefficient = Column(String)
    value = Column(Float)
    model_domain = Column(String)
    model_version = Column(String)

class RADxToCC(Base):
    __tablename__ = 'ra_dx_to_cc'
    id = Column(Integer, primary_key=True)
    diagnosis_code = Column(String)
    cc = Column(String)
    model_name = Column(String)

class RAEligibleCptHcpcs(Base):
    __tablename__ = 'ra_eligible_cpt_hcpcs'
    id = Column(Integer, primary_key=True)
    cpt_hcpcs_code = Column(String)
    year = Column(Integer)

class RAHierarchies(Base):
    __tablename__ = 'ra_hierarchies'
    id = Column(Integer, primary_key=True)
    cc_parent = Column(String)
    cc_child = Column(String)
    model_domain = Column(String)
    model_version = Column(String)
    model_fullname = Column(String)

def rebuild_database():
    """Forces a rebuild of the data from the source zip file."""
    if os.path.exists(_db_path):
        os.remove(_db_path)
    
    engine = get_engine()
    Base.metadata.create_all(engine)
    
    with importlib.resources.as_file(importlib.resources.files('hccinfhir.data').joinpath('data.zip')) as zip_path:
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            for filename in os.listdir(temp_dir):
                if not filename.endswith('.csv'):
                    continue

                filepath = os.path.join(temp_dir, filename)
                df = pd.read_csv(filepath)
                
                if 'ra_eligible_cpt_hcpcs' in filename:
                    year = int(filename.split('_')[-1].split('.')[0])
                    df['year'] = year
                    table_name = 'ra_eligible_cpt_hcpcs'
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                elif 'ra_coefficients' in filename:
                    table_name = 'ra_coefficients'
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                elif 'ra_dx_to_cc' in filename:
                    table_name = 'ra_dx_to_cc'
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                elif 'ra_hierarchies' in filename:
                    table_name = 'ra_hierarchies'
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                elif 'hcc_is_chronic' in filename:
                    table_name = 'hcc_is_chronic'
                    df.to_sql(table_name, engine, if_exists='append', index=False)

    with engine.connect() as connection:
        connection.execute(text('CREATE INDEX IF NOT EXISTS ix_ra_coefficients_lookup ON ra_coefficients (coefficient, model_domain, model_version);'))
        connection.execute(text('CREATE INDEX IF NOT EXISTS ix_ra_dx_to_cc_lookup ON ra_dx_to_cc (diagnosis_code, model_name);'))
        connection.execute(text('CREATE INDEX IF NOT EXISTS ix_ra_hierarchies_lookup ON ra_hierarchies (cc_parent, model_fullname);'))
        connection.execute(text('CREATE INDEX IF NOT EXISTS ix_ra_eligible_cpt_hcpcs_year ON ra_eligible_cpt_hcpcs (year);'))

def load_is_chronic_from_db(model_name: str) -> Dict[Tuple[str, str], bool]:
    """Load is_chronic mapping from the database for a specific model."""
    db_session = get_db_session()
    try:
        model_domain, model_version_str = model_name.split(" Model ")
        model_version = model_version_str.split("V")[1]
        query = db_session.query(HccIsChronic.hcc, HccIsChronic.is_chronic).filter(
            HccIsChronic.model_domain == model_domain,
            HccIsChronic.model_version.like(f'%{model_version}')
        )
        mapping = {}
        for hcc, is_chronic in query.all():
            key = (hcc, model_name)
            mapping[key] = is_chronic == 'Y'
        return mapping
    finally:
        db_session.close()