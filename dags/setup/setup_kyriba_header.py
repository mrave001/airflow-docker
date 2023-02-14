from sqlalchemy.types import DATE
from sqlalchemy.types import BIGINT
from sqlalchemy.types import INTEGER
from sqlalchemy.types import VARCHAR
from sqlalchemy.types import TIMESTAMP

# This function returns the column names for the Teradata table based on ADM
def create_header_columns():
    columns = {
        "id":"",
        "process_id": "",
        "filename": "",
        "envmnt_value": "",
        "payment_clear_date": "",
        "payment_reference": "",
        "status": "",
        "status_description": "",
        "created_by": "",
        "insert_ts": "",
        "updated_ts": "",
        "last_modified_by": ""  
    }
    return columns

def create_header_dtypes() :
    dtypes = {
        "id": BIGINT,
        "process_id": VARCHAR(40),
        "filename": VARCHAR(500),
        "envmnt_value": VARCHAR(6),
        "payment_clear_date": DATE,
        "payment_reference": VARCHAR(50),
        "status": INTEGER,
        "status_description": VARCHAR(1000),
        "created_by": VARCHAR(100),
        "insert_ts": TIMESTAMP,
        "updated_ts": TIMESTAMP,
        "last_modified_by": VARCHAR(100)
    }
    return dtypes