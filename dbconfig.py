# specify database configurations
import sqlalchemy as db
from sqlalchemy import event
import numpy as np
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Mi4man11',
    'database': 'covid_economy_impact'
}
db_user = config.get('user')
db_pwd = config.get('password')
db_host = config.get('host')
db_port = config.get('port')
db_name = config.get('database')
# specify connection string
connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
engine = db.create_engine(connection_str,  echo=True)
'''metadata.create_all(engine)'''


def add_own_encoders(conn, cursor, query, *args):
    cursor.connection.encoders[np.float64] = lambda value, encoders: float(value)
    cursor.connection.encoders[np.int64] = lambda value, encoders: int(value)
# Solution
event.listen(engine, "before_cursor_execute", add_own_encoders)