import pymssql
from os import environ

def connection():
    return pymssql.connect(
        server=environ.get('PHYSCH_HOST', 'MSCHCPSCHSQLP1.cdhb.local'),
        user=f"cdhb\\{environ['SSO_USER']}",
        password=environ['SSO_PASSWORD'],
        database='PhySch',
        tds_version='7.4',
    )
