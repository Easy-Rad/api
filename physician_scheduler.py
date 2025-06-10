from os import environ
import pymssql

from app import app

def connection():
    return pymssql.connect(
        server=environ.get('PHYSCH_HOST', 'MSCHCPSCHSQLP1.cdhb.local'),
        user=f"cdhb\\{environ['SSO_USER']}",
        password=environ['SSO_PASSWORD'],
        database='PhySch',
        tds_version='7.4',
    )

base_roster_query = r"""
select DayNum as day, ShiftName as shift
from Pattern
join Shift on Shift.ShiftID=Pattern.ShiftID
join Employee on Pattern.EmployeeID=Employee.EmployeeID
where Employee.Abbr=%s
and IsRetired=0
order by DayNum, StartTime
"""
@app.get('/base_roster/<user_code>')
def get_base_roster(user_code: str):
    output = {}
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_roster_query, user_code)
            for day, shift in cursor:
                try:
                    output[day].append(shift)
                except KeyError:
                    output[day] = [shift]
    return output
