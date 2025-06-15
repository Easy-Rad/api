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

base_roster_query_users = r"""
select distinct Employee.Abbr, FirstName, LastName
from Pattern
join Employee on Pattern.EmployeeID=Employee.EmployeeID
order by LastName
"""
@app.get('/base_roster/users')
def get_base_roster_users():
    output = []
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_roster_query_users)
            return cursor.fetchall()

base_roster_query_shifts = r"""
select distinct ShiftName
from Pattern
join Shift on Shift.ShiftID = Pattern.ShiftID
order by ShiftName
"""
@app.get('/base_roster/shifts')
def get_base_roster_shifts():
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_roster_query_shifts)
            return [shift for shift, in cursor.fetchall()]

base_roster_query_user = r"""
select DayNum as day, ShiftName as shift
from Pattern
join Shift on Shift.ShiftID=Pattern.ShiftID
join Employee on Pattern.EmployeeID=Employee.EmployeeID
where Employee.Abbr=%s
and IsRetired=0
order by DayNum, StartTime
"""
@app.get('/base_roster/user/<string:user_code>')
def get_base_roster_user(user_code: str):
    output = {}
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_roster_query_user, user_code)
            for day, shift in cursor:
                try:
                    output[day].append(shift)
                except KeyError:
                    output[day] = [shift]
    return output

base_roster_query_shift = r"""
select DayNum as day,
       Employee.Abbr as user_code,
       Employee.FirstName as first,
       Employee.LastName as last
from Pattern
join Shift on Shift.ShiftID=Pattern.ShiftID
join Employee on Pattern.EmployeeID=Employee.EmployeeID
where ShiftName=%s
order by DayNum, Employee.LastName
"""
@app.get('/base_roster/shift/<string:shift_name>')
def get_base_roster_shift(shift_name: str):
    output = {}
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(base_roster_query_shift, shift_name)
            for day, user_code, first, last in cursor:
                entry = (user_code, first, last)
                try:
                    output[day].append(entry)
                except KeyError:
                    output[day] = [entry]
    return output