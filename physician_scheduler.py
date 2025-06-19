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

requests_query_users = r"""
select
Abbr as user_code,
FirstName as first,
LastName as last
from (select EmployeeID
from Request
where StartDate >= year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP)
group by EmployeeID) R
join Employee E on R.EmployeeID=E.EmployeeID
order by LastName
"""
@app.get('/requests/users')
def get_requests_users():
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(requests_query_users)
            return cursor.fetchall()

requests_query_shifts = r"""
select distinct ShiftName
from (select ShiftID
from Request
where StartDate >= year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP)
group by ShiftID) R
join Shift S on R.ShiftId=S.ShiftID
order by ShiftName
"""
@app.get('/requests/shifts')
def get_requests_shifts():
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(requests_query_shifts)
            return [shift for shift, in cursor.fetchall()]

requests_query_user = r"""
select
DATEDIFF(s, '1970-01-01', AddedDate AT TIME ZONE 'New Zealand Standard Time') as added,
DATEDIFF(s, '1970-01-01', datetime2fromparts(
    StartDate/10000,
    StartDate/100%100,
    StartDate%100,
    R.StartTime%2400/100,
    R.StartTime%100,
    0, 0, 0
) AT TIME ZONE 'New Zealand Standard Time') as start,
DATEDIFF(s, '1970-01-01', datetime2fromparts(
    EndDate/10000,
    EndDate/100%100,
    EndDate%100,
    R.EndTime%2400/100,
    R.EndTime%100,
    0, 0, 0
) AT TIME ZONE 'New Zealand Standard Time') as finish,
ShiftName as shift
from Request R
join dbo.Employee on R.EmployeeID = Employee.EmployeeID
join Shift on R.ShiftID = Shift.ShiftID
where Employee.Abbr=%s
and StartDate >= year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP)
order by StartDate, R.StartTime
"""
@app.get('/requests/user/<string:user_code>')
def get_requests_user(user_code: str):
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(requests_query_user, user_code)
            return cursor.fetchall()

requests_query_shift = r"""
select
DATEDIFF(s, '1970-01-01', AddedDate AT TIME ZONE 'New Zealand Standard Time') as added,
DATEDIFF(s, '1970-01-01', datetime2fromparts(
    StartDate/10000,
    StartDate/100%100,
    StartDate%100,
    R.StartTime%2400/100,
    R.StartTime%100,
    0, 0, 0
) AT TIME ZONE 'New Zealand Standard Time') as start,
DATEDIFF(s, '1970-01-01', datetime2fromparts(
    EndDate/10000,
    EndDate/100%100,
    EndDate%100,
    R.EndTime%2400/100,
    R.EndTime%100,
    0, 0, 0
) AT TIME ZONE 'New Zealand Standard Time') as finish,
Employee.Abbr as user_code,
Employee.FirstName as first,
Employee.LastName as last
from Request R
join dbo.Employee on R.EmployeeID = Employee.EmployeeID
join Shift on R.ShiftID = Shift.ShiftID
where Shift.ShiftName=%s
and StartDate >= year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP)
order by StartDate, R.StartTime
"""
@app.get('/requests/shift/<string:user_code>')
def get_requests_shift(user_code: str):
    with connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(requests_query_shift, user_code)
            return cursor.fetchall()

