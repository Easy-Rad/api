from typing import LiteralString
from . import app, TZ
from ..database import local_pool, comrad_pool, phy_sch_connection
from psycopg.rows import dict_row
from quart import render_template
from datetime import datetime

users_query: LiteralString = r"""
with
    filtered_users as (select *, extract(epoch from ps360_last_event_timestamp)::int as ps360_last_event_posix from users where show_in_locator and pacs_presence <> 'Offline'),
    windows_logons as (select ris, key as windows_computer, value::int as windows_logon, row_number() over (partition by ris order by value::int desc) ranked_order from filtered_users, jsonb_each(windows_logons)),
    combined_data as (select
        filtered_users.ris,
        first_name,
        last_name,
        physch,
        specialty,
        radiologist,
        case
            when ps360_last_event_workstation is null then windows_computer
            when windows_computer is null then ps360_last_event_workstation
            when windows_logon > ps360_last_event_posix then windows_computer
            else ps360_last_event_workstation
            end as computer,
        windows_computer,
        windows_logon,
        pacs_presence,
        extract(epoch from pacs_last_updated)::int as pacs_last_updated,
        ps360_last_event_posix as ps360_last_event_timestamp,
        ps360_last_event_type
    from filtered_users
    left join windows_logons on windows_logons.ris = filtered_users.ris and ranked_order = 1)
select
    combined_data.*,
    desks.name as desk,
    phone
from combined_data
left join desks on desks.computer_name = combined_data.computer
order by last_name, first_name
"""

ris_query: LiteralString = r"""
with earliest_triage_serial as (select rfe_serial
                                from case_referral_exam
                                where rfe_dor <= now() at time zone 'Pacific/Auckland' - '24 hours'::interval
                                order by rfe_serial desc
                                limit 1)
select st_user_code::text as ris,
       (select extract(epoch from ct_dor at time zone 'Pacific/Auckland')::int
        from case_staff
        where ct_staff_serial = st_serial
          and ct_key_type = 'R'
          and ct_staff_function = 'R'
        order by ct_dor desc
        limit 1) as last_report,
       (select extract(epoch from rfe_dor at time zone 'Pacific/Auckland')::int
        from case_referral_exam
        where rfe_staff = st_serial
          and rfe_serial > (select rfe_serial from earliest_triage_serial)
        order by rfe_serial desc
        limit 1) as last_triage
from staff
    where st_user_code = any(%s)
"""

phys_sched_query: LiteralString = r"""
declare
    @today int = year(CURRENT_TIMESTAMP) * 10000 + month(CURRENT_TIMESTAMP) * 100 + day(CURRENT_TIMESTAMP),
    @current_time int = 100 * DATEPART(hour, CURRENT_TIMESTAMP) + DATEPART(minute, CURRENT_TIMESTAMP)
select
    ShiftName as shift,
    FORMAT(Shift.StartTime / 100 % 24, '00') + ':' + format(Shift.StartTime % 100, '00') as start,
    FORMAT(Shift.EndTime / 100 % 24, '00') + ':' + format(Shift.EndTime % 100, '00') as 'end',
    CAST(IIF(Shift.StartTime <= @current_time and Shift.EndTime > @current_time, 1, 0) as bit) as active
from SchedData
    join Employee on SchedData.EmployeeID = Employee.EmployeeID
    join Shift on SchedData.ShiftID = Shift.ShiftID
where AssignDate = @today and Employee.Abbr = %s
order by Shift.StartTime, Shift.EndTime, Shift.DisplayOrder, Shift.ShiftName, Shift.ShiftID
"""

available_desks_query: LiteralString = r"""
with windows_logons as (
    select
        key as computer_name,
        jsonb_object_agg(first_name || ' ' || last_name, value::int) AS users
    from users, jsonb_each(windows_logons)
    where pacs_presence = 'Offline'
    group by computer_name
)
select
    computer_name as computer,
    name as desk,
    phone,
    online,
    users
from desks
    left join windows_logons using (computer_name)
where show_if_available
order by sort_order
"""

@app.get('/wally_data')
async def wally_data():
    async with local_pool.connection() as coolify_conn:
        async with coolify_conn.cursor(row_factory=dict_row) as coolify_cur:
            await coolify_cur.execute(users_query, prepare=True)
            online_users = {}
            with phy_sch_connection() as phys_sched_conn:
                with phys_sched_conn.cursor(as_dict=True) as phys_sched_cur:
                    async for user in coolify_cur:
                        phys_sched_cur.execute(phys_sched_query, (user["physch"],))
                        user["roster"] = phys_sched_cur.fetchall()
                        online_users[user.pop("ris")] = user
            await coolify_cur.execute(available_desks_query, prepare=True)
            available_desks = await coolify_cur.fetchall()
    async with comrad_pool.connection() as conn:
        async with await conn.execute(ris_query, ([ris for ris in online_users.keys()],), prepare=True) as cur:
            ris_data = {ris:dict(
                last_report=last_report,
                last_triage=last_triage,
            ) async for ris, last_report, last_triage in cur}
    for ris, user in online_users.items():
        user.update(ris_data[ris])
        timestamps = [timestamp for timestamp in (
            user["last_report"],
            user["last_triage"],
            user["pacs_last_updated"],
            user["ps360_last_event_timestamp"],
            user["windows_logon"],
        ) if timestamp is not None]
        user["last_active"] = max(timestamps) if len(timestamps) > 0 else None
    return dict(
        online_users=online_users,
        available_desks=available_desks,
    )

def format_iso8601(posix: int) -> str:
    return datetime.fromtimestamp(posix, TZ).isoformat()

def format_epoch(posix: int, format_string="%d/%m/%Y %-I:%M:%S %p") -> str:
    return datetime.fromtimestamp(posix, TZ).strftime(format_string)

def presence_icon (presence: str) -> str:
    match presence:
        case 'Available':
            return 'user'
        case 'Away':
            return 'clock'
        case 'Busy':
            return 'ban'
        case _:
            return 'user-slash'

def presence_icon_class(presence: str) -> str:
    match presence:
        case 'Available':
            return 'success'
        case 'Away':
            return 'warning'
        case 'Busy':
            return 'danger'
        case _:
            return 'grey'

app.jinja_env.filters['format_iso8601'] = format_iso8601
app.jinja_env.filters['format_epoch'] = format_epoch
app.jinja_env.filters['presence_icon'] = presence_icon
app.jinja_env.filters['presence_icon_class'] = presence_icon_class

@app.get('/wally')
async def wally():
    return await render_template('wally.html')

@app.get('/locator-data')
async def locator():
    return await render_template('locator-data.html', data = await wally_data())