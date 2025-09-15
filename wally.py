from app import app
from comrad import pool as comrad_pool
from coolify import pool as coolify_pool
from psycopg.rows import dict_row
from flask import render_template
from datetime import datetime
from zoneinfo import ZoneInfo

TZ=ZoneInfo("Pacific/Auckland")

users_query = r"""
with
    filtered_users as (select *, extract(epoch from ps360_last_event_timestamp)::int as ps360_last_event_posix from users where show_in_locator and pacs_presence <> 'Offline' order by last_name, first_name),
    windows_logons as (select ris, key as windows_computer, value::int as windows_logon, ROW_NUMBER() OVER (PARTITION BY ris ORDER BY value::int DESC) ranked_order from filtered_users, jsonb_each(windows_logons)),
    combined_data as (select
        filtered_users.ris,
        first_name,
        last_name,
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
"""

ris_query = r"""
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

available_desks_query = r"""
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
order by sort_order
"""

@app.get('/wally_data')
def wally_data():
    with coolify_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(users_query, prepare=True)
            online_users = {user["ris"]:user for user in cur}
            cur.execute(available_desks_query, prepare=True)
            available_desks = cur.fetchall()
    with comrad_pool.connection() as conn:
        with conn.execute(ris_query, ([ris for ris in online_users.keys()],), prepare=True) as cur:
            ris_data = {ris:dict(
                last_report=last_report,
                last_triage=last_triage,
            ) for ris, last_report, last_triage in cur}
    for ris, user in online_users.items():
        del user["ris"]
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

# def last_timestamp(user: UserRow, windows_logon: datetime | None) -> datetime | None:
#     timestamps = [timestamp for timestamp in (
#         user.ris_data.last_report,
#         user.ris_data.last_triage,
#         user.ris_data.timestamp,
#         user.presence.timestamp,
#         windows_logon,
#     ) if timestamp is not None]
#     return max(timestamps) if len(timestamps) > 0 else None

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
# app.jinja_env.filters['last_timestamp'] = last_timestamp
app.jinja_env.filters['presence_icon'] = presence_icon
app.jinja_env.filters['presence_icon_class'] = presence_icon_class

@app.get('/wally')
def wally():
    return render_template('wally.html')

@app.get('/locator-data')
def locator():
    return render_template('locator-data.html', data=wally_data())