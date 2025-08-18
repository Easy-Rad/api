from app import app
from comrad import pool as comrad_pool
from coolify import pool as coolify_pool
from psycopg.rows import dict_row
from flask import render_template
from os import path
import json
from datetime import datetime
from zoneinfo import ZoneInfo

locator_data = r"""
with ris_log as (select max(sg_serial) as sg_serial
                 from syslog
                 where sg_error_number = 121
                   and sg_datetime > now() at time zone 'Pacific/Auckland' - '24 hours'::interval
                 group by sg_user
                 order by sg_serial desc),
     ris_users as (select sg_user::text                                                                      as ris,
                          extract(epoch from sg_datetime at time zone 'Pacific/Auckland')::int               as ris_logon,
                          sg_terminal_tcpip::text                                                            as ris_terminal,
                          (regexp_match(sg_err_supplement, 'Login from ((?:[0-9]{1,3}\.){3}[0-9]{1,3})'))[1] as ris_ip
                   from ris_log
                            join syslog using (sg_serial)
                   where sg_err_supplement ~~ 'Login from %%'),
     windows_users as (select unnest(%s::text[]) as ris)
select ris,
       ris_logon,
       ris_terminal,
       ris_ip,
       (select extract(epoch from ct_dor at time zone 'Pacific/Auckland')::int
        from case_staff
        where ct_staff_serial = st_serial
          and ct_key_type = 'R'
        order by ct_dor desc
        limit 1) as last_report
from ris_users
         full join windows_users using (ris)
         join staff on ris = st_user_code
where st_status = 'A'
  and st_job_class in ('MC', 'JR')
"""

def locator_ris(windows_users:list[str]):
    # result = []
    with comrad_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(locator_data, [windows_users], prepare=True)
            return {user["ris"]:user for user in cur.fetchall()}

@app.get('/wally_data')
def wally_data():
    file_path = 'locator/locator.json'
    updated = int(path.getmtime(file_path))
    with open(file_path, 'r') as f:
        desks = json.load(f)
    with coolify_pool.connection() as conn:
        conn.row_factory=dict_row
        with conn.execute(r"""select ris from users where sso ilike any(%s)""", [[user["username"] for desk in desks for user in desk["users"]]], prepare=True) as cur:
            ris_data = locator_ris([user["ris"] for user in cur.fetchall()])
        with conn.execute(r"""select * from users where ris = any(%s)""", [[ris_user for ris_user in ris_data.keys()]], prepare=True) as cur:
            user_data = dict()
            for user in cur.fetchall():
                user |= ris_data[user["ris"]]
                user["windows_logons"]=dict()
                user_data[user.pop("ris")]=user
    for desk in desks:
        newUsers = set()
        for user in desk["users"]:
            uid = next((uid for uid, data in user_data.items() if data["sso"].lower() == user["username"].lower()), None)
            if uid is not None:
                user_data[uid]["windows_logons"][desk["ip"]]=user["logon"]
                newUsers.add(uid)
        newUsers.update((uid for uid, data in user_data.items() if data["ris_ip"]==desk["ip"]))
        desk["users"] = list(newUsers)
    return dict(
        updated=updated,
        desks=desks,
        users=user_data,
    )

tz=ZoneInfo("Pacific/Auckland")

def format_epoch(timestamp, format_string="%d/%m/%Y %H:%M:%S %p"):
    return datetime.fromtimestamp(timestamp, tz).strftime(format_string)

app.jinja_env.filters['format_epoch'] = format_epoch

@app.get('/wally')
def wally():
    return render_template('wally.html', data=wally_data())