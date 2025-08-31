from app import app
from comrad import pool as comrad_pool
from coolify import pool as coolify_pool
from psycopg.rows import dict_row
from flask import render_template
from os import path
import json
from datetime import datetime
from xmpp import xmpp_client, Presence
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
          and ct_staff_function = 'R'
        order by ct_dor desc
        limit 1) as last_report,
       (select extract(epoch from rfe_dor at time zone 'Pacific/Auckland')::int
        from case_referral_exam
        where rfe_staff = st_serial
          and rfe_serial > (select rfe_serial
                             from case_referral_exam
                             where rfe_dor <= now() at time zone 'Pacific/Auckland' - '24 hours'::interval
                             order by rfe_serial desc
                             limit 1)
        order by rfe_serial desc
        limit 1) as last_triage
from ris_users
         full join windows_users using (ris)
         join staff on ris = st_user_code
where st_status = 'A'
  and st_job_class in ('MC', 'JR')
"""

def locator_ris(windows_users:list[str]):
    with comrad_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(locator_data, [windows_users], prepare=True)
            return {user["ris"]:dict(
                last_report=user["last_report"],
                last_triage=user["last_triage"],
                ris_logon=dict(
                    ip=user["ris_ip"],
                    terminal=user["ris_terminal"],
                    timestamp=user["ris_logon"],
                ) if user["ris_ip"] else None,
            ) for user in cur.fetchall()}

@app.get('/wally_data')
def wally_data():
    file_path = 'locator/locator.json'
    updated = int(path.getmtime(file_path))
    with open(file_path, 'r') as f:
        # desks and windows users with logon timestamp
        desks = {desk["ip"]:desk for desk in json.load(f)}
    pacs_users = {pacs: dict(presence=user.presence,updated=user.updated) for pacs, user in xmpp_client.users.items()}
    with coolify_pool.connection() as conn:
        with conn.execute(r"""select ris from users where sso ilike any(%s) or pacs = any(%s)""", [
                [user["username"] for desk in desks.values() for user in desk["users"]],
                [pacs for pacs, presence_data in pacs_users.items() if presence_data['presence'] != Presence.OFFLINE],
                # list(pacs_users_online.keys()),
            ], prepare=True) as cur:
            # ris users, ris logon data and last triage/last report timestamps
            ris_data = locator_ris([ris for (ris,) in cur.fetchall()])
        with conn.execute(r"""select * from users where show_in_locator and ris = any(%s)""", [[ris_user for ris_user in ris_data.keys()]], prepare=True) as cur:
            cur.row_factory=dict_row
            users = {user["ris"]:user|ris_data[user["ris"]]|dict(
                windows_logons=dict(),
                pacs_presence=pacs_users.get(user["pacs"], dict(presence=Presence.OFFLINE, updated=0)),
            ) for user in cur.fetchall()}
    sso_map = {user["sso"].lower(): uid for uid, user in users.items()}
    # radiologist_desks = {}
    # reg_fellow_desks = {}
    # empty_desks = []
    for ip, desk in desks.items():
        del desk["ip"]
        desk_available = True
        # empty = True
        # [sso_map[sso] for user_entry in desk["users"] if (sso := user_entry["username"].lower()) in sso_map]
        desk_users = set()
        for user_entry in desk["users"]:
            if (sso:= user_entry["username"].lower()) in sso_map:
                uid = sso_map[sso]
                desk_users.add(uid)
                if desk_available and users[uid]['pacs_presence']['presence'] != Presence.OFFLINE:
                    desk_available = False
                # deskmap = radiologist_desks if users[uid]["radiologist"] else reg_fellow_desks
                # if ip not in deskmap:
                #     deskmap[ip] = set()
                # deskmap[ip].add(uid)
                users[uid]["windows_logons"][ip]=user_entry["logon"]
                # empty = False
        desk["available"] = desk_available
        desk["users"] = list(desk_users)
        # if empty:
        #     empty_desks.append(ip)
    offsite : list[str] = []
    for uid, user in users.items():
        del user["ris"]
        if len(user["windows_logons"]) == 0 and (user["ris_logon"] is None or user["ris_logon"]["ip"] not in desks):
            offsite.append(uid)
    return dict(
        # radiologist_desks={ip: list(uids) for ip, uids in radiologist_desks.items()},
        # reg_fellow_desks={ip: list(uids) for ip, uids in reg_fellow_desks.items()},
        # empty_desks=empty_desks,
        offsite=offsite,
        users=users,
        desks=desks,
        updated=updated,
    )

TZ=ZoneInfo("Pacific/Auckland")

def format_iso8601(posix: int) -> str:
    return datetime.fromtimestamp(posix, TZ).isoformat()

def format_epoch(timestamp, format_string="%d/%m/%Y %-I:%M:%S %p") -> str:
    return datetime.fromtimestamp(timestamp, TZ).strftime(format_string)

def last_timestamp(user):
    timestamp = 0
    for test in ('last_report', 'last_triage'):
        if user[test] is not None and user[test] > timestamp:
            timestamp = user[test]
    if user['ris_logon'] is not None and user['ris_logon']['timestamp'] > timestamp:
        timestamp = user['ris_logon']['timestamp']
    for w in user['windows_logons'].values():
        if w > timestamp:
            timestamp = w
    return max(timestamp, user['pacs_presence']['updated'])

def presence_icon (presence: Presence) -> str:
    match presence:
        case Presence.AVAILABLE:
            return 'user'
        case Presence.AWAY:
            return 'clock'
        case Presence.BUSY:
            return 'ban'
        case Presence.OFFLINE:
            return 'user-slash'

def presence_icon_class(presence: Presence) -> str:
    match presence:
        case Presence.AVAILABLE:
            return 'success'
        case Presence.AWAY:
            return 'warning'
        case Presence.BUSY:
            return 'danger'
        case Presence.OFFLINE:
            return 'grey'

app.jinja_env.filters['format_iso8601'] = format_iso8601
app.jinja_env.filters['format_epoch'] = format_epoch
app.jinja_env.filters['last_timestamp'] = last_timestamp
app.jinja_env.filters['presence_icon'] = presence_icon
app.jinja_env.filters['presence_icon_class'] = presence_icon_class

@app.get('/wally')
def wally():
    return render_template('wally.html')

@app.get('/locator-data')
def locator():
    return render_template('locator-data.html', data=wally_data())