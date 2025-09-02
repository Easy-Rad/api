from app import app
from comrad import pool as comrad_pool
from coolify import pool as coolify_pool
from flask import render_template
from os import path
import json
from dataclasses import dataclass, field
from datetime import datetime
from xmpp import xmpp_client, Presence
from zoneinfo import ZoneInfo

TZ=ZoneInfo("Pacific/Auckland")

locator_query = r"""
with ris_log as (select max(sg_serial) as sg_serial
                 from syslog
                 where sg_error_number = 121
                   and sg_datetime > now() at time zone 'Pacific/Auckland' - '24 hours'::interval
                 group by sg_user
                 order by sg_serial desc),
     ris_users as (select sg_user::text                                                                      as ris,
                          extract(epoch from sg_datetime at time zone 'Pacific/Auckland')::int               as ris_logon
                   from ris_log
                            join syslog using (sg_serial)
                   where sg_err_supplement ~~ 'Login from %%'),
     windows_users as (select unnest(%s::text[]) as ris)
select ris,
       ris_logon,
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

@dataclass 
class RisData:
    timestamp: datetime | None
    last_report: datetime | None
    last_triage: datetime | None

@dataclass 
class PacsPresence:
    presence: Presence
    timestamp: datetime | None

@dataclass 
class DeskWindowsLogon:
    first_name: str
    last_name: str
    timestamp: datetime

@dataclass
class Desk:
    desk_name: str
    computer_name: str
    phone: str | None
    online: bool
    available: bool = True
    windows_logons: list[DeskWindowsLogon] = field(default_factory=list)

@dataclass 
class UserWindowsLogon:
    desk: Desk
    timestamp: datetime

@dataclass
class UserRow:
    first_name: str
    last_name: str
    specialty: str
    radiologist: bool
    ris_data: RisData
    presence: PacsPresence
    windows_logons: list[UserWindowsLogon] = field(default_factory=list)

def locator_ris(windows_users:list[str]) -> dict[str, RisData]:
    with comrad_pool.connection() as conn:
        with conn.execute(locator_query, [windows_users], prepare=True) as cur:
            return {ris:RisData(
                datetime.fromtimestamp(ris_logon, TZ) if ris_logon is not None else None,
                datetime.fromtimestamp(last_report, TZ) if last_report is not None else None,
                datetime.fromtimestamp(last_triage, TZ) if last_triage is not None else None,
            ) for  ris, ris_logon, last_report, last_triage in cur.fetchall()}

def wally_data():
    file_path = 'locator/locator.json'
    updated = datetime.fromtimestamp(int(path.getmtime(file_path)), TZ)
    with open(file_path, 'r') as f:
        desks = {desk["ip"]:desk for desk in json.load(f)}
    pacs_users = {pacs: PacsPresence(user.presence, datetime.fromtimestamp(user.updated, TZ) if user.updated > 0 else None) for pacs, user in xmpp_client.users.items()}
    with coolify_pool.connection() as conn:
        with conn.execute(r"""select ris from users where sso ilike any(%s) or pacs = any(%s)""", [
                [user["username"] for desk in desks.values() for user in desk["users"]],
                [pacs for pacs, pacs_presence in pacs_users.items() if pacs_presence.presence != Presence.OFFLINE],
            ], prepare=True) as cur:
            ris_data = locator_ris([ris for (ris,) in cur.fetchall()])
        with conn.execute(r"""select ris, first_name, last_name, specialty, lower(sso) as sso, radiologist, pacs from users where show_in_locator and ris = any(%s) order by last_name""", [[ris_user for ris_user in ris_data.keys()]], prepare=True) as cur:
            users: dict[str, UserRow] = {}
            for ris, first_name, last_name, specialty, sso, radiologist, pacs in cur.fetchall():
                u = UserRow(
                    first_name,
                    last_name,
                    specialty,
                    radiologist,
                    ris_data[ris],
                    pacs_users.get(pacs, PacsPresence(Presence.OFFLINE, None)),
                )
                users[sso] = u
    available_desks = []
    for desk_data in desks.values():
        desk = Desk(
            desk_data["name"],
            desk_data["computer_name"],
            desk_data["phone"],
            desk_data["online"],
        )
        for user_entry in desk_data["users"]:
            try:
                u = users[user_entry["username"].lower()]
                timestamp = datetime.fromtimestamp(user_entry["logon"], TZ)
                desk.available = desk.available and u.presence.presence == Presence.OFFLINE
                u.windows_logons.append(UserWindowsLogon(desk, timestamp))
                desk.windows_logons.append(DeskWindowsLogon(u.first_name, u.last_name, timestamp))
            except KeyError:
                pass
        if desk.available:
            available_desks.append(desk)
    return dict(
        users=list(users.values()),
        available_desks=available_desks,
        updated=updated,
    )

def format_iso8601(timestamp: datetime) -> str:
    return timestamp.isoformat()

def format_epoch(timestamp: datetime, format_string="%d/%m/%Y %-I:%M:%S %p") -> str:
    return timestamp.strftime(format_string)

def last_timestamp(user: UserRow, windows_logon: datetime | None) -> datetime | None:
    timestamps = [timestamp for timestamp in (
        user.ris_data.last_report,
        user.ris_data.last_triage,
        user.ris_data.timestamp,
        user.presence.timestamp,
        windows_logon,
    ) if timestamp is not None]
    return max(timestamps) if len(timestamps) > 0 else None

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