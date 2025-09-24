from . import app, TZ
from ..database import local_pool, comrad_pool
from quart import request, render_template, Response

from os import environ
import httpx
from dataclasses import dataclass, field
from datetime import datetime, date
from lxml import html, etree # pyright: ignore[reportAttributeAccessIssue]
from psycopg.types.json import Jsonb
import logging
import pandas as pd
from .parts_parser import parse, LOOKUP_TABLE

IB_HOST = environ.get('IB_HOST','app-inteleradha-p.healthhub.health.nz')
IB_USER = environ['IB_USER']
IB_PASSWORD = environ['IB_PASSWORD']

@app.get('/registrar_numbers')
async def get_registrar_numbers():
    async with local_pool.connection() as conn:
        async with await conn.execute(r"""select ris, last_name || ', ' || first_name || ' (' || replace(specialty, 'Registrar - Year ', 'Y') || ')' as name from users where starts_with(specialty, 'Registrar - Year ') and show_in_locator order by last_name""") as cur:
            return await render_template('registrar-numbers.html', users = await cur.fetchall())

@app.post('/registrar_numbers')
async def fetch_registrar_numbers():
    r = await request.get_json()
    ris = r["ris"]
    async with local_pool.connection() as conn:
        async with await conn.execute(r"""select pacs from users where ris = %s""", (ris,)) as cur:
            if (pacs := await cur.fetchone()) is None:
                logging.error(f'No PACS username found in database for RIS user: {ris}')
                return []
    user = User(
        pacs[0],
        ris,
        date.fromisoformat(r["fromDate"]),
        date.fromisoformat(r["toDate"]),
    )
    logging.info(f'Generating registrar numbers for {user.pacs} ({user.ris}) from {user.fromDate} to {user.toDate}')
    async with InteleBrowserClient(timeout=None) as client:        
        df = await client.process_user(user)
    return Response(df.to_json(orient='records'), mimetype='application/json')

@dataclass
class AuditEntry:
    accession: str
    timestamp: float
    is_impression: bool

@dataclass
class User:
    pacs: str
    ris: str
    fromDate: date
    toDate: date
    audit_data: dict[str,AuditEntry] = field(default_factory=dict)

DB_QUERY = r"""
with events as (
    select distinct on (ct_ce_serial)
        ct_ce_serial,
        (extract(epoch from ct_dor at time zone 'Pacific/Auckland') * 1000)::bigint as ris_timestamp,
        case ct_staff_function when 'R' then 'Final' when 'V' then 'Prelim' end as ris_action
    from case_staff
             join staff on st_serial = ct_staff_serial
    where st_user_code = %s
              and ct_staff_function in ('R','V')
              and ct_dor between %s and %s + interval '1 day'
    order by ct_ce_serial, ct_staff_function, ct_dor
),
     audit_data as (
         select
             elements_array ->> 0 as or_accession_no,
             ((elements_array ->> 1)::double precision * 1000)::bigint as audit_timestamp,
             (elements_array ->> 2)::boolean as is_impression
         from jsonb_array_elements(%s) as elements_array
        --  from jsonb_array_elements('[["CA-19350057-MR", "2025-09-21T09:16:41.713000", false],["CA-19349781-CT", "2025-09-20T10:00:45.812000", true]]'::jsonb) as elements_array
     ),
     accessions as (
         select
             or_accession_no as accession,
             coalesce(ris_timestamp, audit_timestamp) as report_timestamp,
             coalesce(ris_action, case when is_impression then 'Impression' else 'Prelim' end) as action
         from events
                  join orders on or_event_serial = events.ct_ce_serial
                  full join audit_data using (or_accession_no)
     )
select
    report_timestamp,
    action,
    accession::text,
    case when or_ex_type = 'OD' then 'XR' else or_ex_type end as modality,
    (select jsonb_agg(ex_description) from case_procedure join exams on cx_key = ex_serial and cx_key_type = 'X' where cx_ce_serial = ce_serial) as exams,
    ce_description as description,
    (extract(epoch from ce_dor at time zone 'Pacific/Auckland') * 1000)::bigint as case_timestamp,
    extract(year from age(ce_dor, pa_dob))::int as age
from accessions
         join orders on or_accession_no = accession and or_status != 'X'
         join case_event on ce_serial = or_event_serial
         join case_main on ce_cs_serial = cs_serial
         join patient on cs_pno = pa_pno
order by report_timestamp
"""
class InteleBrowserClient(httpx.AsyncClient):
    LOGIN_URL = f"http://{IB_HOST}/InteleBrowser/login/ws/auth"
    LOGOUT_URL = f"http://{IB_HOST}/InteleBrowser/login/ws/auth/revoke"
    APP_URL = f"http://{IB_HOST}/InteleBrowser/app"

    async def __aenter__(self):
        await super().__aenter__()
        (await self.post(
            self.LOGIN_URL,
            data={
                "username": IB_USER,
                "password": IB_PASSWORD,
                "mfaToken": "",
                "keepMeLoggedIn": "false",
            },
        )).raise_for_status() # log in
        (await self.post(
            self.APP_URL,
            data={
                "service": "direct/1/AuditDetails/auditDetailsTable.tableForm",
                "sp": "S2",
                "Form2": "pageSizeSelect,pageSizeSelect$0",
                "pageSizeSelect": "4",
                "pageSizeSelect$0": "4",
            },
        )).raise_for_status() # set page size to 1000
        return self

    async def __aexit__(
        self,
        exc_type = None,
        exc_value = None,
        traceback = None,
    ):
        (await self.get(
            self.LOGOUT_URL,
            follow_redirects=True,
        )).raise_for_status()
        await super().__aexit__(exc_type, exc_value, traceback)

    async def process_user(self, user: User) -> pd.DataFrame:
        await self.fetch_impressions(user)
        return await self.fetch_ris_data(user)

    async def fetch_impressions(self, user: User) -> None:
        start_date = user.fromDate.strftime("%Y/%m/%d")
        end_date = user.toDate.strftime("%Y/%m/%d")
        response = await self.post(
            self.APP_URL,
            data={
                "service": "direct/1/AuditDetails/$Form",
                "sp": "S0",
                "Form0": "usernameFilter,$PropertySelection,patientIdFilter,studyDescriptionFilter,$PropertySelection$0,$Checkbox,$Checkbox$0,$Checkbox$1,$Checkbox$2,$Checkbox$3,$Checkbox$4,$Checkbox$5,$Checkbox$6,$Checkbox$7,$Checkbox$8,$Checkbox$9,$Checkbox$10,$Checkbox$11,$Checkbox$12,$Checkbox$13,$Checkbox$14,$Checkbox$15,$Checkbox$16,$Checkbox$17,$Checkbox$18,$Checkbox$19,$Checkbox$20,$Checkbox$21,$Checkbox$22,$Checkbox$23,$Checkbox$24,$Checkbox$25,$Checkbox$26,$Checkbox$27,$Checkbox$28,$Checkbox$29,$Checkbox$30,$Checkbox$31,$Checkbox$32,$Checkbox$33,$Checkbox$34,$Checkbox$35,$Checkbox$36,$Checkbox$37,$Checkbox$38,$Checkbox$39,$Checkbox$40,$Checkbox$41,$Checkbox$42,$Submit",
                "usernameFilter": user.pacs,
                "$PropertySelection": "anyRole",
                "patientIdFilter": "",
                "studyDescriptionFilter": "",
                "$PropertySelection$0": f"{start_date}:{end_date}",
                "$Checkbox$2": "on",  # Add Emergency Impression
                "$Checkbox$9": "on",  # Complete Dictation
                "$Submit": "Update",
            },
        )
        page_count = 0
        while True:
            root = html.fromstring(response.text)
            # await asyncio.gather(*(self.add_accession(accessions, uid) for e in root.iterfind(".//a[@studyuid]") if (uid := e.attrib['studyuid'])))
            for a in root.xpath("//a[@studyuid!=''][@actiontype][@date]"):
                uid = a.attrib['studyuid']
                is_impression = a.attrib['actiontype'] == 'AddEmergencyImpression'
                timestamp = datetime.fromisoformat(a.attrib['date']).replace(tzinfo=TZ).timestamp()
                try:
                    audit_data = user.audit_data[uid]
                    if (audit_data.is_impression and not is_impression) or (audit_data.is_impression == is_impression and timestamp < audit_data.timestamp):
                        audit_data.timestamp = timestamp
                    audit_data.is_impression &= is_impression
                except KeyError:
                    extra_response = await self.get(
                        self.APP_URL,
                        params={"service": "xtile/null/AuditDetails/$XTile$2", "sp": uid},
                    )
                    extra_root = etree.fromstring(extra_response.content)
                    if (accession_node := extra_root.find("./sp[2]")) is not None:
                        user.audit_data[uid] = AuditEntry(accession_node.text, timestamp, is_impression)
                    else:
                        logging.error(f'No response for uid: {uid}')
            if root.find(".//a[@name='nextPage']") is None:
                logging.info(f"Scraped {page_count + 1} InteleBrowser audit pages and got {len(user.audit_data)} accessions")
                break
            logging.info(f"Processed page {page_count + 1}, fetching next page (total {len(user.audit_data)} accessions)")
            response = await self.post(
                f"http://{IB_HOST}/InteleBrowser/app",
                params={'service':'direct/1/AuditDetails/auditDetailsTable.customPaginationControlTop.nextPage'},
            )
            page_count += 1

    async def fetch_ris_data(self, user: User) -> pd.DataFrame:
        async with comrad_pool.connection() as conn:
            async with await conn.execute(DB_QUERY, [
                user.ris,
                user.fromDate,
                user.toDate,
                Jsonb([(data.accession, data.timestamp, data.is_impression) for data in user.audit_data.values()]),
            ]) as cur:
                column_names = [desc.name for desc in cur.description] # pyright: ignore[reportOptionalIterable]
                df = pd.DataFrame(await cur.fetchall(), columns=column_names)
                logging.info(f"Retrieved {len(df)} orders from RIS database")
                if len(df) > 0:
                    df['sum_of_parts'] = df.apply(lambda row: max(parse(row['description']) if row['modality'] == 'XR' else 1, len(row['exams'])), axis=1)
                    logging.info(f"Parsing complete")
                return df

@app.get('/lookup_table')
async def get_lookup_table():
    return LOOKUP_TABLE