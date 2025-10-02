from . import app, TZ
from ..database import local_pool, comrad_pool
from quart import request, render_template, Response

from os import environ
import httpx
from dataclasses import dataclass, field
from datetime import datetime, date, time
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
class Report:
    accession: str
    timestamp: datetime
    overread: bool

@dataclass
class User:
    pacs: str
    ris: str
    fromDate: date
    toDate: date

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
             (elements_array ->> 2)::boolean as overread
         from jsonb_array_elements(%s) as elements_array
        --  from jsonb_array_elements('[["CA-19350057-MR", "2025-09-21T09:16:41.713000", false],["CA-19349781-CT", "2025-09-20T10:00:45.812000", true]]'::jsonb) as elements_array
     ),
     accessions as (
         select
             or_accession_no as accession,
             coalesce(ris_timestamp, audit_timestamp) as report_timestamp,
             coalesce(ris_action, case when overread then 'Overread' else 'Impression' end) as action
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
        impressions = await self.fetch_impressions(user)
        return await self.fetch_ris_data(user, impressions)

    async def fetch_impressions(self, user: User) -> list[Report]:
        today = datetime.now(tz=TZ).date()
        async with local_pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(r"""select impression from registrar_numbers where user_pacs = %s and impression is not null order by impression desc limit 1""", [user.pacs])
                if (row := await cur.fetchone()) is not None:
                    last_database_timestamp: datetime = row[0]
                    scrape_from_date = last_database_timestamp.astimezone(tz=TZ).date()
                else:
                    scrape_from_date = today.replace(year=today.year - 10)
                logging.info(f"Scraping audit data for {user.pacs} from {scrape_from_date} to {today}")
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
                        "$PropertySelection$0": f'{scrape_from_date.strftime("%Y/%m/%d")}:{today.strftime("%Y/%m/%d")}',
                        "$Checkbox$2": "on",  # Add Emergency Impression
                        # "$Checkbox$9": "on",  # Complete Dictation
                        "$Submit": "Update",
                    },
                )
                page_count = 0
                scraped_count = 0
                update_count = 0
                while True:
                    root = html.fromstring(response.text)
                    for a in root.xpath("//a[@studyuid!=''][@actiontype][@date]"):
                        uid = a.attrib['studyuid']
                        timestamp = datetime.fromisoformat(a.attrib['date']).replace(tzinfo=TZ)
                        # is_impression = a.attrib['actiontype'] == 'AddEmergencyImpression' # todo remove
                        await cur.execute(r"""select accession from registrar_numbers_accessions where pacs_audit_uid = %s""", [uid], prepare=True)
                        if (row := await cur.fetchone()) is not None:
                            accession, = row
                            logging.debug(f"Fetched accession from database: {accession}")
                        else:
                            extra_response = await self.get(
                                self.APP_URL,
                                params={"service": "xtile/null/AuditDetails/$XTile$2", "sp": uid},
                            )
                            extra_root = etree.fromstring(extra_response.content)
                            if (accession_node := extra_root.find("./sp[2]")) is not None:
                                accession = accession_node.text
                                logging.debug(f"Fetched accession from internet: {accession}")
                                await cur.execute(r"""insert into registrar_numbers_accessions (accession, pacs_audit_uid) values (%s, %s)""", [accession, uid], prepare=True)
                                scraped_count += 1
                            else:
                                logging.error(f'Error fetching accession for uid: {uid}')
                                continue
                        await cur.execute(r"""
                            insert into registrar_numbers (user_pacs, accession, impression)
                            values (%s, %s, %s)
                            on conflict (user_pacs, accession)
                            do update set impression = excluded.impression
                            where registrar_numbers.impression > excluded.impression
                            """, [user.pacs, accession, timestamp], prepare=True)
                        if cur.rowcount > 0:
                            logging.debug(f"Updated database for {accession}")
                            update_count += cur.rowcount
                    if root.find(".//a[@name='nextPage']") is None:
                        logging.info(f"Scraped {page_count + 1} InteleBrowser audit pages and got {scraped_count} accessions ({update_count} database updates)")
                        break
                    logging.info(f"Processed page {page_count + 1}, fetching next page (total {scraped_count} accessions, {update_count} database updates)")
                    response = await self.post(
                        f"http://{IB_HOST}/InteleBrowser/app",
                        params={'service':'direct/1/AuditDetails/auditDetailsTable.customPaginationControlTop.nextPage'},
                    )
                    page_count += 1
                await cur.execute(r"""
                    select
                    accession,
                    coalesce(overread, impression) as added,
                    overread is not null as overread
                    from registrar_numbers
                    where user_pacs = %s
                    and coalesce(overread, impression) between %s and %s
                    order by added
                    """, [user.pacs, datetime.combine(user.fromDate, time.min, tzinfo=TZ), datetime.combine(user.toDate, time.max, tzinfo=TZ)])
                return [Report(accession, timestamp, overread) for accession, timestamp, overread in await cur.fetchall()]

    async def fetch_ris_data(self, user: User, reports: list[Report]) -> pd.DataFrame:
        async with comrad_pool.connection() as conn:
            async with await conn.execute(DB_QUERY, [
                user.ris,
                user.fromDate,
                user.toDate,
                Jsonb([(report.accession, report.timestamp.timestamp(), report.overread) for report in reports]),
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

@app.get('/scrape_all')
async def scrape_all():
    async with local_pool.connection() as conn:
        async with await conn.execute(r"""select pacs, ris from users where starts_with(specialty, 'Registrar - Year ') and show_in_locator order by last_name""") as cur:
            users = await cur.fetchall()
    async with InteleBrowserClient(timeout=None) as client:
        today = datetime.now(tz=TZ).date()
        for pacs, ris in users:
            await client.fetch_impressions(User(
                pacs,
                ris,
                today,
                today,
            ))
    logging.info(f"Scraping complete")
    return 'Scraping complete'