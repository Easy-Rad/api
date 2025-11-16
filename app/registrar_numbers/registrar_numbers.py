import asyncio

from psycopg import AsyncConnection
from . import app, TZ
from ..database import local_pool, comrad_pool, PS360
from quart import request, render_template, jsonify

from os import environ
import httpx
from dataclasses import dataclass
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
        async with await conn.execute(r"""
            select ris,
            last_name || ', ' || first_name || ' (' || replace(specialty, 'Registrar - Year ', 'Y') || ')' as name,
            registrar_start_date as start_date
            from users 
            where starts_with(specialty, 'Registrar - Year ')
            and show_in_locator
            and registrar_start_date is not null
            order by last_name
        """) as cur:
            return await render_template('registrar-numbers.jinja', users = await cur.fetchall())

@app.post('/registrar_numbers')
async def fetch_registrar_numbers():
    # with open(Path(__file__).parent.parent.parent / 'tmp' / 'data' / 'data.json', 'r') as f: return json.load(f)
    r = await request.get_json()
    ris = r["ris"]
    async with local_pool.connection() as conn:
        async with await conn.execute(r"""select pacs, case when can_overread then ps360 end as ps360 from users where ris = %s""", (ris,)) as cur:
            if (u := await cur.fetchone()) is None:
                logging.error(f'No PACS username found in database for RIS user: {ris}')
                return []
        pacs, ps360 = u
        user = User(
            pacs,
            ris,
            ps360,
            date.fromisoformat(r["fromDate"]),
            date.fromisoformat(r["toDate"]),
        )
        logging.info(f'Generating registrar numbers for {user.pacs} ({user.ris}) from {user.fromDate} to {user.toDate}')
        async with InteleBrowserClient(timeout=None) as client:        
            data = await client.process_user(user, conn)
    if data is None: return jsonify(None)
    report_data=data.assign(report_timestamp = data['report_timestamp'].astype(int) // 10**9, case_timestamp = data['case_timestamp'].astype(int) // 10**9).to_dict('split', index=False)['data']
    modality_pivot = data.pivot_table(index='modality',values='sum_of_parts', aggfunc=['count', 'sum'], margins=False).to_dict('split')
    time_series = data[data['modality'].isin(('CT','MR','NM','XR'))].groupby('modality').resample(pd.tseries.offsets.Week(weekday=0), on='report_timestamp', origin='end_day', include_groups=False)[['sum_of_parts']].sum()['sum_of_parts'].unstack(level='modality', fill_value=0).cumsum()
    chart_data = [dict(name=modality, data=list(series.items())) for modality, series in time_series.items()]
    return dict(
		report_data=report_data,
		modality_pivot=modality_pivot,
		chart_data=chart_data,
	)

@dataclass
class Report:
    accession: str
    timestamp: datetime
    overread: bool

@dataclass
class User:
    pacs: str
    ris: str
    ps360: int | None
    fromDate: date
    toDate: date

DB_QUERY = r"""
with events as (
    select distinct on (ct_ce_serial)
        ct_ce_serial,
        ct_dor at time zone 'Pacific/Auckland' as ris_timestamp,
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
             to_timestamp((elements_array ->> 1)::double precision) as audit_timestamp,
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
    ce_dor at time zone 'Pacific/Auckland' as case_timestamp,
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

    async def process_user(self, user: User, conn: AsyncConnection) -> pd.DataFrame | None:
        async with PS360() as ps360:
            impressions = await self.fetch_impressions(user, conn, ps360)

        return await self.fetch_ris_data(user, impressions)

    async def fetch_impressions(self, user: User, conn: AsyncConnection, ps360: PS360) -> list[Report]:
        _from, _to = datetime.combine(user.fromDate, time.min, tzinfo=TZ), datetime.combine(user.toDate, time.max, tzinfo=TZ)
        today = datetime.now(tz=TZ).date()
        async with conn.cursor() as cur:
            if user.ps360 is not None:
                await cur.execute(r"""
                    select overread from registrar_numbers
                    where user_pacs = %s and overread is not null
                    order by overread desc
                    limit 1
                    """, [user.pacs], prepare=True)
                scrape_from_datetime = datetime.combine(row[0].astimezone(tz=TZ).date() if (row := await cur.fetchone()) is not None else today.replace(year=today.year - 2), time.min, tzinfo=TZ)
                scrape_to_datetime = datetime.combine(today, time.max, tzinfo=TZ)
                async with asyncio.TaskGroup() as tg:
                    async def get_report(reportId: int, accession: str):
                        if (overread := await ps360.get_overread(reportId)) is not None:
                            await cur.execute(r"""
                                insert into registrar_numbers_accessions (accession, ps360_report_id) values (%s, %s)
                                on conflict (accession)
                                do update set ps360_report_id = excluded.ps360_report_id
                                where registrar_numbers_accessions.ps360_report_id is null
                                """, [accession, reportId], prepare=True)
                            await cur.execute(r"""
                                insert into registrar_numbers (user_pacs, accession, overread)
                                values (%s, %s, %s)
                                on conflict (user_pacs, accession)
                                do update set overread = excluded.overread
                                where registrar_numbers.overread is null
                                or registrar_numbers.overread > excluded.overread
                                """, [user.pacs, accession, overread], prepare=True)
                            if cur.rowcount > 0:
                                logging.debug(f'{user.pacs} overread {accession} at {overread}')
                    logging.info(f"Scraping PS360 overread data for {user.pacs} from {scrape_from_datetime} to {scrape_to_datetime}")
                    async for reportId, accession in ps360.orders(user.ps360, scrape_from_datetime, scrape_to_datetime):
                        tg.create_task(get_report(reportId, accession))
            await cur.execute(r"""
                select impression from registrar_numbers
                where user_pacs = %s and impression is not null
                order by impression desc
                limit 1
                """, [user.pacs], prepare=True)
            if (row := await cur.fetchone()) is not None:
                last_database_timestamp: datetime = row[0]
                scrape_from_date = last_database_timestamp.astimezone(tz=TZ).date()
            else:
                scrape_from_date = today.replace(year=today.year - 10)
            logging.info(f"Scraping impressions data for {user.pacs} from {scrape_from_date} to {today}")
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
                    await cur.execute(r"""
                        select accession from registrar_numbers_accessions
                        where pacs_audit_uid = %s
                        """, [uid], prepare=True)
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
                            await cur.execute(r"""
                                insert into registrar_numbers_accessions (accession, pacs_audit_uid)
                                values (%s, %s)
                                on conflict (accession)
                                do update set pacs_audit_uid = excluded.pacs_audit_uid
                                where registrar_numbers_accessions.pacs_audit_uid is null
                                """, [accession, uid], prepare=True)
                            scraped_count += 1
                        else:
                            logging.error(f'Error fetching accession for uid: {uid}')
                            continue
                    await cur.execute(r"""
                        insert into registrar_numbers (user_pacs, accession, impression)
                        values (%s, %s, %s)
                        on conflict (user_pacs, accession)
                        do update set impression = excluded.impression
                        where registrar_numbers.impression is null
                        or registrar_numbers.impression > excluded.impression
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
                """, [user.pacs, _from, _to], prepare=True)
            return [Report(accession, timestamp, overread) for accession, timestamp, overread in await cur.fetchall()]

    async def fetch_ris_data(self, user: User, reports: list[Report]) -> pd.DataFrame | None:
        async with comrad_pool.connection() as conn:
            async with await conn.execute(DB_QUERY, [
                user.ris,
                user.fromDate,
                user.toDate,
                Jsonb([(report.accession, report.timestamp.timestamp(), report.overread) for report in reports]),
            ]) as cur:
                column_names = [desc.name for desc in cur.description] # pyright: ignore[reportOptionalIterable]
                df = pd.DataFrame(await cur.fetchall(), columns=column_names)
                if df.empty:
                    logging.info(f"No orders retrieved from RIS database")
                    return None
                logging.info(f"Retrieved {len(df)} orders from RIS database, parsing...")
                df['sum_of_parts'] = df.apply(lambda row: max(parse(row['description']) if row['modality'] == 'XR' else 1, len(row['exams'])), axis=1, result_type='reduce')
                logging.info(f"Parsing complete")
                return df

@app.get('/lookup_table')
async def get_lookup_table():
    return LOOKUP_TABLE

@app.get('/scrape_all')
async def scrape_all():
    async with local_pool.connection() as conn:
        async with await conn.execute(r"""
            select pacs, ris, case when can_overread then ps360 end as ps360 from users
            where starts_with(specialty, 'Registrar - Year ')
            and show_in_locator
            order by last_name
            """) as cur:
            users = await cur.fetchall()
        async with InteleBrowserClient(timeout=None) as client, PS360() as ps360:
            today = datetime.now(tz=TZ).date()
            for pacs, ris, ps360_id in users:
                await client.fetch_impressions(User(
                    pacs,
                    ris,
                    ps360_id,
                    today,
                    today,
                ), conn, ps360)
    logging.info(f"Scraping complete")
    return 'Scraping complete'