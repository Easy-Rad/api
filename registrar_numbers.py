from app import app, TZ
from comrad import pool as comrad_pool
from coolify import pool as coolify_pool
from flask import request, render_template, Response

from os import environ
import httpx
from dataclasses import dataclass, field
from datetime import datetime, date
from lxml import html, etree # type: ignore
from psycopg.types.json import Jsonb
import logging
import pandas as pd
from parts_parser import parse

IB_HOST = environ.get('IB_HOST','app-inteleradha-p.healthhub.health.nz')
IB_USER = environ['IB_USER']
IB_PASSWORD = environ['IB_PASSWORD']

@app.get('/registrar_numbers')
def get_registrar_numbers():
    with coolify_pool.connection() as conn:
        with conn.execute(r"""select ris, last_name || ', ' || first_name || ' (' || replace(specialty, 'Registrar - Year ', 'Y') || ')' as name from users where starts_with(specialty, 'Registrar - Year ') and show_in_locator order by last_name""") as cur:
            return render_template('registrar-numbers.html', users=cur.fetchall())

@app.post('/registrar_numbers')
def fetch_registrar_numbers():
    r = request.get_json()
    ris = r["ris"]
    with coolify_pool.connection() as conn:
        with conn.execute(r"""select pacs from users where ris = %s""", (ris,)) as cur:
            if (pacs := cur.fetchone()) is None:
                logging.error(f'No PACS username found in database for RIS user: {ris}')
                return []
    user = User(
        pacs[0],
        ris,
        date.fromisoformat(r["fromDate"]),
        date.fromisoformat(r["toDate"]),
    )
    # pacs, fromDate, toDate = pacs[0], date.fromisoformat(r["fromDate"]), date.fromisoformat(r["toDate"])
    logging.info(f'Generating registrar numbers for {user.pacs} ({user.ris}) from {user.fromDate} to {user.toDate}')
    with InteleBrowserClient() as client:        
        df = client.process_user(user)
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
class InteleBrowserClient(httpx.Client):
    LOGIN_URL = f"http://{IB_HOST}/InteleBrowser/login/ws/auth"
    LOGOUT_URL = f"http://{IB_HOST}/InteleBrowser/login/ws/auth/revoke"
    APP_URL = f"http://{IB_HOST}/InteleBrowser/app"

    def __enter__(self):
        super().__enter__()
        self.post(
            self.LOGIN_URL,
            data={
                "username": IB_USER,
                "password": IB_PASSWORD,
                "mfaToken": "",
                "keepMeLoggedIn": "false",
            },
        ).raise_for_status() # log in
        self.post(
            self.APP_URL,
            data={
                "service": "direct/1/AuditDetails/auditDetailsTable.tableForm",
                "sp": "S2",
                "Form2": "pageSizeSelect,pageSizeSelect$0",
                "pageSizeSelect": "4",
                "pageSizeSelect$0": "4",
            },
        ).raise_for_status() # set page size to 1000
        return self

    def __exit__(
        self,
        exc_type = None,
        exc_value = None,
        traceback = None,
    ):
        self.get(self.LOGOUT_URL, follow_redirects=True).raise_for_status()
        super().__exit__(exc_type, exc_value, traceback)

    def process_user(self, user: User) -> pd.DataFrame:
        self.fetch_impressions(user)
        return self.fetch_ris_data(user)

    def process_users(self, users: list[User]) -> list[User]:
        # self._log_in()
        for user in users:
            self.fetch_impressions(user)

        # async with AsyncConnectionPool(
        #     kwargs=dict(
        #         host=DB_HOST,
        #         port=DB_PORT,
        #         dbname=DB_NAME,
        #         user=DB_USER,
        #         password=DB_PASSWORD,
        #     ),
        #     # min_size=1,
        #     # max_size=4,
        #     open=False,
        # ) as pool:
        for user in users:
            self.fetch_ris_data(user)
        # await asyncio.gather(*(self.fetch_ris_data(user) for user in users)) # type: ignore
            # async with pool.connection() as conn:
            #     async with await conn.execute(DB_QUERY, [
            #         user.ris,
            #         user.start,
            #         user.end,
            #         accessions,
            #     ]) as cur:
            #         logging.info(await cur.fetchall())

        # await asyncio.gather(*(self.process_user(user) for user in users))
        # async with asyncio.TaskGroup() as tg:
        #     for user in self.users:
        #         tg.create_task(self.process_user(user))
        return users

    def fetch_impressions(self, user: User) -> None:
        # logging.info("Current user: %s", user.name)
        # logging.info("Date range: %s - %s", user.fromDate, user.toDate)
        start_date = user.fromDate.strftime("%Y/%m/%d")
        end_date = user.toDate.strftime("%Y/%m/%d")

        # response = self.post(
        #     self.APP_URL,
        #     data={
        #         "service": "direct/1/AuditDetails/$Form",
        #         "sp": "S0",
        #         "Form0": "usernameFilter,$PropertySelection,patientIdFilter,studyDescriptionFilter,$PropertySelection$0,$Checkbox,$Checkbox$0,$Checkbox$1,$Checkbox$2,$Checkbox$3,$Checkbox$4,$Checkbox$5,$Checkbox$6,$Checkbox$7,$Checkbox$8,$Checkbox$9,$Checkbox$10,$Checkbox$11,$Checkbox$12,$Checkbox$13,$Checkbox$14,$Checkbox$15,$Checkbox$16,$Checkbox$17,$Checkbox$18,$Checkbox$19,$Checkbox$20,$Checkbox$21,$Checkbox$22,$Checkbox$23,$Checkbox$24,$Checkbox$25,$Checkbox$26,$Checkbox$27,$Checkbox$28,$Checkbox$29,$Checkbox$30,$Checkbox$31,$Checkbox$32,$Checkbox$33,$Checkbox$34,$Checkbox$35,$Checkbox$36,$Checkbox$37,$Checkbox$38,$Checkbox$39,$Checkbox$40,$Checkbox$41,$Checkbox$42,$Submit",
        #         "usernameFilter": user.pacs,
        #         "$PropertySelection": "anyRole",
        #         "patientIdFilter": "",
        #         "studyDescriptionFilter": "",
        #         "$PropertySelection$0": f"{start_date}:{end_date}",
        #         "$Checkbox$2": "on",  # Add Emergency Impression
        #         # "$Checkbox$9": "on",  # Complete Dictation
        #         "$Submit": "Update",
        #         "Form2": "pageSizeSelect,pageSizeSelect$0",
        #         "pageSizeSelect": "4",
        #         "pageSizeSelect$0": "4",

        #     },
        # )
        response = self.post(
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

        # async with asyncio.TaskGroup() as tg:
        #     task1 = tg.create_task(some_coro(...))
        #     task2 = tg.create_task(another_coro(...))
        page_count = 0
        while True:
            # logging.info(f"Processing page {page_count + 1}")
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
                    extra_response = self.get(
                        self.APP_URL,
                        params={"service": "xtile/null/AuditDetails/$XTile$2", "sp": uid},
                    )
                    if extra_response.content:
                        extra_root = etree.fromstring(extra_response.content)
                        if (accession_node := extra_root.find("./sp[2]")) is not None:
                            user.audit_data[uid] = AuditEntry(accession_node.text, timestamp, is_impression)
                            # if len(user.audit_data) % 100 == 0:
                            #     logging.info(f"Got {len(user.audit_data)} accessions")
                        else:
                            logging.error(f'No response for uid: {uid}')
                        
                    # if len(uids) < 5:
                    #     uids.add(uid)
            # uids = [e.attrib['studyuid'] for e in root.iterfind(".//a[@studyuid]") if e.attrib['studyuid']]
            if root.find(".//a[@name='nextPage']") is None:
                logging.info(f"Scraped {page_count + 1} InteleBrowser audit pages and got {len(user.audit_data)} accessions")
                break
            response = self.post(
                f"http://{IB_HOST}/InteleBrowser/app",
                params={'service':'direct/1/AuditDetails/auditDetailsTable.customPaginationControlTop.nextPage'},
            )
            page_count += 1

        # client.get(
        #     self.APP_URL,
        #     params={"service": "xtile/null/AuditDetails/$XTile$2", "sp": uid},
        # )
        # logging.info(f"Total: {len(user.audit_data)} accessions")
        # user.studies = [accession for accession in await asyncio.gather(*(self.get_accession(uid) for uid in uids)) if accession is not None]
        # logging.info(f"Got {len(user.studies)} accessions")
            
        # # return response.text
        # logging.info("A total of %d pages retrieved", len(pages))
        # # self.save_cache(pages)

        # result = await self._parse_pages(pages)
        # return result
        # # self._log_in()
        # await asyncio.gather(*(self.process_user(user) for user in self.users))
        # # async with asyncio.TaskGroup() as tg:
        # #     for user in self.users:
        # #         tg.create_task(self.process_user(user))
        # return users

    def fetch_ris_data(self, user: User) -> pd.DataFrame:
        # with open(filename, 'w', newline='') as file:
            # writer = csv.writer(file)
        with comrad_pool.connection() as conn:
            with conn.execute(DB_QUERY, [
                user.ris,
                user.fromDate,
                user.toDate,
                Jsonb([(data.accession, data.timestamp, data.is_impression) for data in user.audit_data.values()]),
                # list(user.audit_data),
            ]) as cur:
                # records = cur.fetchall()
                column_names = [desc.name for desc in cur.description] # type: ignore
                df = pd.DataFrame(cur.fetchall(), columns=column_names)
                # df['sum_of_parts'] = df['description'].apply(parse)
                logging.info(f"Retrieved {len(df)} orders from RIS database")
                df['sum_of_parts'] = df['exams'].apply(len)
                logging.info(f"Parsing complete")
                return df
                # filename_base = f'tmp/{user.ris}_{user.start}_{user.end}'
                # df.to_csv(filename_base + '_raw.csv', index=False)
                # logging.info(f'Wrote {cur.rowcount} records to {filename_base}')

            # async with conn.cursor() as cur:
            #     async for record in cur.stream(DB_QUERY, [
            #         user.ris,
            #         user.start,
            #         user.end,
            #         list(user.accessions),
            #     ], size=1000):
                # async for record in cur:
                #     writer.writerow(record)


    # async def add_accession(self, accessions: set[str], uid: str):
    #     response = await self.get(
    #         self.APP_URL,
    #         params={"service": "xtile/null/AuditDetails/$XTile$2", "sp": uid},
    #     )
    #     if response.content:
    #         root = etree.fromstring(response.content)
    #         if (accession_node := root.find("./sp[2]")) is not None:
    #             accessions.add(accession_node.text)
    #     else:
    #         logging.info(f'No response for uid: {uid}')
        
    # async def _get_first_page(self, user: User) -> list[str]:
    #     """
    #     Get the first page of the audit data for the given user and date range
    #     """
    #     start_date = user.start.strftime("%Y/%m/%d")
    #     end_date = user.end.strftime("%Y/%m/%d")

    #     data = {
    #         "service": "direct/1/AuditDetails/$Form",
    #         "sp": "S0",
    #         "Form0": "usernameFilter,$PropertySelection,patientIdFilter,studyDescriptionFilter,$PropertySelection$0,$Checkbox,$Checkbox$0,$Checkbox$1,$Checkbox$2,$Checkbox$3,$Checkbox$4,$Checkbox$5,$Checkbox$6,$Checkbox$7,$Checkbox$8,$Checkbox$9,$Checkbox$10,$Checkbox$11,$Checkbox$12,$Checkbox$13,$Checkbox$14,$Checkbox$15,$Checkbox$16,$Checkbox$17,$Checkbox$18,$Checkbox$19,$Checkbox$20,$Checkbox$21,$Checkbox$22,$Checkbox$23,$Checkbox$24,$Checkbox$25,$Checkbox$26,$Checkbox$27,$Checkbox$28,$Checkbox$29,$Checkbox$30,$Checkbox$31,$Checkbox$32,$Checkbox$33,$Checkbox$34,$Checkbox$35,$Checkbox$36,$Checkbox$37,$Checkbox$38,$Checkbox$39,$Checkbox$40,$Checkbox$41,$Checkbox$42,$Submit",
    #         "usernameFilter": user.username,
    #         "$PropertySelection": "anyRole",
    #         "patientIdFilter": "",
    #         "studyDescriptionFilter": "",
    #         "$PropertySelection$0": f"{start_date}:{end_date}",
    #         "$Checkbox$2": "on",  # Add Emergency Impression
    #         # "$Checkbox$9": "on",  # Complete Dictation
    #         "$Submit": "Update",
    #     }
    #     response = await self.post(
    #         f"http://{IV_HOST}/InteleBrowser/app",
    #         data=data,
    #     )
    #     return response.text


# class ServerError(Exception):
#     def __init__(self, code: int, message: str, error_class: str):
#         self.code = code
#         self.message = message
#         self.error_class = error_class
#     def __str__(self):
#         return f'{self.message} (code {self.code}) ({self.error_class})'


# async def main():
#     context = ssl.create_default_context()
#     context.verify_flags &= ~ssl.VERIFY_X509_STRICT
#     async with InteleBrowserClient(
#         # params=dict(username=IV_USER),
#         # headers={'intelerad-serialization-protocol': 'JsonSerializationProtocol1.0'},
#         # verify=context,
#     ) as client:
#         await client.process_users([
#             User(
#                 "Caro MCV",
#                 "CarolinM1",
#                 date(2025, 9, 19),
#                 date(2025, 9, 20),
#             ),
#             User(
#                 "Mo G",
#                 "MohammaG",
#                 date(2025, 9, 19),
#                 date(2025, 9, 20),
#             ),
#         ])

# async def main2():
#     context = ssl.create_default_context()
#     context.verify_flags &= ~ssl.VERIFY_X509_STRICT
#     async with httpx.AsyncClient(
#         # params=dict(username=IV_USER),
#         headers={'intelerad-serialization-protocol': 'JsonSerializationProtocol1.0'},
#         verify=context,
#     ) as client:
#         async def request(service:str, protocol:str, method: str, params: list, subpath: str | None = None):
#             result = await client.post(
#                 url=f'http://{IV_HOST}/{service}/{service if subpath is None else subpath}',
#                 headers={
#                     'intelerad-application-protocol': protocol,
#                 },
#                 json=dict(method=method, params=params),
#             )
#             result = result.raise_for_status()
#             try:
#                 result = result.json()
#             except:
#                 raise Exception(result.text)
#             if not result['success']:
#                 error = result['error']
#                 raise ServerError(
#                     code=error['code'],
#                     message=error['message'],
#                     error_class=error['data']['class'],
#                 )
#             else:
#                 # result = result['result']
#                 if result['result'] is dict and (failure := result['result']['mFailureType']) is not None:
#                     try:
#                         raise Exception(failure['enumName'])
#                     except KeyError:
#                         raise Exception(json.dumps(failure))
#                 else:
#                     return result['result']
#         async def get_new_session_id() -> str:
#             sign_in_request = await request(
#                 service='InteleViewerService',
#                 protocol='UserAuthenticationProtocol2.0',
#                 method='authenticateWithClientId',
#                 params= [
#                     IV_USER,
#                     IV_PASSWORD,
#                     'InteleViewer_5-7-1-P448',
#                     None
#                 ],
#             )
#             user, session_id = sign_in_request['mUser'], sign_in_request['mSessionId']
#             print(f'Signed in to InteleViewer as {user['mFirstName']} {user['mLastName']} with session ID {session_id}')
#             return session_id
#         async def get_users() -> dict[str, tuple[str, str]]:
#             return {user['mUsername']: (user['mPersonName']['mFirstName'], user['mPersonName']['mLastName']) for user in await request('WorklistService', 'WorkflowProtocol13.1', 'getDictatingUsers', [])}
#         # client.params = client.params.set('sessionId', get_new_session_id())
#         # result = request('InteleViewerService', 'ProtocolNegotiationProtocol1.0', 'getProtocols', [])
#         # client.params = client.params.set('username', IV_USER).set('sessionId', 'af35fd415fad96a8f2aa42380fd9ef42')
#         # client.cookies.set('PACS_SESSION_ID', await get_new_session_id())
#         # client.cookies.set('PACS_SESSION_ID', 'af35fd415fad96a8f2aa42380fd9ef42')

#         # start_date = date(2025,9,20).strftime("%Y/%m/%d")
#         # end_date = date(2025,9,21).strftime("%Y/%m/%d")

#         # data = {
#         #     "service": "direct/1/AuditDetails/$Form",
#         #     "sp": "S0",
#         #     "Form0": "usernameFilter,$PropertySelection,patientIdFilter,studyDescriptionFilter,$PropertySelection$0,$Checkbox,$Checkbox$0,$Checkbox$1,$Checkbox$2,$Checkbox$3,$Checkbox$4,$Checkbox$5,$Checkbox$6,$Checkbox$7,$Checkbox$8,$Checkbox$9,$Checkbox$10,$Checkbox$11,$Checkbox$12,$Checkbox$13,$Checkbox$14,$Checkbox$15,$Checkbox$16,$Checkbox$17,$Checkbox$18,$Checkbox$19,$Checkbox$20,$Checkbox$21,$Checkbox$22,$Checkbox$23,$Checkbox$24,$Checkbox$25,$Checkbox$26,$Checkbox$27,$Checkbox$28,$Checkbox$29,$Checkbox$30,$Checkbox$31,$Checkbox$32,$Checkbox$33,$Checkbox$34,$Checkbox$35,$Checkbox$36,$Checkbox$37,$Checkbox$38,$Checkbox$39,$Checkbox$40,$Checkbox$41,$Checkbox$42,$Submit",
#         #     "usernameFilter": "MohammaG",
#         #     "$PropertySelection": "anyRole",
#         #     "patientIdFilter": "",
#         #     "studyDescriptionFilter": "",
#         #     "$PropertySelection$0": f"{start_date}:{end_date}",
#         #     "$Checkbox$2": "on",  # Add Emergency Impression
#         #     "$Checkbox$9": "on",  # Complete Dictation
#         #     "$Submit": "Update",
#         # }
#         # response = await client.post(
#         #     f"http://app-inteleradha-p.healthhub.health.nz/InteleBrowser/app",
#         #     data=data,
#         #     cookies={'PACS_SESSION_ID': '35f9ea1523d7291c4cf0785a7d8e0aa7'},
#         # )
#         # print (response.text)
#         # return
#         # return response.text
#         # client.params = client.params.set('username', IV_USER).set('sessionId', await get_new_session_id())
#         # result = request('WorklistService', 'StudyClassifierProtocol1.0', 'getListOfConfiguredProperties', [])
#         # result = request('WorklistService', 'StudyClassifierProtocol1.0', 'getListOfConfiguredProperties', [])
#         # result = request('WorklistService', 'StudyClassifierProtocol1.0', 'getListOfConfiguredProperties', [])
        
#         # /InteleViewerService/InteleViewerService
#         # result = request('WorklistService', 'WorklistProtocol8.2', 'queryOrdersWithoutReports', [])
#         # result = request('WorklistService', 'WorklistProtocol8.2', 'getOrdersByAccessionNumbersWithoutReports', [['CA-19349655-CT']])
#         # result = await request('WorklistService', 'WorklistProtocol8.2', 'getOrdersByAccessionNumbersWithoutReports', [['CA-19331208-MR', 'CA-19349655-CT']])
#         # result = await request('WorklistService', 'QueryServiceProtocol1.0', 'queryServices', [])
#         # result = await request('WorklistService', 'ProtocolNegotiationProtocol1.0', 'getProtocols', [])
#         # result = await request('RoamingProfiles', 'ProtocolNegotiationProtocol1.0', 'getProtocols', [], subpath='Services')
#         # result = await request('RoamingProfiles', 'QueryServiceProtocol1.0', 'getService ', [], subpath='Services')
#         # result = await request('RoamingProfiles', 'QueryServiceProtocol1.0', 'getService ', [], subpath='Services')
#         # result = await request('WorklistService', 'WorklistProtocol8.2', 'getOrdersByAccessionNumbersWithoutReports', [['CA-19349655-CT']])
#         # result = await request('WorklistService', 'WorklistProtocol8.2',
#         #                     #    'queryOrdersWithoutReports',
#         #                        'queryOrderCounts',
#         #                        [
#         #     {
#         #         'class': 'com.intelerad.worklistservice.lib.transferobject.WorklistIdTransferObject',
#         #         'mWorklistIdName': 'ready',
#         #     },
#         #     [{
#         #         'class': 'com.intelerad.datamodels.search.transferobject.MatchTransferObject',
#         #         'mClauses': [{
#         #             'class': 'com.intelerad.datamodels.search.transferobject.ClauseTransferObject',
#         #             'mTermList': [{
#         #                 'class': 'com.intelerad.datamodels.search.transferobject.TermTransferObject',
#         #                 'mAttribute': 'uid',
#         #                 'mOperatorName': 'Equals',
#         #                 'mValue': '1.2.840.114202.4.1160887503.2156455695.4202856821.1717279547',
#         #             }],
#         #             # 'mTermList': [{
#         #             #     'class': 'com.intelerad.datamodels.search.transferobject.TermTransferObject',
#         #             #     'mAttribute': 'BodyPartList',
#         #             #     'mOperatorName': 'Contains',
#         #             #     'mValue': '1.2.840.114202.4.1160887503.2156455695.4202856821.1717279547',
#         #             # }],
#         #         }],
#         #     }]
#         # ])
#         # users = await get_users()
#         # studyuid="1.2.840.114202.4.1160887503.2156455695.4202856821.1717279547"
#         # QueryServiceProtocol1.0
#         # print (result[0]['mResult'])
#         # client.params.set('sessionId', 'f0b367c294503f6f1b43fb79b217436f')

# async def main():
#     context = ssl.create_default_context()
#     context.verify_flags &= ~ssl.VERIFY_X509_STRICT
#     with InteleBrowserClient() as client:        
#         await client.process_users([
#             # User(
#             #     "Caro MCV",
#             #     "CarolinM1",
#             #     "CARM",
#             #     date(2025, 9, 20),
#             #     date(2025, 9, 22),
#             # ),
#             # User(
#             #     "Mo G",
#             #     "MohammaG",
#             #     "MOGH",
#             #     date(2025, 4, 19),
#             #     date(2025, 9, 20),
#             # ),
#             # User(
#             #     "Angelo",
#             #     "AngeloD",
#             #     "ADB",
#             #     date(2025, 9, 20),
#             #     date(2025, 9, 22),
#             # ),
#             User(
#                 "EGanly",
#                 "ESG",
#                 date(2025, 9, 22),
#                 date(2025, 9, 22),
#             ),
#         ])