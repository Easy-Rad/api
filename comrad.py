import atexit
import json
from os import environ

from app import app
from flask import jsonify
from flask import request
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from autotriage import autotriage, AutoTriageError, remember
from werkzeug.exceptions import BadRequest

DB_HOST = environ.get('DB_HOST', '159.117.39.229')
DB_PORT = environ.get('DB_PORT', '5432')
DB_NAME = environ.get('DB_NAME', 'prod_cdhb')
DB_USER = environ['DB_USER']
DB_PASSWORD = environ['DB_PASSWORD']

pool = ConnectionPool(
    open=True,
    min_size=1,
    max_size=4,
    kwargs=dict(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
)
atexit.register(pool.close)

dashboard_query = r"""
select
    rf_registered_id id,
    rf_site::text site,
    rf_pat_type::text patient_type,
    case (rf_priority)
        when 'S' then 'STAT'
        when 'O' then '1 hour'
        when 'F' then '4 hours'
        when 'T' then '24 hours'
        when 'V' then '2 days'
        when 'W' then '2 weeks'
        when 'X' then '4 weeks'
        when 'Y' then '6 weeks'
        when 'P' then 'Planned'
        when 'D' then 'DAROT'
    end urgency,
    pa_nhi::text as nhi,
    initcap(pa_surname) as pa_surname,
    initcap(pa_firstname) as pa_firstname,
    rf_pat_location::text as location,
    rf_reason::text as description,
    extract(epoch from rf_dor at time zone 'Pacific/Auckland')::int as received,
    coalesce(gen_notes.notes, array []::text[]) as gen_notes,
    coalesce(rad_notes.notes, array []::text[]) as rad_notes
from case_referral
join patient on rf_pno=pa_pno
left join lateral (
    select array_agg(trim(s) order by no_serial) as notes
    from notes
    left join doctext on no_serial = te_key and xml_is_well_formed_document(te_text)
    cross join lateral unnest(xpath('//p/text()', xmlparse(document te_text))::text[]) as s
    where no_key = rf_serial
    and no_type = 'F'
    and no_category = 'Q'
    and no_sub_category = 'M'
    and no_status = 'A'
    and trim(s) <> ''
    group by rf_registered_id
) as gen_notes on true
left join lateral (
    select array_agg(trim(s) order by no_serial) as notes
    from notes
    left join doctext on no_serial = te_key and xml_is_well_formed_document(te_text)
    cross join lateral unnest(xpath('//p/text()', xmlparse(document te_text))::text[]) as s
    where no_key = rf_serial
    and no_type = 'F'
    and no_category = 'Q'
    and no_sub_category = 'G'
    and no_status = 'A'
    and trim(s) <> ''
    group by rf_registered_id
) as rad_notes on true
where (rf_new_rf_serial=0 or rf_new_rf_serial is null)
and rf_status='W'
and (rf_site in ('CDHB','EMER') or (rf_exam_type='NM' and rf_site='NUC'))
and rf_pat_type in ('INP','ED')
and rf_exam_type=%s
order by rf_dor desc
"""


@app.get('/dashboard/<modality>')
# ["CT", "DI", "DS", "DX", "MC", "MM", "MR", "NM", "NO", "OD", "OT", "PT", "SC", "US", "XR"]
def get_dashboard(modality: str):
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("set local enable_sort = off")
            cur.execute("set local jit_above_cost = -1")
            cur.execute(dashboard_query, [modality], prepare=True)
            return cur.fetchall()


comrad_sessions = r"""
with log as (
select max(sg_serial) as sg_serial
from syslog
where sg_error_number = 121
and sg_datetime > now() at time zone 'Pacific/Auckland' - '3 days'::interval
group by sg_user, sg_terminal_tcpip
order by sg_serial desc
)
select
    extract(epoch from sg_datetime at time zone 'Pacific/Auckland')::int as last_login,
    sg_user::text as user_id,
    st_firstnames::text as firstname,
    st_surname::text as surname,
    -- ad_mobile_no as cell_ph,
    -- ad_work_no as work_ph,
    case
        when st_qualification like 'RA%' then 'Radiologist'
        when st_qualification = 'FW' then 'Fellow'
        else 'Registrar'
    end as role,
    ad_add3::text as specialty,
    tl_name::text  terminal,
    (regexp_match(sg_err_supplement, 'Login from ((?:[0-9]{1,3}\.){3}[0-9]{1,3})'))[1] as ip
from log
join syslog using (sg_serial)
join staff on sg_user = st_user_code
join address on st_ad_serial = ad_serial
join terminal on sg_terminal_tcpip = terminal.tl_tcpip
where sg_err_supplement LIKE 'Login from %'
and st_status = 'A'
and st_job_class in ('MC', 'JR')
and (st_qualification in ('JR', 'SR', 'FW') or st_qualification like 'RA%')
"""

# fetch("https://cdhbdepartments.cdhb.health.nz/site/Radiology/_api/Web/Lists(guid'5c51d5f9-e243-4766-b665-5a1877400e77')/items?$select=Title,DeskName,DeskId,DeskPhone&$orderby=DeskName,DeskId", {headers: {Accept: "application/json; odata=nometadata"}})
with open('data/desks.json', 'r') as f:
    desks = {desk['Title']: dict(
        id=desk['DeskId'],
        group=desk['DeskName'],
        phone=desk['DeskPhone'],
    ) for desk in json.load(f)}


@app.get('/locator')
def get_locator():
    result = []
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(comrad_sessions, prepare=True)
            for row in cur:
                row['desk'] = desks.get(row['ip'])
                result.append(row)
            return result


triaged_query = r"""
with codes as (
    select max(rf_serial) as rf_serial,
    array_agg(distinct ex_code::text) exam_codes,
    array_agg(distinct ex_description::text) exam_names
    from case_referral
        join case_referral_exam on rf_registered_id = rfe_reg_serial and rfe_exam <> 0 and rfe_status = 'A'
        join exams on rfe_exam = ex_serial
    where rf_new_rf_serial = 0
    and rf_site in ('CDHB', 'EMER', 'PARK', 'CWHO', 'BURW', 'NUC', 'ASH')
    group by rf_dor
    order by rf_dor desc
    limit 100
), requests as (
    select *,
    XMLPARSE(document te_text) doc
    from codes
    join case_referral using (rf_serial)
    left join notes on no_key = rf_serial and no_type = 'F' and no_category = 'Q' and no_sub_category = 'R' and no_status = 'A'
    left join doctext on te_key = no_serial and te_key_type = 'N' and xml_is_well_formed_document(te_text)
) select
    extract(epoch from rf_dor at time zone 'Pacific/Auckland')::int as received,
    pa_nhi::text nhi,
    pa_firstname::text as patient_firstname,
    pa_surname::text as patient_surname,
    extract(YEAR from age(pa_dob))::int patient_age,
    pa_sex patient_sex,
    rf_original_priority request_priority,
    rf_exam_type modality,
    rf_reason requested_exam,
    case
        when xpath_exists('//p[@id="CLINDETAILS"]',doc)
        then substr(array_to_string(xpath('//p[@id="CLINDETAILS"]/text()|//p[@id="CLINDETAILS"]/following-sibling::p[
            not(@id)
            and count(preceding-sibling::p[@id][1]|//p[@id="CLINDETAILS"])=1
            and count(preceding-sibling::p[@id and @id!="CLINDETAILS" and position() < count(preceding-sibling::p[@id="CLINDETAILS"]/following-sibling::p)])=0
            ]/text()', doc)::text[],' '),3)
        when xpath_exists('//p[b[contains(text(), "Clinical Notes")]]',doc)
        then substr((xpath('//p[b[contains(text(), "Clinical Notes")]]/text()', doc)::text[])[1],2)
    end
        as clinical_details,
    substring((xpath('//p[@id="EGFRRESULT"]/text()', doc))[1]::text from '(\d+) mL/min/1.73m2')::int
        as egfr,
    rf_triage_team as triage_team,
    extract(epoch from rf_triage_complete at time zone 'Pacific/Auckland')::int as triaged,
    st_firstnames::text as user_firstname,
    st_surname::text as user_surname,
    exam_codes,
    exam_names,
    case (rf_priority)
        when 'S' then 'STAT'
        when 'O' then '1 hour'
        when 'F' then '4 hours'
        when 'T' then '24 hours'
        when 'V' then '2 days'
        when 'W' then '2 weeks'
        when 'X' then '4 weeks'
        when 'Y' then '6 weeks'
        when 'P' then 'Planned'
    end as triage_priority,
    case when rf_triage_rank between 1 and 5 then rf_triage_rank end as triage_rank
from requests
join patient on rf_pno = pa_pno
join staff on rf_triage_completed_staff = st_serial
"""


@app.get('/triaged')
def get_triaged():
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(triaged_query, prepare=True)
            return cur.fetchall()

# request_query= r"""
# select
# extract(epoch from rf_dor at time zone 'Pacific/Auckland')::int received,
# extract(YEAR from age(pa_dob))::int patient_age,
# pa_sex patient_sex,
# rf_original_priority request_priority,
# rf_exam_type modality,
# case
#     when xpath_exists('//p[@id="EXAMREQUESTED"]',doc) then
#         substring((xpath('//p[@id="EXAMREQUESTED"]/text()', doc))[1]::text, 3)
#     when xpath_exists('//p[b[text()="Reason For Study:"]]',doc) then
#         substring((xpath('//p[b[text()="Reason For Study:"]]/text()',doc))[1]::text, 2)
#     end requested_exam,
# rf_reason normalised_exam,
# case
#     when xpath_exists('//p[@id="CLINDETAILS"]',doc) then
#         substr(array_to_string(xpath('//p[@id="CLINDETAILS"]/text()|//p[@id="CLINDETAILS"]/following-sibling::p[not(@id) and count(preceding-sibling::p[@id][1]|//p[@id="CLINDETAILS"])=1 and count(preceding-sibling::p[@id and @id!="CLINDETAILS" and position() < count(preceding-sibling::p[@id="CLINDETAILS"]/following-sibling::p)])=0]/text()', doc)::text[],' '),3)
#     when xpath_exists('//p[b[contains(text(), "Clinical Notes")]]',doc) then
#         substr((xpath('//p[b[contains(text(), "Clinical Notes")]]/text()', doc)::text[])[1],2)
#     end clinical_details,
# substring((xpath('//p[@id="EGFRRESULT"]/text()', doc))[1]::text from '(\d+) mL/min/1.73m2')::int egfr
# from case_referral
# join patient on rf_pno = pa_pno
# left join (
#     select no_key,
#     XMLPARSE(document te_text) doc
#     from notes
#     left join doctext on te_key = no_serial and te_key_type = 'N' and xml_is_well_formed_document(te_text)
#     where no_type = 'F' and no_category = 'Q' and no_sub_category = 'R' and no_status = 'A'
# ) xml on no_key = rf_serial
# where rf_serial=%s
# """

# @app.get('/request/<int:request_serial>')
# def get_request(request_serial: int):
#     with pool.connection() as conn:
#         with conn.cursor(row_factory=dict_row) as cur:
#             cur.execute(request_query, [request_serial], prepare=True)
#             if result := cur.fetchone():
#                 result['autotriage'] = autotriage(result['modality'], result['requested_exam'], result['normalised_exam'])
#                 return result
#     return jsonify() # return null if no request found


autotriage_query = r"""
select
rf_exam_type modality,
case
    when xpath_exists('//p[@id="EXAMREQUESTED"]',doc) then
        substring((xpath('//p[@id="EXAMREQUESTED"]/text()', doc))[1]::text, 3)
    when xpath_exists('//p[b[text()="Reason For Study:"]]',doc) then
        substring((xpath('//p[b[text()="Reason For Study:"]]/text()',doc))[1]::text, 2)
    end requested_exam,
rf_reason normalised_exam,
extract(YEAR from age(pa_dob))::int patient_age,
substring((xpath('//p[@id="EGFRRESULT"]/text()', doc))[1]::text from '(\d+) mL/min/1.73m2')::int egfr
from case_referral
join patient on rf_pno = pa_pno
left join (
    select no_key,
    XMLPARSE(document te_text) doc
    from notes
    left join doctext on te_key = no_serial and te_key_type = 'N' and xml_is_well_formed_document(te_text)
    where no_type = 'F' and no_category = 'Q' and no_sub_category = 'R' and no_status = 'A'
) xml on no_key = rf_serial
where rf_serial=%s
"""


@app.post('/autotriage')
def post_autotriage():
    try:
        try:
            r = request.get_json(force=True)
        except BadRequest:
            raise AutoTriageError("Malformed JSON")
        try:
            user_code = r["user"]
            version = r["version"]
            referral = r["referral"]
        except KeyError as e:
            raise AutoTriageError(f"Missing key: {e.args[0]}")
        with pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(autotriage_query, [referral], prepare=True)
                result = cur.fetchone()
        if result is None:
            raise AutoTriageError(f"Invalid referral ID: {referral}")
        return autotriage(
            user_code,
            version,
            referral,
            result['modality'],
            result['requested_exam'],
            result['normalised_exam'],
            result['patient_age'],
            result['egfr'],
        )
    except AutoTriageError as e:
        return dict(error=e.message), 400

@app.post('/autotriage/remember')
def autotriage_remember():
    try:
        try:
            r = request.get_json(force=True)
        except BadRequest:
            raise AutoTriageError("Malformed JSON")
        try:
            remember(
                r["user"],
                r["modality"],
                r["exam"],
                r["code"],
            )
            return '', 204
        except KeyError as e:
            raise AutoTriageError(f"Missing key: {e.args[0]}")
    except AutoTriageError as e:
        return dict(error=e.message), 400

with open('data/body_parts.json', 'r') as f:
    body_parts = json.load(f)

ffs_query = r"""
select
re_serial as serial,
extract(epoch from ct_dor at time zone 'Pacific/Auckland')::int as reported,
or_accession_no::text as accession,
case when or_ex_type='OD' then 'XR' else or_ex_type::text end as modality,
ce_description::text as description
from case_staff
join orders on or_event_serial = ct_ce_serial and or_status!='X'
join reports on ct_key_type='R' and ct_key = re_serial and re_status !='X'
join staff on re_dictator = st_serial
join case_event on ct_ce_serial = ce_serial
join sel_table as site on site.sl_code = 'SIT' and ce_site = sl_key and sl_aux1 = 'CDHB' and ce_site NOT IN ('HAN', 'KAIK')
where
st_user_code=%s
and or_ex_type in ('CT', 'MR', 'US', 'XR' , 'OD')
and ct_staff_function ='R'
and ct_dor > now() at time zone 'Pacific/Auckland' - '2 weeks'::interval
order by ct_dor
"""


@app.get('/ffs/<user_code>')
def get_ffs(user_code: str):
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(ffs_query, [user_code], prepare=True)
            results = cur.fetchall()
    for result in results:
        if result['modality'] in ('XR', 'CT'):
            try:
                bps = body_parts[result['description']]
            except KeyError:
                continue
        else:
            bps = 1
        match result['modality']:
            case 'XR': fee = 35 if bps >= 3 else 20 if bps >= 2 else 12 if bps >= 1 else 0
            case 'CT': fee = 200 if bps >= 4 else 160 if bps >= 3 else 135 if bps >= 2 else 60 if bps >= 1 else 0
            case 'MR': fee = 75 if bps >= 1 else 0
            case 'US': fee = 12 if bps >= 1 else 0
            case _: continue
        result['ffs'] = dict(
            body_parts=bps,
            fee=fee,
        )
    return results
