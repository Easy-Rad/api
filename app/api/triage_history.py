from typing import LiteralString
from psycopg.rows import dict_row
from . import app
from ..database import comrad_pool

triage_history_query: LiteralString = r"""
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
    rf_serial as id,
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


@app.get('/triage_history')
async def get_triage_history():
    async with comrad_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(triage_history_query, prepare=True)
            return await cur.fetchall()