from typing import LiteralString
from quart import jsonify
from psycopg.rows import dict_row
from . import app
from ..database import comrad_pool

request_query: LiteralString = r"""
select
extract(epoch from rf_dor at time zone 'Pacific/Auckland')::int received,
extract(YEAR from age(pa_dob))::int patient_age,
pa_sex patient_sex,
rf_original_priority request_priority,
rf_exam_type modality,
case
    when xpath_exists('//p[@id="EXAMREQUESTED"]',doc) then
        substring((xpath('//p[@id="EXAMREQUESTED"]/text()', doc))[1]::text, 3)
    when xpath_exists('//p[b[text()="Reason For Study:"]]',doc) then
        substring((xpath('//p[b[text()="Reason For Study:"]]/text()',doc))[1]::text, 2)
    end requested_exam,
rf_reason normalised_exam,
case
    when xpath_exists('//p[@id="CLINDETAILS"]',doc) then
        substr(array_to_string(xpath('//p[@id="CLINDETAILS"]/text()|//p[@id="CLINDETAILS"]/following-sibling::p[not(@id) and count(preceding-sibling::p[@id][1]|//p[@id="CLINDETAILS"])=1 and count(preceding-sibling::p[@id and @id!="CLINDETAILS" and position() < count(preceding-sibling::p[@id="CLINDETAILS"]/following-sibling::p)])=0]/text()', doc)::text[],' '),3)
    when xpath_exists('//p[b[contains(text(), "Clinical Notes")]]',doc) then
        substr((xpath('//p[b[contains(text(), "Clinical Notes")]]/text()', doc)::text[])[1],2)
    end clinical_details,
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

@app.get('/request/<int:request_serial>')
async def get_request(request_serial: int):
    async with comrad_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(request_query, [request_serial])
            return result if (result := await cur.fetchone()) else jsonify() # return null if no request found
