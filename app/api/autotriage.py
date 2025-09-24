import re
from typing import LiteralString
from quart import request
from werkzeug.exceptions import BadRequest
from psycopg.rows import dict_row
from .error import ApiError
from . import app
from ..database import comrad_pool, local_pool


referral_data_query: LiteralString = r"""
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

autotriage_query: LiteralString = r"""
select
examination.code,
name as exam,
body_part,
coalesce(username=%s, false) as custom
from label
join examination on label.code=examination.code and label.modality=examination.modality
where tokenised=%s and label.modality=%s
order by custom desc, username is not null
limit 1
"""

autotriage_log_query: LiteralString = r"""
insert into triage_log (
    username,
    version,
    modality,
    referral,
    requested_exam,
    normalised_exam,
    tokenised,
    code
) values (%s, %s, %s, %s, %s, %s, %s, %s)
"""

remember_autotriage_query: LiteralString = r"""
insert into label (username, modality, tokenised, code)
values (%s, %s, %s, %s)
on conflict (tokenised, modality, username) do update
set code = excluded.code
"""

@app.post('/autotriage')
async def post_autotriage():
    try:
        try:
            r = await request.get_json(force=True)
        except BadRequest:
            raise ApiError("Malformed JSON")
        try:
            user_code = r["user"]
            version = r["version"]
            referral = r["referral"]
        except KeyError as e:
            raise ApiError(f"Missing key: {e.args[0]}")
        async with comrad_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(referral_data_query, [referral], prepare=True)
                result = await cur.fetchone()
        if result is None:
            raise ApiError(f"Invalid referral ID: {referral}")
        modality = result['modality']
        requested_exam = result['requested_exam']
        normalised_exam = result['normalised_exam']
        patient_age = result['patient_age']
        egfr = result['egfr']
        tokenised = tokenise_request(requested_exam) if requested_exam is not None else tokenise_request(normalised_exam)
        async with local_pool.connection() as conn:
            async with await conn.execute(autotriage_query, [user_code, tokenised, modality], prepare=True) as cur:
                result = await cur.fetchone()
            if result is None and requested_exam is not None:
                async with await conn.execute(autotriage_query, [user_code, tokenise_request(normalised_exam), modality], prepare=True) as cur:
                    result = await cur.fetchone()
            if result is not None:
                code, exam, body_part, custom = result
                if code == 'Q25' and (patient_age >= 80 or egfr is not None and egfr < 30):
                    code = 'Q25T'  # Barium-tagged CT colonography
                result = dict(
                    body_part=body_part,
                    code=code,
                    exam=exam,
                    custom=custom,
                )
            else:
                code = None
            async with conn.cursor() as cur:
                await cur.execute(autotriage_log_query, [
                    user_code,
                    version,
                    modality,
                    referral,
                    requested_exam,
                    normalised_exam,
                    tokenised,
                    code,
                ], prepare=True)
        return dict(
            request=dict(
                modality=modality,
                exam=requested_exam or normalised_exam,
            ),
            result=result,
        )
    except ApiError as e:
        return dict(error=e.message), 400

@app.post('/autotriage/remember')
async def autotriage_remember():
    try:
        try:
            r = await request.get_json(force=True)
        except BadRequest:
            raise ApiError("Malformed JSON")
        try:
            user = r["user"]
            modality = r["modality"]
            exam = r["exam"]
            code = r["code"]
        except KeyError as e:
            raise ApiError(f"Missing key: {e.args[0]}")
        async with local_pool.connection() as conn:
            await conn.execute(remember_autotriage_query, [user, modality, tokenise_request(exam), code], prepare=True)
            return '', 204
    except ApiError as e:
        return dict(error=e.message), 400

def tokenise_request(s: str) -> str:
    s = re.sub(
        # remove non-alphanumeric characters except for C- and C+
        # remove irrelevant words including modality
        pattern=r'[^\w+-]|(?<!\bC)[+-]|\b(and|or|with|by|left|right|please|GP|CT|MRI?|US|ultrasound|scan|study|protocol|contrast)\b',
        repl=' ',
        string=s,
        flags=re.IGNORECASE|re.ASCII,
    )
    return ' '.join(sorted(re.split(r'\s+', s.lower().strip()))) # remove extra whitespace