import atexit
from os import environ
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from tokenise import tokenise_request
from app import app

pool = ConnectionPool(
    environ['AUTOTRIAGE_CONN'],
    min_size=1,
    max_size=4,
    open=True,
)
atexit.register(pool.close)

autotriage_query = r"""
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

autotriage_log = r"""
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

autotriage_remember = r"""
insert into label (username, modality, tokenised, code)
values (%s, %s, %s, %s)
on conflict (tokenised, modality, username) do update
set code = excluded.code
"""

def autotriage(
    user_code: str,
    version: str,
    referral: int,
    modality: str,
    requested_exam: str | None,
    normalised_exam: str,
    patient_age: int,
    egfr: int | None,
) -> dict:
    tokenised = tokenise_request(
        requested_exam) if requested_exam is not None else tokenise_request(normalised_exam)
    request=dict(
        modality=modality,
        exam=requested_exam or normalised_exam,
    )
    with pool.connection() as conn:
        with conn.execute(autotriage_query, [user_code, tokenised, modality], prepare=True) as cur:
            result = cur.fetchone()
        if result is None and requested_exam is not None:
            with conn.execute(autotriage_query, [user_code, tokenise_request(normalised_exam), modality], prepare=True) as cur:
                result = cur.fetchone()
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
        with conn.cursor() as cur:
            cur.execute(autotriage_log, [
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
        request=request,
        result=result,
    )

def remember_autotriage(
    user: str,
    modality: str,
    exam: str,
    code: str,
) -> None:
    with pool.connection() as conn:
        conn.execute(autotriage_remember, [user, modality, tokenise_request(exam), code], prepare=True)

def ffs(results):
    with pool.connection() as conn:
        with conn.execute(
            "select description, parts from unnest(%s::text[]) as description left join ffs_body_parts on name = description",
            [list(set([result["ce_description"].lower() for result in results if result["examtype"] in ('CT','XR')]))],
            prepare=True,
            ) as c:
            body_parts = {description: parts for description, parts in c.fetchall()}
    total_fee = 0
    unknowns = []
    tally = dict(
        CT=[0, 0, 0, 0],
        XR=[0, 0, 0],
        MR=[0],
        US=[0],
    )
    for result in results:
        if result['examtype'] in ('CT','XR'):
            bps = body_parts[result['ce_description'].lower()]
            if bps is None:
                unknowns.append(result['ce_description'].lower())
                continue
        else:
            bps = 1
        tally[result['examtype']][bps-1] += 1
        match result['examtype']:
            case 'XR': fee = 35 if bps >= 3 else 20 if bps >= 2 else 12 if bps >= 1 else 0
            case 'CT': fee = 200 if bps >= 4 else 160 if bps >= 3 else 135 if bps >= 2 else 60 if bps >= 1 else 0
            case 'MR': fee = 75 if bps >= 1 else 0
            case 'US': fee = 12 if bps >= 1 else 0
            case _: continue
        total_fee += fee
        result['ffs'] = dict(
            body_parts=bps,
            fee=fee,
        )
    return dict(
        results=results,
        count=len(results),
        tally=tally,
        fee=total_fee,
        unknowns=unknowns,
    )


@app.get('/desks')
def get_desks():
    with pool.connection() as conn:
        with conn.execute(r"""select ip, computer_name, name, area, phone from desks order by area, name""", prepare=True) as cur:
            cur.row_factory = dict_row
            result = []
            for desk in cur.fetchall():
                desk["ip"] = str(desk["ip"])
                result.append(desk)
            return result