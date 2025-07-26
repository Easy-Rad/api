import atexit
from os import environ
from psycopg_pool import ConnectionPool
from tokenise import tokenise_request


class AutoTriageError(Exception):
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)


pool_autotriage = ConnectionPool(
    environ['AUTOTRIAGE_CONN'],
    min_size=1,
    max_size=4,
    open=True,
)
atexit.register(pool_autotriage.close)

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
) -> dict | None:
    tokenised = tokenise_request(
        requested_exam) if requested_exam is not None else tokenise_request(normalised_exam)
    request=dict(
        modality=modality,
        exam=requested_exam or normalised_exam,
    )
    with pool_autotriage.connection() as conn:
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

def remember(
    user: str,
    modality: str,
    exam: str,
    code: str,
) -> None:
    with pool_autotriage.connection() as conn:
        conn.execute(autotriage_remember, [user, modality, tokenise_request(exam), code], prepare=True)