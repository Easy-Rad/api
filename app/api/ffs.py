from typing import LiteralString
from datetime import date, timedelta
from quart import request
from werkzeug.exceptions import BadRequest
from psycopg.rows import dict_row

from . import app, HOLIDAYS
from ..database import comrad_pool, local_pool
from .error import ApiError

ffs_users: LiteralString = r"""
with staff_list as (
    select distinct ct_staff_serial
    from case_staff
    join case_event on ct_ce_serial = ce_serial and ct_staff_function = 'R'
    join reports on ct_key = re_serial and ct_key_type = 'R' and (re_old_version is null or re_old_version = 0)
    join orders on or_event_serial = ce_serial and or_status != 'X'
    join sel_table as site ON ce_site = site.sl_key AND site.sl_code = 'SIT'
    where ct_dor >= %s and ct_dor < %s + 1
    and or_ex_type IN ('CT', 'MR', 'US', 'XR', 'OD')
    and site.sl_aux1 = 'CDHB'
    and ce_site NOT IN ('HAN', 'KAIK')
    and (
        extract(hour from ct_dor) not between 8 and 17
        or extract(isodow from ct_dor) > 5
        or date(ct_dor) = any(%s)
    )
)
select st_user_code::text, st_surname::text, st_firstnames::text
from staff_list
join staff on st_serial = ct_staff_serial
where st_job_class = 'MC'
and st_user_code !~ 'Z[A-Z]+RAD'
and st_user_code <> 'ELR'
order by st_surname
"""

ffs_query: LiteralString = r"""
with reports as (
    select
    case_staff_V.ct_dor as nz_time,
    extract(hour from case_staff_V.ct_dor) not between 8 and 17 as nz_after_hours,
    extract(isodow from case_staff_V.ct_dor) > 5 as nz_weekend,
    date(case_staff_V.ct_dor) = any(%s) as nz_holiday,
    case_staff_V.ct_dor at time zone 'Pacific/Auckland' at time zone %s as local_time,
    or_accession_no::text,
    case when or_ex_type = 'OD' then 'XR' else or_ex_type end as examType,
    ce_description,
    ce_start,
    ce_site::text,
    staff_V.st_surname || ', ' || staff_V.st_firstnames as verifier,
    case when staff_R.st_serial <> staff_V.st_serial then staff_R.st_surname || ', ' || staff_R.st_firstnames end as prelim_reporter
    from case_staff case_staff_V
    join staff staff_V on st_serial = ct_staff_serial
    join case_event on case_staff_V.ct_ce_serial = ce_serial and case_staff_V.ct_staff_function = 'R'
    join case_staff case_staff_R on case_staff_R.ct_ce_serial = ce_serial and case_staff_R.ct_staff_function = 'V'
    join staff staff_R on staff_R.st_serial = case_staff_R.ct_staff_serial
    join reports on case_staff_V.ct_key = re_serial and case_staff_V.ct_key_type = 'R' and case_staff_R.ct_key = re_serial and case_staff_R.ct_key_type = 'R' and (re_old_version is null or re_old_version = 0)
    join orders on or_event_serial = ce_serial and or_status != 'X'
    join sel_table as site ON ce_site = site.sl_key AND site.sl_code = 'SIT'
    where staff_V.st_user_code = %s
    and case_staff_V.ct_dor >= %s and case_staff_V.ct_dor < %s + 1
    and or_ex_type IN ('CT', 'MR', 'US', 'XR', 'OD')
    and site.sl_aux1 = 'CDHB'
    and ce_site NOT IN ('HAN', 'KAIK')
),
critieria as (
    select *,
    nz_after_hours or nz_weekend or nz_holiday as nz_eligible,
    extract(hour from local_time) between 6 and 22 as local_eligible
    from reports
)
select *,
nz_eligible and local_eligible as eligible
from critieria
where coalesce(%s = (nz_eligible and local_eligible), true)
"""

@app.post('/ffs')
async def post_ffs():
    try:
        try:
            r = await request.get_json(force=True)
        except BadRequest:
            raise ApiError("Malformed JSON")
        try:
            from_date, to_date = date.fromisoformat(r["from"]), date.fromisoformat(r["to"])
            if (delta := (to_date - from_date).days) >= 28:
                raise ApiError("Maximum 28 days")
            holidays = [d for d in (from_date + timedelta(days=x) for x in range(delta + 1)) if d in HOLIDAYS]
        except ApiError:
            raise
        except KeyError as e:
            raise ApiError(f"Missing key: {e.args[0]}")
        except ValueError as e:
            raise ApiError(str(e))
        except:
            raise ApiError("Invalid request")
        async with comrad_pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                try:
                    await cur.execute(ffs_query, [
                        holidays,
                        r["timezone"],
                        r["user"],
                        from_date,
                        to_date,
                        r["eligible"],
                    ],prepare=True)
                    results = await cur.fetchall()
                except KeyError:
                    await cur.execute(ffs_users, [
                        from_date,
                        to_date,
                        holidays,
                    ],prepare=True)
                    return {u["st_user_code"]: (u["st_firstnames"], u["st_surname"]) async for u in cur}
        async with local_pool.connection() as conn:
            async with await conn.execute(
                "select description, parts from unnest(%s::text[]) as description left join ffs_body_parts on name = description",
                [list(set([result["ce_description"].lower() for result in results if result["examtype"] in ('CT','XR')]))],
                prepare=True,
                ) as cur:
                body_parts = {description: parts async for description, parts in cur}
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
    except ApiError as e:
        return dict(error=e.message), 400
