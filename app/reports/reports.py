from . import app, TZ, HOLIDAYS
from quart import request, render_template
from ..database import comrad_pool
from psycopg.abc import Query, Params
from datetime import date, datetime
import time

@app.get('/reports/')
async def get_reports():
    links: list[tuple[str, str]] = [
        ('FFS reports', 'ffs'),
    ]
    return await render_template('reports/reports.jinja', links=links)

async def get_table(title: str, query: Query, params: Params | None = None):
    start_time = time.perf_counter()
    async with comrad_pool.connection() as conn:
        async with await conn.execute(query, params) as cur:
            headers = [desc.name for desc in cur.description] # pyright: ignore[reportOptionalIterable]
            rows = await cur.fetchall()
    return await render_template(
        'reports/table.jinja',
        title = title,
        headers = headers,
        rows = rows,
        execution_time = time.perf_counter() - start_time,
        params = params,
    )

@app.get('/reports/ffs')
async def get_report_ffs():
    try:
        d = date.fromisoformat(request.args['date'])
    except:
        d = datetime.now(tz=TZ).date()
    holiday = d in HOLIDAYS
    return await get_table('FFS reports', r'''
select
    or_accession_no as "Accession",
    case when or_ex_type = 'OD' then 'XR' else or_ex_type end as "Modality",
    ce_description as "Study description",
    initcap(st_surname) || ', ' || initcap(st_firstnames) as "Reporter",
    ct_dor as "Report timestamp",
    ct_dor - greatest(
        case when extract(isodow from ct_dor) <= 5 and extract(hour from ct_dor) >= 18 then ct_dor::date + '18:00:00'::time else '-infinity' end,
        lag(ct_dor, 1, ct_dor::date + case when extract(isodow from ct_dor) <= 5 and extract(hour from ct_dor) >= 18 then '18:00:00'::time else '06:00:00'::time end) over (partition by ct_staff_serial order by ct_dor)
        ) as "Time to report"
from case_staff
    join staff on ct_staff_serial = st_serial
    join case_event on ct_ce_serial = ce_serial and ct_staff_function = 'R'
    join orders on or_event_serial = ce_serial and or_status != 'X'
    join sel_table as site ON ce_site = site.sl_key AND site.sl_code = 'SIT'
where ct_dor between %(date)s + '06:00:00'::time and %(date)s + '23:00:00'::time
    and (extract(hour from ct_dor) not between 8 and 17 or extract(isodow from ct_dor) > 5 or %(holiday)s)
    and or_ex_type IN ('CT', 'MR', 'US', 'XR', 'OD')
    and ct_staff_serial not in (3725, 8057, 7870, 6692)
    and site.sl_aux1 = 'CDHB'
    and ce_site NOT IN ('HAN', 'KAIK', 'CARD')
order by st_surname, ct_dor''', dict(date=d, holiday=holiday))