from typing import LiteralString
from psycopg.rows import dict_row
from . import app
from ..database import comrad_pool

dashboard_query: LiteralString = r"""
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
async def get_dashboard(modality: str):
    async with comrad_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("set local enable_sort = off")
            await cur.execute("set local jit_above_cost = -1")
            await cur.execute(dashboard_query, [modality], prepare=True)
            return await cur.fetchall()

