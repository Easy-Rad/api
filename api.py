import atexit
from os import environ

from flask import Flask
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DB_HOST = environ.get('DB_HOST', '159.117.39.229')
DB_NAME = environ.get('DB_NAME', 'prod_cdhb')
DB_USER = environ['DB_USER']
DB_PASSWORD = environ['DB_PASSWORD']

pool = ConnectionPool(
    open=True,
    kwargs=dict(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )
)
atexit.register(pool.close)

app = Flask(__name__)

dashboard_query = """
select
    rf_site site,
    rf_pat_type pat_type,
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
    end urgency,
    case rf_triage_status
        when 'I' then 'In progress'
        when 'C' then 'Complete'
        end
        as triage_status,
    -- rf_triage_team as team,
    pa_nhi::text as nhi,
    pa_surname::text,
    pa_firstname::text,
    rf_pat_location::text as location,
    rf_reason as description,
    extract(epoch from rf_dor at time zone 'Pacific/Auckland') as received,
    coalesce((select array_agg(trim(p)) from
        unnest(xpath('//p/text()', xmlparse(document mit.te_text))::text[]) as p
        where trim(p) != ''), array[]::text[]) as mit_notes,
    coalesce((select array_agg(trim(p)) from
        unnest(xpath('//p/text()', xmlparse(document doc.te_text))::text[]) as p
        where trim(p) != ''), array[]::text[]) as doc_notes
from case_referral
join patient on rf_pno=pa_pno
left join notes mit_notes on rf_serial=mit_notes.no_key and mit_notes.no_type='F' and mit_notes.no_category='Q' and mit_notes.no_sub_category='M' and mit_notes.no_sub_category='M' and mit_notes.no_status='A'
left join notes doc_notes on rf_serial=doc_notes.no_key and doc_notes.no_type='F' and doc_notes.no_category='Q' and doc_notes.no_sub_category='G' and doc_notes.no_status='A'
left join doctext mit on mit_notes.no_serial=mit.te_key and xml_is_well_formed_document(mit.te_text)
left join doctext doc on doc_notes.no_serial=doc.te_key and xml_is_well_formed_document(doc.te_text)
where (rf_new_rf_serial=0 or rf_new_rf_serial is null)
    and rf_status='W'
    and rf_site in ('CDHB','EMER')
    and rf_pat_type in ('INP','ED')
    and rf_exam_type=%s
order by rf_dor desc
limit 100
"""

@app.route('/dashboard/<modality>', methods=['GET'])
def get_dashboard(modality: str): #e.g. CT, MR
    with pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(dashboard_query, [modality], prepare=True)
            return cur.fetchall()