import logging
from typing import LiteralString
from quart import request
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from . import app
from ..database import local_pool


update_users: LiteralString = r"""
update users
set windows_logons = coalesce(value, '{}'::jsonb)
from input_data_temp, jsonb_each(data -> 'users')
right join users as u on u.sso ilike key
where users.ris = u.ris
"""

update_desks: LiteralString = r"""
update desks
set online = coalesce(value::boolean, false)
from input_data_temp, jsonb_each(data -> 'online')
right join desks as d on d.computer_name = key
where desks.computer_name = d.computer_name
"""

@app.post('/desks')
async def post_desks():
    r = await request.get_json(force=True)
    async with local_pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(r"""create temp table input_data_temp (data jsonb) on commit drop""")
            await cur.execute(r"""insert into input_data_temp (data) values (%s)""", (Jsonb(r),))
            await cur.execute(update_users)
            await cur.execute(update_desks)
            logging.info(f'Users online: {len(r["users"])}')
            online_computers = [computer_name for computer_name, online in r["online"].items() if online ]
            logging.info(f'Computers online: {len(online_computers)} of {len(r["online"])}')
            return ('',204)

@app.get('/desks')
async def get_desks():
    async with local_pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(r"""select name, computer_name, phone from desks order by sort_order""", prepare=True)
            return await cur.fetchall()