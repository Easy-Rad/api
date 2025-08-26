import json
import psycopg
from os import environ
from itertools import groupby


with psycopg.connect(environ['AUTOTRIAGE_CONN']) as connection:
    with connection.cursor() as cursor:
        # with open('data/exams.json', 'w') as f:
        #     cursor.execute('''select code, name, modality, body_part from examination order by modality, code''')
        #     json.dump(
        #         {modality: {code: [body_part, name] for code, name, _, body_part in exams} for modality, exams in groupby(cursor.fetchall(), lambda x: x[2])},
        #         fp=f,
        #         indent=2,
        #     )
        # with open('examination.json', 'r') as f:
        #     cursor.executemany(
        #         "INSERT INTO examination (code, name, modality, body_part, topic) VALUES (%s, %s, %s, %s, %s)",
        #     [(
        #         code,
        #         data['name'],
        #         modality,
        #         data['bodyPart'],
        #         data['topic'] if 'topic' in data else None,
        #     ) for modality, exam in json.load(f).items() for code, data in exam.items()])
        # with open('data/labels.json', 'w') as f:
        #     cursor.execute('''select tokenised, modality, code from label where username is null order by modality, tokenised''')
        #     json.dump(
        #         {modality: {tokenised: code for tokenised, _, code in labels} for modality, labels in groupby(cursor.fetchall(), lambda x: x[1])},
        #         fp=f,
        #         indent=2,
        #     )
        # with open('labels.json', 'r') as f:
        #     cursor.executemany(
        #         "INSERT INTO label (tokenised, modality, code) VALUES (%s, %s, %s)",
        #     [(
        #         tokenised,
        #         modality,
        #         code,
        #     ) for modality, labels in json.load(f).items() for tokenised, code in labels.items()])
        # with open('data/body_parts.json', 'r') as f:
        #     cursor.executemany(
        #         "INSERT INTO ffs_body_parts (name, parts) VALUES (%s, %s)",
        #     [(
        #         name.lower(),
        #         parts,
        #     ) for name, parts in json.load(f).items()])
        # with open('data/desks.json', 'r') as f:
        #     cursor.executemany(
        #         "INSERT INTO desks (ip, name, area, phone) VALUES (%s, %s, %s, %s)",
        #     [(
        #         desk['Title'],
        #         desk['DeskId'],
        #         desk['DeskName'],
        #         desk['DeskPhone'],
        #     ) for desk in json.load(f)])
        # with open('.vscode/users.json', 'r') as f:
        #     cursor.executemany(
        #         "update users set sso = %s where (first_name = %s and last_name = %s)",
        #     [(
        #         user['sso'],
        #         user['first_name'],
        #         user['last_name'],
        #     ) for user in json.load(f)])
        # with open('.vscode/terminals.json', 'r') as f:
        #     cursor.executemany(
        #         "update desks set terminal = %s where computer_name = %s",
        #     [(
        #         terminal['terminal'],
        #         terminal['computer'],
        #     ) for terminal in json.load(f)])
        with open('.vscode/xmpp_users.json', 'r') as f:
            cursor.executemany(
                "update users set xmpp_jid = %s where first_name = %s and last_name = %s",
            [(  jid,
                user['first_name'],
                user['last_name'],
            ) for jid, user in json.load(f).items()])
