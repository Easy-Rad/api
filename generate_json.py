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
