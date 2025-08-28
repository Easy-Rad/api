import threading
import logging
import slixmpp
import asyncio
import ssl
import enum
import re
from os import environ
from slixmpp.xmlstream import ET
from dataclasses import dataclass
from app import app
from coolify import pool

JID = environ['XMPP_JID']
PASSWORD = environ['XMPP_PASSWORD']
SERVER = environ.get('XMPP_SERVER', 'app-inteleradha-p.healthhub.health.nz')
SERVER_PORT = int(environ.get('XMPP_PORT', '5222'))

class Presence(enum.StrEnum):
    AVAILABLE = "available"
    AWAY = "away"
    BUSY = "busy"
    OFFLINE = "offline"

def presence_from_dict(d: dict[str, dict]) -> Presence:

    try:
        presence = next(iter(d.values()))
        match presence['show']:
            case '':
                return Presence.AVAILABLE
            case 'away':
                return Presence.AWAY
            case 'dnd':
                return Presence.BUSY
            case _:
                return Presence.OFFLINE
    except StopIteration:
        return Presence.OFFLINE

def generate_jid(pacs: str):
    return re.sub(r'([A-Z])', lambda m: '|' + m.group(1).lower(), pacs) + '@cdhb'

def generate_pacs(jid: str):
    return re.sub(r'\|([a-z])', lambda m: m.group(1).upper(), jid.split('@')[0])

@dataclass
class User:
    name: str
    presence: Presence

    def toJSON(self):
        return dict(
            name=self.name,
            presence=self.presence.value,
        )

class XMPP(slixmpp.ClientXMPP):

    def __init__(self, jid, password, jids: set[str]):
        super().__init__(jid, password)
        self.jids = jids
        self.ssl_context.check_hostname = False
        self.ssl_context.verify_mode = ssl.CERT_NONE
        self.enable_direct_tls = False
        self.auto_authorize = False
        self.auto_subscribe = False

        self.users_lock = threading.Lock()
        self.users: dict[str, User] = {}

        self.add_event_handler("session_start", self.start)
        self.add_event_handler("roster_update", self.handle_roster_update)
        self.add_event_handler("changed_status", self.handle_changed_status)
        self.add_event_handler("message", self.message_received)


    async def start(self, event):
        await self.get_roster()
        self.send_presence()

    def handle_changed_status(self, presence):
        jid:str = presence['from'].bare
        pacs = generate_pacs(jid)
        with self.users_lock:
            if pacs in self.users:
                user = self.users[pacs]
                new_presence = presence_from_dict(self.client_roster.presence(jid))
                if user.presence != new_presence:
                    logging.info(f"{user.name}: {user.presence} -> {new_presence}")
                    user.presence = new_presence

    async def handle_roster_update(self, iq):
        valid_jids = [jid.bare for jid in iq['roster']['items'] if jid.bare in self.jids]
        if len(valid_jids) == 0: return
        new_query = self.make_iq_get()
        query = ET.Element('{jabber:iq:roster-dynamic}query')
        new_query.set_payload(query)
        batch = ET.Element('item', attrib={'type': 'batch'})
        query.append(batch)
        for jid in valid_jids:
            item = ET.Element('item', attrib={'jid': jid})
            batch.append(item)
        people = await new_query.send()
        with self.users_lock:
            for person in people.xml.iterfind(".//{jabber:iq:roster-dynamic}item[@jid]"):
                jid = person.attrib['jid']
                user = User(
                    person.findtext("{jabber:iq:roster-dynamic}full-name"),
                    presence_from_dict(self.client_roster.presence(jid)),
                )
                self.users[generate_pacs(jid)] = user

    def message_received(self, msg):
        if msg['type'] == 'chat':
            payload = msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer2}orderContainer') or msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer}orderContainer') or msg.xml.find('{com.intelerad.viewer.im.extensions.phoneRequestAction}phoneRequestAction')
            reply = msg.reply(msg['body'] if payload else f'{msg['body']} yourself!')
            reply['to']=reply['to'].bare
            if payload is not None: reply.set_payload(payload)
            reply.send()

    def reconnect(self, wait: int | float = 2, reason: str = "Reconnecting") -> None:
        logging.info('Scheduled reconnect...')
        super().reconnect(wait, reason)

with pool.connection() as conn:
    with conn.execute("select pacs from users") as cur:
        jids = set(generate_jid(pacs) for (pacs,) in cur.fetchall())

xmpp_client = XMPP(JID, PASSWORD, jids)

@app.get('/xmpp/online')
def get_online():
    return {pacs: user.toJSON() for pacs, user in xmpp_client.users.items() if user.presence != Presence.OFFLINE}

@app.get('/xmpp/all')
def get_all():
    return {pacs: user.toJSON() for pacs, user in xmpp_client.users.items()}

def run_xmpp_client(client: XMPP):
    if client.connect(SERVER, SERVER_PORT):
        client.schedule("Daily reconnect", 60*60*24, client.reconnect, repeat=True)
        asyncio.get_event_loop().run_forever()

    else:
        logging.error("Unable to connect to the XMPP server.")

xmpp_thread = threading.Thread(target=run_xmpp_client, args=(xmpp_client,))
xmpp_thread.daemon = True
xmpp_thread.start()
logging.info("XMPP client thread started.")