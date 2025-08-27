import threading
import logging
import slixmpp
import asyncio
import ssl
import enum
from os import environ
from slixmpp.xmlstream import ET
from dataclasses import dataclass
from app import app

JID = environ['XMPP_JID']
PASSWORD = environ['XMPP_PASSWORD']
SERVER = environ.get('XMPP_SERVER', 'app-inteleradha-p.healthhub.health.nz')
SERVER_PORT = int(environ.get('XMPP_PORT', '5222'))

class Presence(enum.StrEnum):
    AVAILABLE = "available"
    AWAY = "away"
    BUSY = "busy"
    OFFLINE = "offline"

def presence_from_dict(d: dict) -> Presence:

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

@dataclass
class User:
    phone: str
    email: str
    department: str
    first_name: str
    last_name: str
    full_name: str
    title: str
    primary_role_name: str
    job_function: str
    specialties: list[str]
    session_location: str
    session_phone: str
    address: str
    presence: Presence

    def toJSON(self):
        return dict(
            phone=self.phone,
            email=self.email,
            department=self.department,
            first_name=self.first_name,
            last_name=self.last_name,
            full_name=self.full_name,
            title=self.title,
            primary_role_name=self.primary_role_name,
            job_function=self.job_function,
            specialties=self.specialties,
            session_location=self.session_location,
            session_phone=self.session_phone,
            address=self.address,
            presence=self.presence.value,
        )

    def __str__(self):
        return f"User(phone={self.phone}, email={self.email}, department={self.department}, first_name={self.first_name}, last_name={self.last_name}, full_name={self.full_name}, title={self.title}, primary_role_name={self.primary_role_name}, job_function={self.job_function}, specialties={self.specialties}, session_location={self.session_location}, session_phone={self.session_phone}, presence={self.presence})"

class XMPP(slixmpp.ClientXMPP):

    def __init__(self, jid, password):
        super().__init__(jid, password)
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
        new_presence = presence_from_dict(self.client_roster.presence(jid))
        with self.users_lock:
            if jid in self.users:
                user = self.users[jid]
                logging.info(f"{user.full_name}: {user.presence} -> {new_presence}")
                user.presence = new_presence

    async def handle_roster_update(self, iq):
        valid_jids = [jid.bare for jid in iq['roster']['items'] if "Radiologist - CDHB" in iq['roster']['items'][jid]['groups'] or "Registrar Rad - CDHB" in iq['roster']['items'][jid]['groups'] or jid.bare in (
            # jid whitelist
            'lberry@cdhb',
            'dholnn@cdhb',
            'teaenn@cdhb',
        )]
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
                    person.findtext("{jabber:iq:roster-dynamic}phone1"),
                    person.findtext("{jabber:iq:roster-dynamic}email"),
                    person.findtext("{jabber:iq:roster-dynamic}department"),
                    person.findtext("{jabber:iq:roster-dynamic}first-name"),
                    person.findtext("{jabber:iq:roster-dynamic}last-name"),
                    person.findtext("{jabber:iq:roster-dynamic}full-name"),
                    person.findtext("{jabber:iq:roster-dynamic}title"),
                    person.findtext("{jabber:iq:roster-dynamic}primary-role-name"),
                    person.findtext("{jabber:iq:roster-dynamic}job-function"),
                    [n.text for n in person.iterfind(".//{jabber:iq:roster-dynamic}specialty")],
                    person.findtext("{jabber:iq:roster-dynamic}session-physical-location"),
                    person.findtext("{jabber:iq:roster-dynamic}session-phone"),
                    person.findtext("{jabber:iq:roster-dynamic}address"),
                    presence_from_dict(self.client_roster.presence(jid)),
                )
                self.users[jid] = user

    def message_received(self, msg):
        if msg['type'] == 'chat':
            payload = msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer2}orderContainer') or msg.xml.find('{com.intelerad.viewer.im.extensions.orderContainer}orderContainer') or msg.xml.find('{com.intelerad.viewer.im.extensions.phoneRequestAction}phoneRequestAction')
            reply = msg.reply(msg['body'] if payload else f'{msg['body']} yourself!')
            reply['to']=reply['to'].bare
            if payload is not None: reply.set_payload(payload)
            reply.send()

xmpp_client = XMPP(JID, PASSWORD)

@app.get('/xmpp/online')
def get_online():
    return {jid: user.toJSON() for jid, user in xmpp_client.users.items() if user.presence != Presence.OFFLINE}

@app.get('/xmpp/all')
def get_all():
    return {jid: user.toJSON() for jid, user in xmpp_client.users.items()}

def run_xmpp_client(client: XMPP):
    if client.connect(SERVER, SERVER_PORT):
        asyncio.get_event_loop().run_forever()
    else:
        logging.error("Unable to connect to the XMPP server.")

xmpp_thread = threading.Thread(target=run_xmpp_client, args=(xmpp_client,))
xmpp_thread.daemon = True
xmpp_thread.start()
logging.info("XMPP client thread started.")