import logging
from typing import AsyncGenerator
import httpx
from os import environ
from datetime import datetime
from enum import StrEnum
from dataclasses import dataclass
from zeep import AsyncClient, Plugin
from zeep.cache import SqliteCache
from zeep.transports import AsyncTransport
from zeep.ns import SOAP_ENV_12
from lxml import etree # type: ignore

HOST = environ['PS360_HOST']
USERNAME = environ['PS360_USER']
PASSWORD = environ['PS360_PASSWORD']
TIME_ZONE_ID = 'New Zealand Standard Time'
LOCALE = 'en-NZ'
PS_VERSION = '7.0.212.0'
SITE_ID = 0
PS_PAGE_SIZE = 3000

class EventType(StrEnum):
    SIGN = 'Sign'
    EDIT = 'Edit'
    QUEUE_FOR_SIGNATURE = 'QueueForSignature'
    OVERREAD = 'Overread'

@dataclass
class UserLastEvent():
    event_type: EventType
    timestamp: datetime
    workstation: str
    additional_info: str

@dataclass
class User():
    id: int
    name: str
    last_event: UserLastEvent

class PS360:

    _account_session: etree.Element | None

    def __init__(self):
        self._account_session = None
        self._transport = AsyncTransport(
            cache=SqliteCache(timeout=None), # type: ignore
            wsdl_client=httpx.Client(timeout=None),
            client=httpx.AsyncClient(timeout=None),
            )
        self.session_client = AsyncClient(f'http://{HOST}/RAS/Session.svc?wsdl', transport=self._transport, plugins=[SaveAccountSessionPlugin(self)])
        self.explorer_client = AsyncClient(f'http://{HOST}/RAS/Explorer.svc?wsdl', transport=self._transport)
        self.report_client = AsyncClient(f'http://{HOST}/RAS/Report.svc?wsdl', transport=self._transport)

    async def __aenter__(self):
        await self.session_client.service.SignIn(
            loginName=USERNAME,
            password=PASSWORD,
            adminMode=False,
            version=PS_VERSION,
            workstation='',
            locale=LOCALE,
            timeZoneId=TIME_ZONE_ID,
        )
        assert self._account_session is not None
        logging.info(f'PS360 signed in: session ID {self._account_session.text}')
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._account_session is not None:
            sessionId = self._account_session.text
            if await self.session_client.service.SignOut(_soapheaders=[self._account_session]):
                self._account_session = None
                logging.info(f'PS360 signed out: session ID {sessionId}')
        await self._transport.aclose()

    async def get_overread(self, reportId: int) -> datetime | None:
        reportDetail = await self.report_client.service.GetReport(
            reportID=reportId,
            fetchAudio=False,
            fetchImages=False,
            fetchNotes=False,
            fetchAttachments=False,
            _soapheaders=[self._account_session],
        )
        return reportDetail.LastPrelimDate if reportDetail.Overread else None

    async def orders(self, accountID: int, _from: datetime, _to: datetime) -> AsyncGenerator[tuple[int, str]]:
        page_number = 1
        while True:
            response = await self.explorer_client.service.BrowseOrders(
                siteID=SITE_ID,
                time=dict(
                    Period='Custom',
                    From=_from.isoformat(timespec='milliseconds'),
                    To=_to.isoformat(timespec='milliseconds'),
                ),
                orderStatus='Completed',
                transferStatus='Sent',
                reportStatus='Reported',
                accountID=accountID,
                sort='LastModifiedDate ASC',
                pageSize=PS_PAGE_SIZE,
                pageNumber=page_number,
                _soapheaders=[self._account_session],
            ) or []
            for report in response:
                yield report.ReportID, report.Accession
            if len(response) < PS_PAGE_SIZE:
                logging.info(f'Finished scraping PS360. Pages fetched: {page_number}, reports: {PS_PAGE_SIZE * (page_number-1) + len(response)}')
                return
            page_number += 1

class SaveAccountSessionPlugin(Plugin):
    def __init__(self, ps : PS360):
        self.ps = ps

    def ingress(self, envelope, http_headers, operation):
        self.ps._account_session = envelope.find('./s:Header/AccountSession', {'s': SOAP_ENV_12})
        return envelope, http_headers

    def egress(self, envelope, http_headers, operation, binding_options):
        return envelope, http_headers
