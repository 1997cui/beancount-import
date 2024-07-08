import datetime
import socket
from typing import Any, Callable, Dict, List, Optional

import requests
import requests.packages.urllib3.util.connection as urllib3_cn
from beancount.core.data import (EMPTY_SET, Amount, Directive, Meta, Posting,
                                 Transaction)
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D

from beancount_import.source import (AssociatedData, ImportResult, Source,
                                     SourceResults)

from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor

# Constants
MERCURY_API_URL_TEMPLATE = 'https://api.mercury.com/api/v1/account/{account_id}/transactions?limit=500&offset='

def allowed_gai_family(): 
    return socket.AF_INET
urllib3_cn.allowed_gai_family = allowed_gai_family

class MercurySource(Source):
    def __init__(self, log_status: Callable[[str], None], api_key: str, account_id: str, assets_account: str, **kwargs) -> None:
        super().__init__(log_status, **kwargs)
        self.log_status = log_status
        self.api_key = api_key
        self.account_id = account_id
        self.assets_account = assets_account

    @property
    def name(self) -> str:
        return 'mercury'

    def fetch_mercury_transactions(self):
        headers = {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
        transactions = []
        offset = 0
        url = MERCURY_API_URL_TEMPLATE.format(account_id=self.account_id)

        while True:
            response = requests.get(f"{url}{offset}", headers=headers)
            response.raise_for_status()
            data = response.json()
            transactions.extend(data['transactions'])
            if len(data['transactions']) < 500:
                break
            offset += 500

        return transactions

    def create_beancount_transaction(self, txn: Dict[str, Any]) -> Transaction:
        date = datetime.datetime.strptime(txn['date'], '%Y-%m-%d').date()
        amount = D(txn['amount']['value'])
        currency = txn['amount']['currency']
        narration = txn['description']
        payee = txn['merchant'] if txn['merchant'] else 'Unknown'

        meta = {
            'source': 'mercury',
            'mercury_id': txn['id'],
            'date': txn['date']
        }

        postings = [
            Posting(self.assets_account, Amount(amount, currency), None, None, None, None),
            Posting(FIXME_ACCOUNT, Amount(-amount, currency), None, None, None, None)
        ]

        return Transaction(meta, date, FLAG_OKAY, payee, narration, EMPTY_SET, EMPTY_SET, postings)

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        transactions = self.fetch_mercury_transactions()
        beancount_entries = [self.create_beancount_transaction(txn) for txn in transactions]

        for entry in beancount_entries:
            results.add_pending_entry(ImportResult(date=entry.date, entries=[entry], info=None))

        results.add_account(self.assets_account)

    def is_posting_cleared(self, posting: Posting) -> bool:
        return False

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting) -> Dict[str, str]:
        return {}

    def get_associated_data(self, entry: Directive) -> Optional[List[AssociatedData]]:
        return None

def load(spec: Dict[str, Any], log_status: Callable[[str], None]) -> Source:
    return MercurySource(log_status=log_status, **spec)
