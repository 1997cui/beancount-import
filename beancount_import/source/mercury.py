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
from datetime import datetime
import collections
import urllib.parse

# Constants

def allowed_gai_family(): 
    return socket.AF_INET
urllib3_cn.allowed_gai_family = allowed_gai_family

class MercuryAPI():
    MERCURY_API_BASE="https://api.mercury.com/api/v1/"
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.accounts: List[str] = list()
        self.fetch_accounts()

    def _get_headers(self) -> Dict:
        return {
            'Authorization': f'Bearer {self.api_key}',
            'Content-Type': 'application/json'
        }
    
    def fetch_accounts(self):
        response = requests.get(urllib.parse.urljoin(
            self.MERCURY_API_BASE, 'accounts'), headers=self._get_headers())
        response.raise_for_status()
        data = response.json()
        accounts_data = data['accounts'] if 'accounts' in data else []
        for account in accounts_data:
            self.accounts.append(account['id'])

    def fetch_mercury_transactions(self) -> Dict:
        MERCURY_API_TXN = 'account/{account_id}/transactions?limit=500&offset={offset}'
        'https://api.mercury.com/api/v1/account/{account_id}/transactions?limit=500&offset={offset}'
        transactions: Dict[str, List] = {}
        for account in self.accounts:
            transactions[account] = []
            offset = 0
            partial = MERCURY_API_TXN.format(account_id=account, offset=offset)
            full_url = urllib.parse.urljoin(self.MERCURY_API_BASE, partial)
            while True:
                response = requests.get(f"{full_url}{offset}", headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                for txn in data['transactions']:
                    transactions[account].append(txn)
                if len(data['transactions']) < 500:
                    break
                offset += 500
        return transactions
    

class MercurySource(Source):
    def __init__(self, log_status: Callable[[str], None], api_key: str, account_id: str, assets_account: str, **kwargs) -> None:
        super().__init__(log_status, **kwargs)
        self.log_status = log_status
        self.api_key = api_key
        self.account_id = account_id
        self.assets_account = assets_account
        self.mercury_api = MercuryAPI(api_key)
        self.downloaded_txns = self.mercury_api.fetch_mercury_transactions()

    @property
    def name(self) -> str:
        return 'mercury'

   

    def create_beancount_transaction(self, txn: Dict[str, Any]) -> Transaction:
        amount = D(txn['amount'])
        narration = txn['bankDescription']
        payee = txn['counterpartyNickname'] if txn['counterpartyNickname'] else txn['counterpartyName']
        date = datetime.fromisoformat(txn['createdAt'].replace("Z", "+00:00"))
        meta = collections.OrderedDict([
            ('mercury_id', txn['id']),
            ('date', date.date()),
            ('kind', txn['kind']),
            ('counterpartyName', txn['counterpartyName'])
            ])

        postings = [
            Posting(self.assets_account, Amount(amount, 'USD'), None, None, None, meta),
            Posting(FIXME_ACCOUNT, Amount(-amount, 'USD'), None, None, None, None)
        ]

        return Transaction(None, date.date(), FLAG_OKAY, payee, narration, EMPTY_SET, EMPTY_SET, postings)

    def prepare(self, journal: 'JournalEditor', results: SourceResults) -> None:
        for account, transactions in self.downloaded_txns.items():
            if account != self.account_id:
                continue
            print(transactions)
            beancount_entries = [self.create_beancount_transaction(txn) for txn in transactions]

            for entry in beancount_entries:
                results.add_pending_entry(ImportResult(date=entry.date, entries=[entry], info=None))

            results.add_account(self.assets_account)

    def is_posting_cleared(self, posting: Posting) -> bool:
        if not posting.meta:
            return False
        if 'mercury_id' in posting.meta:
            return True

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting) -> Dict[str, str]:
        result = dict()
        result['desc'] = transaction.narration
        result['payee'] = transaction.payee
        if 'kind' in posting.meta:
            result['kind'] = posting.meta['kind']
        if 'counterpartyName' in posting.meta:
            result['counterpartyName'] = posting.meta['counterpartyName']
        return result

def load(spec: Dict[str, Any], log_status: Callable[[str], None]) -> Source:
    return MercurySource(log_status=log_status, **spec)
