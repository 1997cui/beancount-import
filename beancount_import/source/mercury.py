import datetime
import socket
import decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests
import requests.packages.urllib3.util.connection as urllib3_cn
from beancount.core.data import (EMPTY_SET, Amount, Directive, Meta, Posting,
                                 Transaction, Account, Entries, Open)
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import D

from beancount_import.source import (AssociatedData, ImportResult, Source,
                                     SourceResults)

from ..matching import FIXME_ACCOUNT
from ..journal_editor import JournalEditor
from datetime import datetime
import collections
import urllib.parse

"""
Usage:
In the account you want to import, add meta `mercury_id` for account id
In the import module, use `api_key` to pass through Mercury API key
API doc is here: https://docs.mercury.com/reference/welcome-to-mercury-api

"""

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
                data = response.json(parse_float=decimal.Decimal)
                for txn in data['transactions']:
                    if 'status' not in txn or txn['status'] != 'sent':
                        continue
                    transactions[account].append(txn)
                if len(data['transactions']) < 500:
                    break
                offset += 500
        return transactions
    
def get_account_map(accounts: List[Open]) -> Tuple[Dict[Account, str], Dict[str, Account]]:
    account_to_id = dict()
    id_to_account = dict()
    for entry in accounts.values():
        meta = entry.meta
        if meta is None:
            continue
        account_id = entry.meta.get('mercury_id')
        if account_id is None:
            continue
        account_to_id[entry.account] = account_id
        id_to_account[account_id] = entry.account
    return account_to_id, id_to_account

    

class MercurySource(Source):
    def __init__(self, log_status: Callable[[str], None], api_key: str, **kwargs) -> None:
        super().__init__(log_status, **kwargs)
        self.log_status = log_status
        self.api_key = api_key
        self.mercury_api = MercuryAPI(api_key)
        self.downloaded_txns = self.mercury_api.fetch_mercury_transactions()

    @property
    def name(self) -> str:
        return 'mercury'

   

    def create_beancount_transaction(self, txn: Dict[str, Any], mercury_account: Account) -> Transaction:
        amount = D(txn['amount'])
        narration = txn['bankDescription']
        payee = txn['counterpartyNickname'] if txn['counterpartyNickname'] else txn['counterpartyName']
        date = datetime.fromisoformat(txn['postedAt'].replace("Z", "+00:00"))
        meta = collections.OrderedDict([
            ('mercury_id', txn['id']),
            ('date', date.date()),
            ('kind', txn['kind']),
            ('counterpartyName', txn['counterpartyName'])
            ])

        postings = [
            Posting(mercury_account, Amount(amount, 'USD'), None, None, None, meta),
            Posting(FIXME_ACCOUNT, Amount(-amount, 'USD'), None, None, None, None)
        ]

        return Transaction(None, date.date(), FLAG_OKAY, payee, narration, EMPTY_SET, EMPTY_SET, postings)

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:
        self.account_to_id, self.id_to_account = get_account_map(journal.accounts)
        # Dedup: Record all seen posing ids in exising transactions:
        seen_txn_ids = set()
        for entry in journal.all_entries:
            if not isinstance(entry, Transaction):
                continue
            last_lineno = None
            for posting in entry.postings:
                meta = posting.meta
                if meta is None: continue
                # Skip duplicated postings due to booking.
                new_lineno = meta['lineno']
                if new_lineno is not None and new_lineno == last_lineno:
                    continue
                last_lineno = new_lineno
                txn_id = meta.get('mercury_id', None)
                if txn_id is None:
                    continue
                seen_txn_ids.add(txn_id)
        for account_id, transactions in self.downloaded_txns.items():
            if account_id not in self.id_to_account:
                continue
            account: Account = self.id_to_account[account_id]
            for txn in transactions:
                if txn['id'] and txn['id'] in seen_txn_ids:
                    continue
                entry = self.create_beancount_transaction(txn, account)
                results.add_pending_entry(ImportResult(date=entry.date, entries=[entry], info=None))

            results.add_account(account)

    def is_posting_cleared(self, posting: Posting) -> bool:
        if not posting.meta:
            return False
        if 'mercury_id' in posting.meta:
            return True

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting) -> Dict[str, str]:
        result = dict()
        result['desc'] = transaction.narration
        result['payee'] = transaction.payee if transaction.payee else ''
        if 'kind' in posting.meta:
            result['kind'] = posting.meta['kind']
        if 'counterpartyName' in posting.meta:
            result['counterpartyName'] = posting.meta['counterpartyName']
        return result

def load(spec: Dict[str, Any], log_status: Callable[[str], None]) -> Source:
    return MercurySource(log_status=log_status, **spec)
