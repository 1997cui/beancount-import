"""Bao credit-card CSV transaction source for beancount-import.

This source reads Bank-of-America credit card CSV files (like
`currentTransaction_####.csv`) and produces ImportResult entries. It uses the
CSV `Reference` column to deduplicate transactions (skips duplicates with the
same reference within the provided file list and against the existing journal
for the configured account). It also exposes the `reference` metadata on the
generated BOA posting for cross-checking.
"""
from __future__ import annotations

import csv
import os
from typing import Sequence, List, Dict

from beancount_import.source import Source, ImportResult, SourceResults
from beancount_import.source import SourceSpec
from beancount.core.data import Transaction, Posting, Meta
from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.number import D
from ..matching import FIXME_ACCOUNT

from dateutil.parser import parse


def load(spec: SourceSpec, log_status):
    return BaoCreditCardSource(log_status=log_status, **spec)


class BaoCreditCardSource(Source):
    def __init__(self, log_status, transaction_csv_filenames: Sequence[str], account: str, **kwargs):
        super().__init__(log_status=log_status, **kwargs)
        self.transaction_csv_filenames = list(transaction_csv_filenames)
        self.account = account

    @property
    def name(self) -> str:
        return 'bao_creditcard'

    def prepare(self, journal, results: SourceResults) -> None:
        results.add_account(self.account)

        # Gather references already present in the journal so we don't import
        # duplicates that have been previously recorded.
        existing_references = set()
        for entry in getattr(journal, 'all_entries', []):
            from beancount.core.data import Transaction as _Txn
            if not isinstance(entry, _Txn):
                continue
            # Collect references only for transactions that involve our
            # configured source account (avoid cross-account collisions).
            infl_account_present = False
            for p in entry.postings:
                if p.account == self.account:
                    infl_account_present = True
                    meta = p.meta
                    if meta is None:
                        continue
                    ref = meta.get('reference')
                    if ref:
                        existing_references.add(ref)
            # If the transaction itself has a reference and the source
            # account is present, include it as well.
            if infl_account_present and entry.meta is not None and entry.meta.get('reference'):
                existing_references.add(entry.meta.get('reference'))

        seen_references = set()  # dedupe across files processed by this source

        for csv_filename in self.transaction_csv_filenames:
            if not os.path.isfile(csv_filename):
                continue
            with open(csv_filename, newline='') as fh:
                reader = csv.DictReader(fh)
                # adapt to a few likely column names (match BOA export)
                def find_key(row, candidates):
                    for c in candidates:
                        if c in row:
                            return c
                    return None

                for idx, row in enumerate(reader):
                    date_key = find_key(row, ['Posted Date', 'Date', 'Transaction Date', 'Posting Date'])
                    amt_key = find_key(row, ['Amount', 'Transaction Amount', 'Amount (USD)'])
                    desc_key = find_key(row, ['Payee', 'Description', 'Memo'])
                    ref_key = find_key(row, ['Reference Number', 'Reference', 'Reference #', 'Ref #'])

                    if not date_key or not amt_key:
                        continue

                    raw_amount = row.get(amt_key, '').strip()
                    if not raw_amount:
                        continue
                    cleaned = raw_amount.replace('$', '').replace(',', '')
                    is_paren = False
                    if cleaned.startswith('(') and cleaned.endswith(')'):
                        is_paren = True
                        cleaned = cleaned[1:-1]
                    try:
                        amt = D(cleaned)
                    except Exception:
                        continue
                    if is_paren:
                        amt = -amt

                    # BOA CSV uses negative amounts for charges and positive for
                    # credits. Use the sign as provided in the CSV (do not invert).
                    trans_amt = amt
                    # Ensure amounts are quantized to two decimal places so
                    # postings display consistently like "18.74" rather than
                    # "18.740000" or "18.7".
                    try:
                        trans_amt = trans_amt.quantize(D('0.01'))
                    except Exception:
                        # If quantization fails for any unusual decimal, fall
                        # back to the unquantized value.
                        pass

                    raw_date = row.get(date_key, '').strip()
                    try:
                        trans_date = parse(raw_date).date()
                    except Exception:
                        continue

                    reference = row.get(ref_key, '').strip() if ref_key else ''
                    if reference:
                        # Skip if we already processed this reference in the
                        # current run, or if the reference exists in the
                        # journal already.
                        if reference in seen_references:
                            continue
                        if reference in existing_references:
                            # Silently skip duplicates already present in
                            # the journal.
                            continue
                        seen_references.add(reference)

                    desc = row.get(desc_key, '').strip() if desc_key else ''

                    # Only attach metadata to the BOA leg (the source posting).
                    posting_meta: Meta = {'date': trans_date, 'reference': reference, 'source_desc': desc, 'filename': csv_filename, 'lineno': idx}

                    txn = Transaction(
                        meta={},
                        date=trans_date,
                        flag=flags.FLAG_OKAY,
                        payee=desc,
                        narration='',
                        tags=set(),
                        links=set(),
                        postings=[],
                    )

                    # BOA posting (source account)
                    txn.postings.append(
                        Posting(self.account, Amount(number=trans_amt, currency='USD'), None, None, None, posting_meta)
                    )

                    # Add balancing posting to FIXME account (unknown other leg)
                    txn.postings.append(
                        Posting(FIXME_ACCOUNT, Amount(number=(-trans_amt).quantize(D('0.01')), currency='USD'), None, None, None, {})
                    )

                    results.add_pending_entry(ImportResult(date=txn.date, entries=[txn], info={'filename': csv_filename}))

    def is_posting_cleared(self, posting):
        # BOA CSVs only indicate transaction date; no extra clearing info here
        return False

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting) -> Dict[str, str]:
        """Extract training features for auto-categorization.

        This enables the training system to match BOA transactions against:
        - Manually entered transactions with payee fields
        - Previously categorized BOA transactions
        - Transactions from other sources with similar descriptions
        """
        result = dict()
        # Extract transaction-level fields
        result['desc'] = transaction.narration if transaction.narration else ''
        result['payee'] = transaction.payee if transaction.payee else ''
        # Extract posting metadata (merchant/payee description from CSV)
        if posting.meta:
            source_desc = posting.meta.get('source_desc', '')
            if source_desc:
                result['source_desc'] = source_desc
        return result
