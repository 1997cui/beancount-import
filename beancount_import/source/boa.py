"""Bank of America CSV transaction source for beancount-import.

This source reads BOA CSV files (like `currentTransaction_####.csv`) and
produces ImportResult entries. It uses the CSV `Reference` column to
deduplicate transactions (skips duplicates with the same reference within
the provided file list). It also exposes the `reference` metadata on the
generated transaction for cross-checking.
"""
from __future__ import annotations

import csv
import os
from typing import Sequence, List

from beancount_import.source import Source, ImportResult, SourceResults
from beancount_import.source import SourceSpec
from beancount.core.data import Transaction, Posting, Meta
from beancount.core import flags
from beancount.core.amount import Amount
from beancount.core.number import D

from dateutil.parser import parse


def load(spec: SourceSpec, log_status):
    return BOASource(log_status=log_status, **spec)


class BOASource(Source):
    def __init__(self, log_status, transaction_csv_filenames: Sequence[str], account: str, **kwargs):
        super().__init__(log_status=log_status, **kwargs)
        self.transaction_csv_filenames = list(transaction_csv_filenames)
        self.account = account

    @property
    def name(self) -> str:
        return 'boa'

    def prepare(self, journal, results: SourceResults) -> None:
        results.add_account(self.account)

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

                    raw_date = row.get(date_key, '').strip()
                    try:
                        trans_date = parse(raw_date).date()
                    except Exception:
                        continue

                    reference = row.get(ref_key, '').strip() if ref_key else ''
                    if reference:
                        if reference in seen_references:
                            continue
                        seen_references.add(reference)

                    desc = row.get(desc_key, '').strip() if desc_key else ''

                    meta: Meta = {'filename': csv_filename, 'lineno': idx}
                    if reference:
                        meta['reference'] = reference

                    txn = Transaction(
                        meta=meta,
                        date=trans_date,
                        flag=flags.FLAG_OKAY,
                        payee='',
                        narration=desc,
                        tags=set(),
                        links=set(),
                        postings=[],
                    )
                    txn.postings.append(
                        Posting(self.account, Amount(number=trans_amt, currency='USD'), None, None, None,
                                {'date': trans_date, 'reference': reference} )
                    )

                    results.add_pending_entry(ImportResult(date=txn.date, entries=[txn], info={'filename': csv_filename}))

    def is_posting_cleared(self, posting):
        # BOA CSVs only indicate transaction date; no extra clearing info here
        return False
