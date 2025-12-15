import os

import pytest

pytest.importorskip('beancount')

from ..source import SourceResults
import beancount_import.source.boa as boa
from beancount_import.journal_editor import JournalEditor


def test_boa_source_dedup(tmp_path):
    # Use realistic BOA CSV headers (Posted Date, Reference Number, Payee, Address, Amount)
    csv_content = """Posted Date,Reference Number,Payee,Address,Amount
12/13/2025,24801975347580272595791,"TOUS LES JOURS REDMOND REDMOND WA","REDMOND       WA ",-4.25
12/13/2025,24692165347106055830641,"AMAZON MKTPL*BY1WW5N63 Amzn.com/billWA","Amzn.com/bill WA ",-18.74
12/13/2025,24801975347580272595791,"TOUS LES JOURS REDMOND REDMOND WA","REDMOND       WA ",-4.25
"""

    csv_file = tmp_path / 'boa_test.csv'
    csv_file.write_text(csv_content)

    journal_file = tmp_path / 'journal.beancount'
    journal_file.write_text('2025-01-01 open Assets:Checking\n')

    spec = {
        'transaction_csv_filenames': [str(csv_file)],
        'account': 'Liabilities:Credit-Card:BOA:0000'
    }

    editor = JournalEditor(str(journal_file))
    assert editor.errors == []

    source = boa.load(spec, log_status=lambda s: None)
    results = SourceResults()
    source.prepare(editor, results)

    assert 'Liabilities:Credit-Card:BOA:0000' in results.accounts
    # Duplicate reference number should have been deduped, expecting 2 unique refs
    assert len(results.pending) == 2

    refs = {r.entries[0].meta.get('reference') for r in results.pending}
    assert refs == {'24801975347580272595791', '24692165347106055830641'}

    # Check amounts preserved sign: find the AMAZON entry and assert amount is -18.74
    amounts = {r.entries[0].postings[0].units.number for r in results.pending}
    assert -18.74 in [float(a) for a in amounts]
