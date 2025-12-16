import os

import pytest

pytest.importorskip('beancount')

from ..source import SourceResults
import beancount_import.source.bao_creditcard as bao
from beancount_import.journal_editor import JournalEditor
from beancount.core.number import D


def test_bao_creditcard_source_dedup(tmp_path):
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

    source = bao.load(spec, log_status=lambda s: None)
    results = SourceResults()
    source.prepare(editor, results)

    assert 'Liabilities:Credit-Card:BOA:0000' in results.accounts
    # Duplicate reference number should have been deduped, expecting 2 unique refs
    assert len(results.pending) == 2

    refs = set()
    for r in results.pending:
        txn = r.entries[0]
        assert len(txn.postings) == 2
        # Transaction-level metadata should not contain reference keys
        assert not txn.meta or txn.meta == {}
        # BOA posting should have metadata with reference
        boa_posting = next(p for p in txn.postings if p.account == 'Liabilities:Credit-Card:BOA:0000')
        assert 'reference' in boa_posting.meta and boa_posting.meta['reference']
        assert boa_posting.meta.get('filename') == str(csv_file)
        assert boa_posting.meta.get('lineno') is not None
        refs.add(boa_posting.meta['reference'])

        # balancing posting should be to FIXME_ACCOUNT
        bal_posting = next(p for p in txn.postings if p.account != 'Liabilities:Credit-Card:BOA:0000')
        assert bal_posting.account == 'Expenses:FIXME'
        # amounts should balance
        assert float(boa_posting.units.number) == -float(bal_posting.units.number)
        # amounts should be quantized to two decimal places
        assert boa_posting.units.number == boa_posting.units.number.quantize(D('0.01'))
        assert bal_posting.units.number == bal_posting.units.number.quantize(D('0.01'))

    assert refs == {'24801975347580272595791', '24692165347106055830641'}


def test_bao_creditcard_source_skips_existing_journal_reference(tmp_path):
    csv_content = """Posted Date,Reference Number,Payee,Address,Amount
01/01/2025,REF-A,"COCOTREECHEF","WA",-18.74
01/02/2025,REF-B,"DOUGH SAVOR","WA",-12.11
"""

    csv_file = tmp_path / 'boa_test2.csv'
    csv_file.write_text(csv_content)

    # Journal already contains an entry with reference REF-A
    journal_file = tmp_path / 'journal2.beancount'
    journal_file.write_text(
        '2025-01-01 open Liabilities:Credit-Card:BOA:0000\n'
        '2025-01-01 open Expenses:FIXME\n'
        '\n'
        '2025-01-01 * "COCOTREECHEF"\n'
        '  Liabilities:Credit-Card:BOA:0000  -18.74 USD\n'
        '    date: 2025-01-01\n'
        '    reference: "REF-A"\n'
        '  Expenses:FIXME 18.74 USD\n'
    )

    spec = {
        'transaction_csv_filenames': [str(csv_file)],
        'account': 'Liabilities:Credit-Card:BOA:0000'
    }

    editor = JournalEditor(str(journal_file))
    assert editor.errors == []

    source = bao.load(spec, log_status=lambda s: None)
    results = SourceResults()
    source.prepare(editor, results)

    # Only the REF-B row should be pending
    assert len(results.pending) == 1
    boa_posting = next(p for p in results.pending[0].entries[0].postings if p.account == 'Liabilities:Credit-Card:BOA:0000')
    assert boa_posting.meta['reference'] == 'REF-B'
    # amount should be quantized to two decimals
    assert boa_posting.units.number == boa_posting.units.number.quantize(D('0.01'))
