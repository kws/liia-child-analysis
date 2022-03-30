from io import StringIO

from lxml.etree import ElementTree

from sfdata_cincensus_clean.cin_record import message_collector, CINEvent, event_to_records
from sfdata_cincensus_clean.config import Config
from sfdata_cincensus_clean.filters import strip_text, add_context, add_config, clean
from sfdata_stream_parser.checks import type_check
from sfdata_stream_parser.filters.generic import streamfilter, filter_stream
from sfdata_stream_parser.parser.xml import parse_file
from wrangling.cincensus.main import degradefile, cleanfile, build_cinrecord


def test_parse(fixtures_path):
    degraded_tree: ElementTree = degradefile(fixtures_path / "sample.xml")
    root = degraded_tree.getroot()

    assert root.tag == "Message"


def test_flatfile(fixtures_path, config):
    tree = degradefile(fixtures_path / "sample.xml")
    cleaned_tree = cleanfile(tree, config)

    flatfile = build_cinrecord([cleaned_tree])

    outfile = StringIO()
    flatfile.to_csv(outfile, index=False)

    with open(fixtures_path / "sample.csv") as f:
        sample_file = f.read()

    assert outfile.getvalue() == sample_file


def test_stream_record(fixtures_path):
    stream = parse_file(fixtures_path / 'sample.xml')
    stream = strip_text(stream)
    stream = add_context(stream)
    stream = add_config(stream, Config().fields_with_prefix(['Message', 'Children', 'Child']))
    stream = clean(stream)
    stream = message_collector(stream)
    records = list(stream)
    assert len(records) == 2

    print(records[1].as_dict())

    for ix, row in enumerate(event_to_records(records[1])):
        print(ix, row)


@streamfilter
def clean_test(event):
    print("Cleaning", event)
    return event.from_event(event, cleaned_value=f'{event.text} cleaned')
