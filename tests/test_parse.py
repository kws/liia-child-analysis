from io import StringIO

from lxml.etree import ElementTree

from sfdata_cincensus_clean.config import Config
from sfdata_cincensus_clean.filters import strip_text, add_context, add_config, clean
from sfdata_stream_parser import events
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


def to_table(stream):
    container_started = event = False
    child_dict = None
    for event in stream:
        if isinstance(event, events.StartElement) and event.tag == 'Child':
            if not container_started:
                container_started = True
                yield events.StartTable.from_event(event)
            child_dict = {}
            yield events.StartRow.from_event(event)

        elif isinstance(event, events.EndElement) and event.tag == 'Child':
            for key, value in child_dict.items():
                yield events.Cell(column=key, value=value)
            child_dict = None

        else:
            if child_dict is not None and isinstance(event, events.StartElement) and event.text != '':
                value = child_dict.get(event.tag)
                if value is None:
                    child_dict[event.tag] = event.text
                elif isinstance(value, list):
                    value.append(event.text)
                else:
                    child_dict[event.tag] = [value, event.text]

    if container_started and event:
        yield events.EndTable.from_event(event)


def test_stream_record(fixtures_path):
    stream = parse_file(fixtures_path / 'sample.xml')
    stream = strip_text(stream)
    stream = add_context(stream)
    stream = add_config(stream, Config().fields_with_prefix(['Message', 'Children', 'Child']))
    stream = clean(stream)

    stream = to_table(stream)
    for event in stream:
        print(type(event), event.as_dict())