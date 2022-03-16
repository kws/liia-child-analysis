from sfdata_stream_parser import events
from sfdata_stream_parser.events import StartElement
from sfdata_stream_parser.parser.xml import parse_file

from sfdata_cincensus_clean.config import Config
from sfdata_cincensus_clean.filters import add_context, add_config, strip_text, clean


def test_context(fixtures_path):
    stream = parse_file(fixtures_path / 'sample.xml')
    stream = strip_text(stream)
    stream = add_context(stream)
    stream = add_config(stream, Config().fields_with_prefix(['Message', 'Children', 'Child']))
    stream = clean(stream)

    for event in stream:
        if isinstance(event, StartElement):
            print("/".join(event.get('context', [])), event.text, event.get('config'))


def test_add_context_default():
    stream = list(add_context([
        events.StartElement(tag='a'),
        events.StartElement(tag='b'),
        events.EndElement(tag='b'),
        events.EndElement(tag='a'),
    ]))
    stream = [ev.context for ev in stream]
    assert stream == [('a',), ('a', 'b'), ('a', 'b'), ('a',)]


def test_add_context_existing():
    context = []
    list(add_context(events.StartElement(tag='a'), context=context))
    assert context == ['a']
    list(add_context(events.StartElement(tag='b'), context=context))
    assert context == ['a', 'b']
    list(add_context(events.EndElement(tag='b'), context=context))
    assert context == ['a']
    list(add_context(events.EndElement(tag='a'), context=context))
    assert context == []

    context = ['s']
    list(add_context(events.StartElement(tag='a'), context=context))
    assert context == ['s', 'a']
    list(add_context(events.StartElement(tag='b'), context=context))
    assert context == ['s', 'a', 'b']
    list(add_context(events.EndElement(tag='b'), context=context))
    assert context == ['s', 'a']
    list(add_context(events.EndElement(tag='a'), context=context))
    assert context == ['s', ]
