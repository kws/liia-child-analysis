import dataclasses
import functools
from collections import Callable
from functools import cached_property
from io import StringIO
from more_itertools import peekable

from lxml.etree import ElementTree

from sfdata_cincensus_clean.config import Config
from sfdata_cincensus_clean.filters import strip_text, add_context, add_config, clean
from sfdata_stream_parser import events
from sfdata_stream_parser.checks import EventCheck, and_check, type_check
from sfdata_stream_parser.events import ParseEvent
from sfdata_stream_parser.filters.generic import streamfilter, filter_stream, first_then_rest
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


def __until_match(stream, check):
    for event in stream:
        if check(event):
            return event
        yield event


def __collector(func, check: EventCheck, end_check: EventCheck):
    @functools.wraps(func)
    def wrapper(stream, *args, **kwargs):
        stream = peekable(stream)
        while stream:
            yield from __until_match(stream, check)
            yield from func(__until_match(stream, end_check), *args, **kwargs)

    return wrapper


def collector(*args, **kwargs):
    if len(args) > 0 and isinstance(args[0], Callable):
        return __collector(args[0], *args[1:], **kwargs)
    else:
        def wrapper(func):
            return __collector(func, *args, **kwargs)

        return wrapper


def property_check(**kwargs) -> EventCheck:
    def _check(event: events.ParseEvent) -> bool:
        for key, value in kwargs.items():
            if getattr(event, key, None) != value:
                return False
        return True

    return _check


def _prop_test(event_type, **kwargs) -> EventCheck:
    return and_check(
        type_check(event_type),
        property_check(**kwargs),
    )


def xml_collector(func):
    @functools.wraps(func)
    def wrapper(stream, *args, **kwargs):
        try:
            start_element = next(stream)
        except StopIteration:
            return []
        assert isinstance(start_element, events.StartElement)
        return func(__until_match(stream, _prop_test(events.EndElement, tag=start_element.tag)), *args, **kwargs)
    return wrapper


def __get_element_text(event):
    text = event.get('text', '').strip()
    if isinstance(event, events.StartElement) and text != '':
        return text


def _reduce_dict(dict_instance):
    new_dict = {}
    for key, value in dict_instance.items():
        if len(value) == 1:
            new_dict[key] = value[0]
        else:
            new_dict[key] = value


@xml_collector
def _text_collector(stream):
    data_dict = {}
    for event in stream:
        text = __get_element_text(event)
        if text:
            data_dict.setdefault(event.tag, []).append(text)

    return _reduce_dict(data_dict)


@xml_collector
def _cin_collector(stream):
    data_dict = {}
    stream = peekable(stream)
    while stream:
        event = stream.peek()
        if event.tag in ('Assessments', 'CINPlanDates', 'Section47', 'ChildProtectionPlans'):
            data_dict.setdefault(event.tag, []).append(_text_collector(stream))
        else:
            text = __get_element_text(event)
            if text:
                data_dict.setdefault(event.tag, []).append(text)
            next(stream)

    return _reduce_dict(data_dict)


class CINEvent(events.ParseEvent):
    pass


@collector(
    check=_prop_test(events.StartElement, context=('Message', 'Children', 'Child')),
    end_check=_prop_test(events.EndElement, context=('Message', 'Children', 'Child'))
)
def collect_child(stream):
    data_dict = {}
    stream = peekable(stream)
    while stream:
        event = stream.peek()
        if event.tag in ('ChildIdentifiers', 'ChildCharacteristics'):
            data_dict.setdefault(event.tag, []).append(_text_collector(stream))
        elif event.tag == 'CINdetails':
            data_dict.setdefault(event.tag, []).append(_cin_collector(stream))
        else:
            next(stream)

    if data_dict:
        yield CINEvent(record=_reduce_dict(data_dict))


def test_stream_record(fixtures_path):
    stream = parse_file(fixtures_path / 'sample.xml')
    stream = strip_text(list(stream) * 3)
    stream = add_context(stream)
    stream = add_config(stream, Config().fields_with_prefix(['Message', 'Children', 'Child']))
    stream = clean(stream)

    stream = collect_child(stream)
    stream = filter_stream(stream, type_check(CINEvent))
    records = list(stream)
    assert len(records) == 3

    # for event in records:
    # print(event.child_detail)
    # print(event.cin_detail)


@streamfilter
def clean_test(event):
    print("Cleaning", event)
    return event.from_event(event, cleaned_value=f'{event.text} cleaned')
