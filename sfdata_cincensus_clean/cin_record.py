from typing import Generator, Iterator

from more_itertools import peekable

from sfdata_stream_parser import events
from sfdata_stream_parser.checks import (
    EventCheck,
    and_check,
    type_check,
    property_check,
)
from sfdata_stream_parser.collectors import collector, xml_collector


class CINEvent(events.ParseEvent):
    pass


class HeaderEvent(events.ParseEvent):
    pass


def xml_prop_check(event_type, **kwargs) -> EventCheck:
    """
    Checks for xml event with the given type and properties.
    """
    return and_check(
        type_check(event_type),
        property_check(**kwargs),
    )


def _reduce_dict(dict_instance):
    new_dict = {}
    for key, value in dict_instance.items():
        if len(value) == 1:
            new_dict[key] = value[0]
        else:
            new_dict[key] = value
    return new_dict


@xml_collector
def text_collector(stream):
    data_dict = {}
    for event in stream:
        if isinstance(event, events.StartElement) and event.normalised_text:
            data_dict.setdefault(event.tag, []).append(event.normalised_text)

    return _reduce_dict(data_dict)


@xml_collector
def cin_collector(stream):
    data_dict = {}
    stream = peekable(stream)
    while stream:
        event = stream.peek()
        if event.tag in (
            "Assessments",
            "CINPlanDates",
            "Section47",
            "ChildProtectionPlans",
        ):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
        else:
            if isinstance(event, events.StartElement) and event.normalised_text:
                data_dict.setdefault(event.tag, []).append(event.normalised_text)
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def child_collector(stream):
    data_dict = {}
    stream = peekable(stream)
    assert stream.peek().tag == "Child"
    while stream:
        event = stream.peek()
        if event.tag in ("ChildIdentifiers", "ChildCharacteristics"):
            data_dict.setdefault(event.tag, []).append(text_collector(stream))
        elif event.tag == "CINdetails":
            data_dict.setdefault(event.tag, []).append(cin_collector(stream))
        else:
            next(stream)

    return _reduce_dict(data_dict)


@xml_collector
def message_collector(stream):
    stream = peekable(stream)
    assert stream.peek().tag == "Message", "Expected Message, got {}".format(
        stream.peek().tag
    )
    while stream:
        event = stream.peek()
        if event.tag == "Header":
            header_record = text_collector(stream)
            if header_record:
                yield HeaderEvent(record=header_record)
        elif event.tag == "Child":
            cin_record = child_collector(stream)
            if cin_record:
                yield CINEvent(record=cin_record)
        else:
            next(stream)


__EXPORT_HEADERS = [
    "LAchildID",
    "Date",
    "Type",
    "CINreferralDate",
    "ReferralSource",
    "PrimaryNeedCode",
    "CINclosureDate",
    "ReasonForClosure",
    "DateOfInitialCPC",
    "ReferralNFA",
    "S47ActualStartDate",
    "InitialCPCtarget",
    "ICPCnotRequired",
    "AssessmentActualStartDate",
    "AssessmentInternalReviewDate",
    "AssessmentAuthorisationDate",
    "Factors",
    "CPPstartDate",
    "CPPendDate",
    "InitialCategoryOfAbuse",
    "LatestCategoryOfAbuse",
    "NumberOfPreviousCPP",
    "CPPreview",
    "UPN",
    "FormerUPN",
    "UPNunknown",
    "PersonBirthDate",
    "GenderCurrent",
    "PersonDeathDate",
    "PersonSchoolYear",
    "Ethnicity",
    "Disabilities",
]


def _maybe_list(value):
    if value is None:
        value = []
    if not isinstance(value, list):
        value = [value]
    return value


def cin_event(record, property, event_name=None):
    if event_name is None:
        event_name = property
    value = record.get(property)
    if value:
        new_record = {**record, "Date": value, "Type": event_name}
        return ({k: new_record.get(k) for k in __EXPORT_HEADERS},)

    return ()


def event_to_records(event: CINEvent) -> Iterator[dict]:
    record = event.record
    child = {
        **record.get("ChildIdentifiers", {}),
        **record.get("ChildCharacteristics", {}),
    }
    child["Disabilities"] = ",".join(_maybe_list(child.get("Disability")))

    for cin_item in _maybe_list(record.get("CINdetails")):
        yield from cin_event({**child, **cin_item}, "CINreferralDate")
        yield from cin_event({**child, **cin_item}, "CINclosureDate")
        yield from cin_event({**child, **cin_item}, "DateOfInitialCPC")

        for assessment in _maybe_list(cin_item.get("Assessments")):
            assessment["Factors"] = ",".join(
                _maybe_list(assessment.get("AssessmentFactors"))
            )
            yield from cin_event(
                {**child, **cin_item, **assessment}, "AssessmentActualStartDate"
            )
            yield from cin_event(
                {**child, **cin_item, **assessment}, "AssessmentAuthorisationDate"
            )

        for cpp in _maybe_list(cin_item.get("CINPlanDates")):
            yield from cin_event(
                {**child, **cin_item, **cpp},
                "CINPlanStartDate",
                event_name="CPPstartDate",
            )
            yield from cin_event(
                {**child, **cin_item, **cpp}, "CINPlanEndDate", event_name="CPPendDate"
            )

        for s47 in _maybe_list(cin_item.get("Section47")):
            yield from cin_event({**child, **cin_item, **s47}, "S47ActualStartDate")


def export_table(stream, filename):
    rows = []
    for event in stream:
        if isinstance(event, CINEvent):
            rows += [r for r in event_to_records(event)]
