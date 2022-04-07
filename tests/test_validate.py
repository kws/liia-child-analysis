from typing import Iterator
import xmlschema
from xmlschema import XsdElement, XsdComponent


def test_validate(config_folder, fixtures_path):
    schema = xmlschema.XMLSchema(config_folder / 'cin.xsd')

    errors = list(schema.iter_errors(fixtures_path / 'sample.xml'))
    for error in errors:
        print(error)

    assert len(errors) == 0


class parents(Iterator[XsdComponent]):

    def __init__(self, node: XsdComponent):
        self._node = node

    def __next__(self) -> XsdComponent:
        try:
            self._node = self._node.parent
        except AttributeError:
            self._node = None

        if self._node is None:
            raise StopIteration()

        return self._node


def test_print_schema(config_folder):
    schema = xmlschema.XMLSchema(config_folder / 'cin.xsd')

    for comp in schema.iter_components():
        if isinstance(comp, XsdElement):
            print(comp.name, [p.name for p in parents(comp)])