from lxml.etree import ElementTree

from wrangling.cincensus.main import degradefile


def test_parse(fixtures_path):
    degraded_tree: ElementTree = degradefile(fixtures_path / "sample.xml")
    root = degraded_tree.getroot()

    assert root.tag == "Message"

