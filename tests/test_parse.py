from lxml.etree import ElementTree

from wrangling.cincensus.main import degradefile, cleanfile, build_cinrecord


def test_parse(fixtures_path):
    degraded_tree: ElementTree = degradefile(fixtures_path / "sample.xml")
    root = degraded_tree.getroot()

    assert root.tag == "Message"


def test_flatfile(fixtures_path, config):
    tree = degradefile(fixtures_path / "sample.xml")
    cleaned_tree = cleanfile(tree, config)

    flatfile = build_cinrecord([cleaned_tree])
    flatfile.to_csv(fixtures_path / "sample.csv", index=False)

