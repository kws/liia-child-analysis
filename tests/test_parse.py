from io import StringIO

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

    outfile = StringIO()
    flatfile.to_csv(outfile, index=False)

    with open(fixtures_path / "sample.csv") as f:
        sample_file = f.read()

    assert outfile.getvalue() == sample_file

