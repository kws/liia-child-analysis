from sfdata_cincensus_clean.config import Config


def test_config_as_dict():
    config = Config()

    assert "LAchildID" in config['ChildIdentifiers']
    assert "category" in config['ChildCharacteristics']['Ethnicity']


def test_config_all_fields():
    config = Config()

    fields = config.fields

    assert "LAchildID" in fields
    assert "ChildCharacteristics" in fields
    assert "Ethnicity" in fields
    assert "category" not in fields

    assert "LAchildID" in config['ChildIdentifiers']
    assert "category" in config['ChildCharacteristics']['Ethnicity']


def test_config_all_fields_prefix():
    config = Config()

    fields = config.fields_with_prefix(["Message", "Children", "Child"])

    assert "Message/Children/Child/ChildIdentifiers/LAchildID" in fields
    assert "Message/Children/Child/ChildCharacteristics" in fields
    assert "Message/Children/Child/ChildCharacteristics/Ethnicity" in fields
    assert "category" not in fields

    assert "LAchildID" in config['ChildIdentifiers']
    assert "category" in config['ChildCharacteristics']['Ethnicity']