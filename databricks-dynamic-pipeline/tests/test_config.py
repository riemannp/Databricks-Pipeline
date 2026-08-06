import json
import os

def test_sources_config_structure():
    config_path = "config/sources.json"
    assert os.path.exists(config_path), "config/sources.json does not exist."

    with open(config_path, "r") as f:
        data = json.load(f)

    assert isinstance(data, list), "sources.json root element must be a list."
    for source in data:
        assert "source_name" in source
        assert "layer" in source