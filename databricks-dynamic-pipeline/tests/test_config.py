import json
import os

import pytest

CONFIG_PATH = "config/sources.json"


@pytest.fixture(scope="module")
def sources_config():
    """Load sources configuration for testing."""
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


class TestSourcesConfigStructure:
    """Test suite for sources.json configuration structure."""

    def test_config_file_exists(self):
        """Verify config/sources.json file exists."""
        assert os.path.exists(CONFIG_PATH), f"{CONFIG_PATH} does not exist."

    def test_config_is_valid_json(self):
        """Verify config file contains valid JSON."""
        with open(CONFIG_PATH, "r") as f:
            data = json.load(f)  # Should not raise JSONDecodeError
            assert data is not None

    def test_config_root_is_list(self, sources_config):
        """Verify root element is a list."""
        assert isinstance(sources_config, list), "sources.json root must be a list."

    def test_config_not_empty(self, sources_config):
        """Verify config contains at least one source."""
        assert len(sources_config) > 0, "sources.json must contain at least one source."


class TestSourceConfigFields:
    """Test suite for required fields in each source configuration."""

    def test_all_sources_have_source_name(self, sources_config):
        """Verify every source has a source_name field."""
        for idx, source in enumerate(sources_config):
            assert "source_name" in source, f"Source at index {idx} missing 'source_name'"
            assert isinstance(source["source_name"], str), f"source_name at index {idx} must be string"
            assert len(source["source_name"]) > 0, f"source_name at index {idx} cannot be empty"

    def test_all_sources_have_layer(self, sources_config):
        """Verify every source has a layer field."""
        for idx, source in enumerate(sources_config):
            assert "layer" in source, f"Source at index {idx} missing 'layer'"

    def test_layer_values_are_valid(self, sources_config):
        """Verify layer field contains valid values."""
        valid_layers = ["bronze_to_silver", "silver_to_gold", "gold_custom"]
        for idx, source in enumerate(sources_config):
            layer = source.get("layer")
            assert layer in valid_layers, (
                f"Source '{source.get('source_name')}' at index {idx} has invalid layer '{layer}'. "
                f"Valid layers: {valid_layers}"
            )


class TestBronzeToSilverSources:
    """Test suite for bronze_to_silver layer sources."""

    @pytest.fixture
    def bronze_sources(self, sources_config):
        """Filter sources with bronze_to_silver layer."""
        return [s for s in sources_config if s.get("layer") == "bronze_to_silver"]

    def test_bronze_sources_have_required_fields(self, bronze_sources):
        """Verify bronze sources have all required fields."""
        required_fields = ["source_name", "layer", "source_type", "source_format", "source_path"]
        for source in bronze_sources:
            for field in required_fields:
                assert field in source, (
                    f"Bronze source '{source.get('source_name')}' missing required field '{field}'"
                )

    def test_source_type_is_auto_loader(self, bronze_sources):
        """Verify source_type is 'auto_loader' for bronze sources."""
        for source in bronze_sources:
            assert source.get("source_type") == "auto_loader", (
                f"Source '{source.get('source_name')}' should use 'auto_loader'"
            )

    def test_source_format_is_valid(self, bronze_sources):
        """Verify source_format is a supported type."""
        valid_formats = ["csv", "json", "parquet", "avro"]
        for source in bronze_sources:
            fmt = source.get("source_format")
            assert fmt in valid_formats, (
                f"Source '{source.get('source_name')}' has unsupported format '{fmt}'. "
                f"Valid formats: {valid_formats}"
            )

    def test_source_path_is_volume_path(self, bronze_sources):
        """Verify source_path points to Unity Catalog volume."""
        for source in bronze_sources:
            path = source.get("source_path", "")
            assert path.startswith("/Volumes/"), (
                f"Source '{source.get('source_name')}' path should start with '/Volumes/' (Unity Catalog)"
            )

    def test_read_options_is_dict(self, bronze_sources):
        """Verify read_options is a dictionary if present."""
        for source in bronze_sources:
            if "read_options" in source:
                assert isinstance(source["read_options"], dict), (
                    f"Source '{source.get('source_name')}' read_options must be a dict"
                )

    def test_silver_transform_structure(self, bronze_sources):
        """Verify silver_transform configuration if present."""
        for source in bronze_sources:
            if "silver_transform" in source:
                transform = source["silver_transform"]
                assert isinstance(transform, dict), "silver_transform must be a dict"
                assert "mode" in transform, f"Source '{source.get('source_name')}' silver_transform missing 'mode'"

    def test_cdc_merge_has_primary_key(self, bronze_sources):
        """Verify CDC merge mode has primary_key configured."""
        for source in bronze_sources:
            transform = source.get("silver_transform", {})
            if transform.get("mode") == "cdc_merge":
                assert "primary_key" in transform, (
                    f"Source '{source.get('source_name')}' uses cdc_merge but missing 'primary_key'"
                )
                assert isinstance(transform["primary_key"], str), "primary_key must be string"


class TestGoldCustomSources:
    """Test suite for gold_custom layer sources."""

    @pytest.fixture
    def gold_sources(self, sources_config):
        """Filter sources with gold_custom layer."""
        return [s for s in sources_config if s.get("layer") == "gold_custom"]

    def test_gold_sources_have_transform_config(self, gold_sources):
        """Verify gold sources have gold_transform configuration."""
        for source in gold_sources:
            assert "gold_transform" in source, (
                f"Gold source '{source.get('source_name')}' missing 'gold_transform'"
            )

    def test_gold_transform_has_mode(self, gold_sources):
        """Verify gold_transform has mode field."""
        for source in gold_sources:
            transform = source.get("gold_transform", {})
            assert "mode" in transform, (
                f"Gold source '{source.get('source_name')}' gold_transform missing 'mode'"
            )

    def test_custom_python_mode_has_function_name(self, gold_sources):
        """Verify custom_python mode has function_name."""
        for source in gold_sources:
            transform = source.get("gold_transform", {})
            if transform.get("mode") == "custom_python":
                assert "function_name" in transform, (
                    f"Gold source '{source.get('source_name')}' uses custom_python but missing 'function_name'"
                )


class TestSourceNameUniqueness:
    """Test suite for source name uniqueness."""

    def test_source_names_are_unique(self, sources_config):
        """Verify all source names are unique."""
        source_names = [s.get("source_name") for s in sources_config]
        duplicates = [name for name in source_names if source_names.count(name) > 1]
        assert len(duplicates) == 0, f"Duplicate source names found: {set(duplicates)}"


class TestSpecificSources:
    """Test suite for specific known sources in the config."""

    def test_orders_source_exists(self, sources_config):
        """Verify orders source is configured."""
        source_names = [s.get("source_name") for s in sources_config]
        assert "olist_orders" in source_names, "olist_orders source missing from config"

    def test_customers_source_exists(self, sources_config):
        """Verify customers source is configured."""
        source_names = [s.get("source_name") for s in sources_config]
        assert "olist_customers" in source_names, "olist_customers source missing from config"

    def test_reviews_source_exists(self, sources_config):
        """Verify reviews source is configured."""
        source_names = [s.get("source_name") for s in sources_config]
        assert "olist_reviews" in source_names, "olist_reviews source missing from config"
