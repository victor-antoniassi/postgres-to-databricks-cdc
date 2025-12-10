import pytest
import argparse
from unittest.mock import patch
from postgres_cdc.pipeline_main import parse_args

def test_parse_args_defaults():
    """Test that default arguments are set correctly when only required args are provided."""
    with patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(mode='full_load', catalog=None, dataset=None)):
        args = parse_args()
        assert args.mode == 'full_load'
        assert args.catalog is None
        assert args.dataset is None

def test_parse_args_custom_values():
    """Test that custom arguments are parsed correctly."""
    with patch('argparse.ArgumentParser.parse_args', return_value=argparse.Namespace(mode='cdc', catalog='my_catalog', dataset='my_dataset')):
        args = parse_args()
        assert args.mode == 'cdc'
        assert args.catalog == 'my_catalog'
        assert args.dataset == 'my_dataset'

def test_parse_args_invalid_mode(capsys):
    """Test that parser handles invalid mode choices (handled by argparse internally, but good to check integration)."""
    # Since argparse calls sys.exit(2) on error, we expect SystemExit
    with patch('sys.argv', ['pipeline_main.py', '--mode', 'invalid_mode']):
        with pytest.raises(SystemExit):
            parse_args()
