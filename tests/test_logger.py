import os
import logging
import pytest
from unittest.mock import patch, MagicMock
from postgres_cdc.utils.logger import setup_logger, get_logger
from rich.logging import RichHandler

@pytest.fixture(autouse=True)
def cleanup_loggers():
    """Fixture to clean up loggers after each test."""
    # Store initial state of loggers
    initial_loggers = list(logging.Logger.manager.loggerDict.keys())
    yield
    # Restore loggers to initial state
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name not in initial_loggers and name != 'dlt': # 'dlt' logger might be added by dlt framework and should not be removed
            del logging.Logger.manager.loggerDict[name]
            
    # Also clean up handlers from root logger if any were added
    if logging.root.handlers:
        logging.root.handlers.clear()


def test_setup_logger_defaults():
    """Test setup_logger with default settings (INFO level)."""
    logger_name = "test_logger_defaults"
    logger = setup_logger(logger_name)
    
    assert isinstance(logger, logging.Logger)
    assert logger.name == logger_name
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], RichHandler)
    assert not logger.propagate # Should not propagate to root


def test_setup_logger_custom_level():
    """Test setup_logger with explicit level argument."""
    logger_name = "test_logger_debug"
    logger = setup_logger(logger_name, level="DEBUG")
    
    assert logger.level == logging.DEBUG


def test_setup_logger_env_var_override():
    """Test that LOG_LEVEL env var sets the level when argument is not provided."""
    logger_name = "test_logger_env"
    
    with patch.dict(os.environ, {"LOG_LEVEL": "ERROR"}, clear=True):
        # Must create a new logger instance (or clear existing) as logging.getLogger is cached
        logger = setup_logger(logger_name)
        assert logger.level == logging.ERROR


def test_get_logger_alias():
    """Test that get_logger is a working alias for setup_logger."""
    logger_name = "test_alias"
    logger_instance = get_logger(logger_name)
    
    assert isinstance(logger_instance, logging.Logger)
    assert logger_instance.name == logger_name
    assert logger_instance.level == logging.INFO # Default level
    
    # Ensure it configures like setup_logger
    assert len(logger_instance.handlers) == 1
    assert isinstance(logger_instance.handlers[0], RichHandler)


def test_logger_is_singleton_for_same_name():
    """Test that calling setup_logger twice with the same name returns the same configured instance."""
    name = "test_singleton"
    
    # Ensure no previous logger with this name
    if name in logging.Logger.manager.loggerDict:
        del logging.Logger.manager.loggerDict[name]
        
    l1 = setup_logger(name, level="INFO")
    l2 = setup_logger(name, level="DEBUG") # Try to set different level
    
    assert l1 is l2 # Should be the same instance
    assert len(l1.handlers) == 1 # Should not add duplicate handlers


def test_logger_differs_for_different_name():
    """Test that calling setup_logger with different names returns different instances."""
    l1 = setup_logger("name1")
    l2 = setup_logger("name2")
    
    assert l1 is not l2
    assert l1.name == "name1"
    assert l2.name == "name2"


    def test_logger_captures_messages(caplog):


        """Test that logger messages are captured."""


        logger_name = "test_capture"


        logger = setup_logger(logger_name, level="INFO")


        


        # Temporarily enable propagation for caplog to capture messages


        logger.propagate = True


    


        with caplog.at_level(logging.INFO):


            logger.info("This is an info message.")


            logger.debug("This is a debug message.")


        


        logger.propagate = False # Reset for other tests if needed


    


        assert "This is an info message." in caplog.text


        assert "This is a debug message." not in caplog.text # Should not capture DEBUG


        # assert logger_name in caplog.text # caplog.text usually doesn't include logger name prefix by default



