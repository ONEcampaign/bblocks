import logging

# Create a root logger
logger = logging.getLogger(__name__)

# Create two handlers (terminal and file)
shell_handler = logging.StreamHandler()

# Set levels for the logger, shell and file
logger.setLevel(logging.DEBUG)
shell_handler.setLevel(logging.DEBUG)

# Format the outputs
fmt_shell = "%(levelname)s [%(filename)s: %(funcName)s:] %(message)s"

# Create formatters
shell_formatter = logging.Formatter(fmt_shell)


# Add formatters to handlers
shell_handler.setFormatter(shell_formatter)

# Add handlers to the logger
logger.addHandler(shell_handler)
