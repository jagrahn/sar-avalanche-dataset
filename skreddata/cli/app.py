import logging
import logging.handlers
import os
import pathlib
import sys
import warnings

from rich.logging import Console, RichHandler
import typer

from . import gen


THIS_FOLDER = pathlib.Path(__file__).resolve().parents[2]
SCHEDULER_ADDRESS = "tcp://scheduler:8786"
HELP_FLAGS = {"-h", "--help"}
NO_SCHEDULER_FLAGS = HELP_FLAGS | {"--install-completion", "--show-completion"}


def configure_logging():
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    handlers = [
        RichHandler(
            rich_tracebacks=False,
            tracebacks_show_locals=False,
            console=Console(stderr=True),
        ),
        logging.handlers.RotatingFileHandler(
            THIS_FOLDER / "skredata.log",
            maxBytes=10_000_000,
            backupCount=3,
        ),
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(process)d | %(message)s",
        datefmt="[%X]",
        handlers=handlers,
    )

    warnings.filterwarnings(
        action="ignore",
        category=RuntimeWarning,
        module=r".*gdar",
    )
    warnings.filterwarnings(
        action="ignore",
        category=RuntimeWarning,
        module=r".*shapely",
    )
    logging.captureWarnings(True)
    logging.getLogger("numexpr").setLevel(logging.WARNING)


app = typer.Typer(
    name="skreddata",
    no_args_is_help=True,
    help=(
        "SKREDATA is an interface to the SAR avalanche dataset.\n\n"
        "Copyright: NORCE - The Norwegian Research Center AS.\n"
        "Contact: jgra@norceresearch.no"
    ),
    context_settings={"help_option_names": ["-h", "--help"]},
    add_completion=True,
    rich_markup_mode="rich",
    pretty_exceptions_enable=False,
)
app.add_typer(gen.app, rich_help_panel="Sample generation")


def _needs_scheduler(argv):
    return len(argv) > 1 and not any(flag in argv[1:] for flag in NO_SCHEDULER_FLAGS)


def main():
    configure_logging()

    if not _needs_scheduler(sys.argv):
        app()
        return

    from dask import distributed

    scheduler_address = os.environ.get("DASK_SCHEDULER_ADDRESS", SCHEDULER_ADDRESS)
    client = distributed.Client(scheduler_address)
    with client:
        app()
