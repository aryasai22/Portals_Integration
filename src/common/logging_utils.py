"""
Logging setup and metrics tracking with type safety
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
import json

from src.common.config import LOG_LEVEL, LOG_TO_FILE, LOG_DIR
from src.common.enums import PipelineStatus


def setup_logging(name: str, debug: bool = False) -> logging.Logger:
    """
    Setup logging with console and file handlers.

    Args:
        name: Logger name
        debug: Enable debug logging

    Returns:
        Configured logger instance
    """
    if LOG_TO_FILE:
        LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if debug else getattr(logging, LOG_LEVEL))
    logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)

    # File handler
    if LOG_TO_FILE:
        log_file = LOG_DIR / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_format = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

    return logger


@dataclass
class PipelineMetrics:
    """
    Track pipeline execution metrics with type safety.

    Attributes:
        pipeline_name: Name of the pipeline
        resource: Optional resource being processed
        start_time: When execution started
        end_time: When execution ended
        status: Current execution status
        rows_input: Number of input rows
        rows_output: Number of output rows
        errors: List of error records
    """
    pipeline_name: str
    resource: Optional[str] = None
    start_time: Optional[datetime] = field(default=None, init=False)
    end_time: Optional[datetime] = field(default=None, init=False)
    status: PipelineStatus = field(default=PipelineStatus.PENDING, init=False)
    rows_input: int = field(default=0, init=False)
    rows_output: int = field(default=0, init=False)
    errors: List[Dict[str, str]] = field(default_factory=list, init=False)

    @property
    def duration_sec(self) -> float:
        """Calculate duration in seconds"""
        if not self.start_time or not self.end_time:
            return 0.0
        return (self.end_time - self.start_time).total_seconds()

    def start(self) -> None:
        """Start metrics tracking"""
        self.start_time = datetime.utcnow()
        self.status = PipelineStatus.RUNNING

    def set_input_count(self, count: int) -> None:
        """Set number of input rows"""
        self.rows_input = count

    def set_output_count(self, count: int) -> None:
        """Set number of output rows"""
        self.rows_output = count

    def add_error(self, error: str) -> None:
        """Add error to tracking"""
        self.errors.append({
            'timestamp': datetime.utcnow().isoformat(),
            'error': str(error)
        })

    def finish(self, status: str = "SUCCESS") -> None:
        """
        Finish metrics tracking.

        Args:
            status: Final status string (converted to enum)
        """
        self.end_time = datetime.utcnow()
        # Accept string for backward compatibility
        if isinstance(status, str):
            self.status = PipelineStatus(status)
        else:
            self.status = status

    def get_summary(self) -> str:
        """Get human-readable summary"""
        return (
            f"Status: {self.status.value} | "
            f"Duration: {self.duration_sec:.2f}s | "
            f"Rows: {self.rows_input} -> {self.rows_output}"
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'pipeline': self.pipeline_name,
            'resource': self.resource,
            'start_ts': self.start_time.isoformat() if self.start_time else None,
            'end_ts': self.end_time.isoformat() if self.end_time else None,
            'duration_sec': self.duration_sec,
            'rows_input': self.rows_input,
            'rows_output': self.rows_output,
            'status': self.status.value,
            'errors': self.errors
        }

    @property
    def metrics(self) -> Dict[str, Any]:
        """
        Backward compatibility property.
        Returns the same dictionary structure as the old implementation.
        """
        return self.to_dict()

    def save(self, output_dir: Optional[Path] = None) -> None:
        """
        Save metrics to JSON file.

        Args:
            output_dir: Directory to save metrics (defaults to output/metrics)
        """
        if output_dir is None:
            output_dir = Path("output/metrics")

        output_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{self.pipeline_name}_{datetime.now().strftime('%Y%m%d')}.json"
        filepath = output_dir / filename

        with open(filepath, 'a', encoding='utf-8') as f:
            f.write(json.dumps(self.to_dict()) + '\n')


def log_separator(logger: logging.Logger, char: str = '=', length: int = 80) -> None:
    """
    Log a separator line.

    Args:
        logger: Logger instance
        char: Character to use for separator
        length: Length of separator line
    """
    logger.info(char * length)


def log_section(logger: logging.Logger, title: str) -> None:
    """
    Log a section header with separators.

    Args:
        logger: Logger instance
        title: Section title
    """
    log_separator(logger)
    logger.info(title)
    log_separator(logger)
