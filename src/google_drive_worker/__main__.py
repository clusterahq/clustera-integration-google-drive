"""Entry point for Google Drive integration worker.

This module sets up the async runtime and starts the worker.
"""

import asyncio
import sys
from typing import Optional

from .config import settings
from .worker import GoogleDriveWorker


async def main() -> int:
    """Main entry point for the worker.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    worker: Optional[GoogleDriveWorker] = None

    try:
        # Initialize and start worker
        worker = GoogleDriveWorker(settings)
        await worker.start()
        return 0

    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")
        return 0

    except Exception as e:
        print(f"Worker failed: {e}", file=sys.stderr)
        return 1

    finally:
        # Ensure cleanup
        if worker:
            await worker.stop()


def run() -> None:
    """Run the worker with proper async context."""
    # Python 3.11+ has better async exception handling
    if sys.version_info >= (3, 11):
        with asyncio.Runner() as runner:
            sys.exit(runner.run(main()))
    else:
        # Fallback for older Python versions
        sys.exit(asyncio.run(main()))


if __name__ == "__main__":
    run()