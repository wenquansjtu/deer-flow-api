# Copyright (c) 2025 Bytedance Ltd. and/or its affiliates
# SPDX-License-Identifier: MIT

"""
Server script for running the DeerFlow API.
"""

import argparse
import asyncio
import logging
import signal
import sys
import uvicorn
from src.server import app
from src.graph.builder import build_graph_with_memory

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Global flag to track shutdown state
is_shutting_down = False

async def cleanup():
    """Cleanup function to handle graceful shutdown"""
    logger.info("Starting cleanup...")
    
    # Get all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    # First, try to gracefully stop langgraph tasks
    for task in tasks:
        if 'langgraph' in str(task):
            try:
                # Set a flag or state to indicate shutdown
                if hasattr(task, 'cancel'):
                    task.cancel()
                # Wait a short time for the task to clean up
                try:
                    await asyncio.wait_for(task, timeout=2.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout waiting for langgraph task to cleanup: {task}")
            except Exception as e:
                logger.error(f"Error cleaning up langgraph task: {e}")
    
    # Then cancel remaining tasks
    remaining_tasks = [t for t in tasks if not t.done()]
    if remaining_tasks:
        logger.info(f"Cancelling {len(remaining_tasks)} outstanding tasks")
        for task in remaining_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete with a timeout
        try:
            await asyncio.wait_for(
                asyncio.gather(*remaining_tasks, return_exceptions=True),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for tasks to cancel")
    
    logger.info("Cleanup completed")

async def shutdown():
    """Coordinated shutdown function"""
    global is_shutting_down
    if is_shutting_down:
        return
    
    is_shutting_down = True
    logger.info("Starting graceful shutdown...")
    
    try:
        await cleanup()
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
    finally:
        # Force exit after cleanup
        sys.exit(0)

def handle_shutdown(signum, frame):
    """Handle graceful shutdown on SIGTERM/SIGINT"""
    if is_shutting_down:
        logger.warning("Received second shutdown signal, forcing exit...")
        sys.exit(1)
    
    logger.info("Received shutdown signal. Starting graceful shutdown...")
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(shutdown())
        else:
            loop.run_until_complete(shutdown())
    except Exception as e:
        logger.error(f"Error initiating shutdown: {e}")
        sys.exit(1)

# Register signal handlers
signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run the DeerFlow API server")
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload (default: True except on Windows)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="Host to bind the server to (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the server to (default: 8000)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Log level (default: info)",
    )

    args = parser.parse_args()

    # Determine reload setting
    reload = False
    if args.reload:
        reload = True

    try:
        logger.info(f"Starting DeerFlow API server on {args.host}:{args.port}")
        uvicorn.run(
            "src.server:app",
            host=args.host,
            port=args.port,
            reload=reload,
            log_level=args.log_level,
        )
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        sys.exit(1)
