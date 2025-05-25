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
from contextlib import suppress

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Global flag to track shutdown state
is_shutting_down = False

async def cancel_task_with_cleanup(task):
    """Cancel a task and wait for it to complete with proper cleanup."""
    if task.done():
        return
    
    with suppress(asyncio.CancelledError):
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for task to cancel: {task.get_name()}")
        except Exception as e:
            logger.error(f"Error cancelling task {task.get_name()}: {e}")

async def cleanup_langgraph_task(task):
    """Specially handle langgraph task cleanup."""
    if task.done():
        return

    try:
        # Try to get the underlying callback manager if it exists
        if hasattr(task, 'get_coro'):
            coro = task.get_coro()
            if hasattr(coro, 'cr_frame') and hasattr(coro.cr_frame, 'f_locals'):
                locals_dict = coro.cr_frame.f_locals
                if 'run_manager' in locals_dict:
                    run_manager = locals_dict['run_manager']
                    if hasattr(run_manager, 'on_chain_end'):
                        await run_manager.on_chain_end()
    except Exception as e:
        logger.error(f"Error cleaning up langgraph callbacks: {e}")
    
    # Then cancel the task
    await cancel_task_with_cleanup(task)

async def cleanup():
    """Cleanup function to handle graceful shutdown"""
    logger.info("Starting cleanup...")
    
    # Get all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    
    # First handle langgraph tasks
    langgraph_tasks = [t for t in tasks if 'langgraph' in str(t) or 'researcher' in str(t)]
    other_tasks = [t for t in tasks if t not in langgraph_tasks]
    
    if langgraph_tasks:
        logger.info(f"Cleaning up {len(langgraph_tasks)} langgraph tasks")
        await asyncio.gather(*[cleanup_langgraph_task(t) for t in langgraph_tasks], return_exceptions=True)
    
    if other_tasks:
        logger.info(f"Cleaning up {len(other_tasks)} other tasks")
        await asyncio.gather(*[cancel_task_with_cleanup(t) for t in other_tasks], return_exceptions=True)
    
    logger.info("Cleanup completed")

async def shutdown():
    """Coordinated shutdown function"""
    global is_shutting_down
    if is_shutting_down:
        return
    
    is_shutting_down = True
    logger.info("Starting graceful shutdown...")
    
    try:
        # Ensure we have an event loop
        loop = asyncio.get_running_loop()
        
        # Set a reasonable timeout for the entire shutdown process
        try:
            await asyncio.wait_for(cleanup(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Cleanup timed out after 10 seconds")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        
        # Give pending tasks a final chance to complete
        pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
        if pending:
            logger.warning(f"{len(pending)} tasks still pending after cleanup")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        # Use loop.stop() instead of sys.exit() to allow for cleaner shutdown
        loop.stop()

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
