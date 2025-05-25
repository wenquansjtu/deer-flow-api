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
import weakref
from src.server import app
from src.graph.builder import build_graph_with_memory
from contextlib import suppress
from functools import partial
from typing import Any, Dict, Optional

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Monkey patch to fix FuturesDict callback issue
def patch_futures_dict():
    """Patch FuturesDict to handle None callbacks gracefully."""
    try:
        from langgraph.pregel.runner import FuturesDict
        
        def safe_on_done(self, task, exception):
            """Safe version of on_done that handles None callbacks."""
            try:
                # Check if callback exists and is callable
                if hasattr(self, 'callback') and self.callback is not None and callable(self.callback):
                    # Call the callback safely with proper argument handling
                    try:
                        # Always try single argument first (most common case)
                        try:
                            self.callback(task)
                            logger.debug("Successfully called callback with task only")
                        except TypeError as te:
                            # If single argument fails, try with both arguments
                            if "positional argument" in str(te) or "takes 1" in str(te):
                                try:
                                    self.callback(task, exception)
                                    logger.debug("Successfully called callback with task and exception")
                                except TypeError as te2:
                                    # If both fail, try with no arguments
                                    try:
                                        self.callback()
                                        logger.debug("Successfully called callback with no arguments")
                                    except TypeError:
                                        logger.error(f"Could not determine callback signature. Single arg error: {te}, Double arg error: {te2}")
                            else:
                                # Re-raise if it's not an argument count issue
                                raise te
                    except Exception as callback_error:
                        logger.error(f"Error in callback execution: {callback_error}")
                else:
                    # If callback is None or not callable, just log and return
                    logger.debug("Skipping None or non-callable callback in FuturesDict.on_done")
                    
                # Always return None to prevent further issues
                return None
                    
            except Exception as e:
                logger.error(f"Error in FuturesDict.on_done: {e}")
                return None
        
        # Replace the method entirely
        FuturesDict.on_done = safe_on_done
        logger.info("Successfully patched FuturesDict.on_done")
        
    except ImportError as e:
        logger.warning(f"Could not patch FuturesDict: {e}")
    except Exception as e:
        logger.error(f"Error patching FuturesDict: {e}")

# Apply the patch early
patch_futures_dict()

# Global flag to track shutdown state
is_shutting_down = False
shutdown_event = asyncio.Event()

async def cancel_task_with_cleanup(task):
    """Cancel a task and wait for it to complete with proper cleanup."""
    if task.done():
        return
    
    try:
        task.cancel()
        
        # Wait for the task to complete with multiple timeout attempts
        for timeout in [0.5, 1.0, 2.0]:
            try:
                await asyncio.wait_for(task, timeout=timeout)
                break
            except asyncio.TimeoutError:
                if timeout == 2.0:  # Last attempt
                    logger.warning(f"Timeout waiting for task to cancel: {task.get_name()}")
                continue
            except asyncio.CancelledError:
                # Task was successfully cancelled
                break
            except Exception as e:
                logger.error(f"Error cancelling task {task.get_name()}: {e}")
                break
                
    except Exception as e:
        # Suppress all errors during cleanup
        logger.debug(f"Suppressed error during task cleanup: {e}")
    finally:
        # Ensure task is marked as done
        if not task.done():
            logger.warning(f"Task {task.get_name()} still not done after cleanup attempts")

async def cleanup_callback_manager(run_manager: Any) -> None:
    """Safely cleanup a callback manager."""
    try:
        if hasattr(run_manager, 'on_chain_end'):
            # 创建一个空的输出对象
            empty_output: Dict[str, Any] = {}
            try:
                await asyncio.wait_for(
                    run_manager.on_chain_end(outputs=empty_output), 
                    timeout=2.0
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout in callback manager cleanup")
            except TypeError as e:
                # 如果 on_chain_end 的签名不匹配，尝试不同的调用方式
                try:
                    await asyncio.wait_for(
                        run_manager.on_chain_end(), 
                        timeout=2.0
                    )
                except Exception as inner_e:
                    logger.error(f"Error in alternative callback cleanup: {inner_e}")
            except Exception as e:
                logger.error(f"Error in callback cleanup: {e}")
    except Exception as e:
        logger.error(f"Error accessing callback manager: {e}")

async def safe_cleanup_futures_dict(futures_dict):
    """Safely clean up a FuturesDict instance."""
    try:
        if hasattr(futures_dict, 'callback'):
            futures_dict.callback = None
        if hasattr(futures_dict, 'callbacks'):
            futures_dict.callbacks.clear()
        if hasattr(futures_dict, '_done_callbacks'):
            futures_dict._done_callbacks.clear()
        # Cancel any pending futures
        if hasattr(futures_dict, '_futures'):
            for fut in futures_dict._futures:
                if not fut.done():
                    fut.cancel()
    except Exception as e:
        logger.error(f"Error cleaning up FuturesDict: {e}")

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
                
                # Handle FuturesDict instances more comprehensively
                futures_dict_candidates = []
                
                # Direct FuturesDict instance
                if 'futures_dict' in locals_dict:
                    futures_dict_candidates.append(locals_dict['futures_dict'])
                
                # Look for FuturesDict in task attributes
                if hasattr(task, '_futures_dict'):
                    futures_dict_candidates.append(task._futures_dict)
                
                # Look for FuturesDict in task state
                if hasattr(task, 'state') and hasattr(task.state, 'futures_dict'):
                    futures_dict_candidates.append(task.state.futures_dict)
                
                # Clean up all found FuturesDict instances
                for futures_dict in futures_dict_candidates:
                    if futures_dict is not None:
                        await safe_cleanup_futures_dict(futures_dict)
                
                # 尝试获取和清理所有可能的回调管理器
                managers_to_cleanup = []
                
                # 直接的 run_manager
                if 'run_manager' in locals_dict:
                    managers_to_cleanup.append(locals_dict['run_manager'])
                
                # 检查 task.proc 中的回调管理器
                if hasattr(task, 'proc'):
                    proc = task.proc
                    if hasattr(proc, 'callbacks'):
                        managers_to_cleanup.append(proc.callbacks)
                    if hasattr(proc, 'callback_manager'):
                        managers_to_cleanup.append(proc.callback_manager)
                
                # 清理所有找到的回调管理器
                for manager in managers_to_cleanup:
                    if manager:
                        await cleanup_callback_manager(manager)
                
    except Exception as e:
        logger.error(f"Error cleaning up langgraph callbacks: {e}")
    finally:
        # 无论如何都要取消任务
        await cancel_task_with_cleanup(task)

async def cleanup():
    """Cleanup function to handle graceful shutdown"""
    logger.info("Starting cleanup...")
    
    try:
        # Get all running tasks
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        
        # Categorize tasks
        langgraph_tasks = []
        server_tasks = []
        other_tasks = []
        
        for task in tasks:
            task_str = str(task)
            if any(name in task_str.lower() for name in ['langgraph', 'researcher', 'agent', 'chain', 'pregel']):
                langgraph_tasks.append(task)
            elif any(name in task_str.lower() for name in ['server', 'uvicorn', 'http']):
                server_tasks.append(task)
            else:
                other_tasks.append(task)
        
        # Clean up langgraph tasks first (most problematic)
        if langgraph_tasks:
            logger.info(f"Cleaning up {len(langgraph_tasks)} langgraph tasks")
            cleanup_results = await asyncio.gather(
                *[cleanup_langgraph_task(t) for t in langgraph_tasks], 
                return_exceptions=True
            )
            for i, result in enumerate(cleanup_results):
                if isinstance(result, Exception):
                    logger.error(f"Error cleaning langgraph task {i}: {result}")
        
        # Clean up other tasks
        if other_tasks:
            logger.info(f"Cleaning up {len(other_tasks)} other tasks")
            cleanup_results = await asyncio.gather(
                *[cancel_task_with_cleanup(t) for t in other_tasks], 
                return_exceptions=True
            )
            for i, result in enumerate(cleanup_results):
                if isinstance(result, Exception):
                    logger.error(f"Error cleaning other task {i}: {result}")
        
        # Clean up server tasks last
        if server_tasks:
            logger.info(f"Cleaning up {len(server_tasks)} server tasks")
            cleanup_results = await asyncio.gather(
                *[cancel_task_with_cleanup(t) for t in server_tasks], 
                return_exceptions=True
            )
            for i, result in enumerate(cleanup_results):
                if isinstance(result, Exception):
                    logger.error(f"Error cleaning server task {i}: {result}")
        
        # Give a moment for cleanup to complete
        await asyncio.sleep(0.1)
        
        # Set shutdown event
        shutdown_event.set()
        
        logger.info("Cleanup completed")
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        shutdown_event.set()

async def shutdown(app, signal_=None):
    """Coordinated shutdown function"""
    global is_shutting_down
    if is_shutting_down:
        return
    
    is_shutting_down = True
    if signal_:
        logger.info(f"Received exit signal {signal_.name}")
    
    logger.info("Starting graceful shutdown...")
    
    try:
        # 确保我们有一个事件循环
        loop = asyncio.get_running_loop()
        
        # 设置合理的超时时间
        try:
            await asyncio.wait_for(cleanup(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("Cleanup timed out after 10 seconds")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        
        # 等待所有回调完成
        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Timeout waiting for shutdown event")
        
        # 给剩余任务最后的完成机会
        pending = [t for t in asyncio.all_tasks() if not t.done() and t is not asyncio.current_task()]
        if pending:
            logger.warning(f"{len(pending)} tasks still pending after cleanup")
            
        # 停止接受新的连接
        if hasattr(app, 'state'):
            app.state.should_exit = True
            
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
    finally:
        # 使用事件循环的stop而不是直接退出
        try:
            loop = asyncio.get_running_loop()
            loop.stop()
        except Exception as e:
            logger.error(f"Error stopping event loop: {e}")

def handle_shutdown(signum, frame):
    """Handle graceful shutdown on SIGTERM/SIGINT"""
    if is_shutting_down:
        logger.warning("Received second shutdown signal, forcing exit...")
        sys.exit(1)
    
    logger.info(f"Received shutdown signal {signal.Signals(signum).name}")
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            shutdown_coro = shutdown(app, signal.Signals(signum))
            asyncio.run_coroutine_threadsafe(shutdown_coro, loop)
        else:
            loop.run_until_complete(shutdown(app, signal.Signals(signum)))
    except Exception as e:
        logger.error(f"Error initiating shutdown: {e}")
        sys.exit(1)

# Register signal handlers
for sig in (signal.SIGTERM, signal.SIGINT):
    signal.signal(sig, handle_shutdown)

class CustomServer(uvicorn.Server):
    """Custom server class with enhanced shutdown handling"""
    
    async def shutdown(self, sockets=None):
        """Enhanced shutdown process"""
        logger.info("Starting server shutdown...")
        
        # First run the standard shutdown
        await super().shutdown(sockets)
        
        # Then run our custom shutdown
        await shutdown(self.config.app)

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
        
        # 配置服务器
        config = uvicorn.Config(
            "src.server:app",
            host=args.host,
            port=args.port,
            reload=reload,
            log_level=args.log_level,
        )
        
        server = CustomServer(config)
        server.run()
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        sys.exit(1)
