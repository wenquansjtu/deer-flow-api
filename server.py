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
from functools import partial
from typing import Any, Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

# Global flag to track shutdown state
is_shutting_down = False
shutdown_event = asyncio.Event()

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
                
                # Try to safely handle any FuturesDict callbacks
                if 'futures_dict' in locals_dict:
                    futures_dict = locals_dict['futures_dict']
                    if hasattr(futures_dict, 'callbacks'):
                        futures_dict.callbacks.clear()
                
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
        
        # First handle langgraph tasks
        langgraph_tasks = [t for t in tasks if any(name in str(t) for name in ['langgraph', 'researcher', 'agent', 'chain'])]
        other_tasks = [t for t in tasks if t not in langgraph_tasks]
        
        if langgraph_tasks:
            logger.info(f"Cleaning up {len(langgraph_tasks)} langgraph tasks")
            await asyncio.gather(*[cleanup_langgraph_task(t) for t in langgraph_tasks], return_exceptions=True)
        
        if other_tasks:
            logger.info(f"Cleaning up {len(other_tasks)} other tasks")
            await asyncio.gather(*[cancel_task_with_cleanup(t) for t in other_tasks], return_exceptions=True)
        
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
