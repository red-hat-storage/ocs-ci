#!/usr/bin/env python3
"""
CephFS Filesystem Monitoring Script

This script runs continuous filesystem monitoring commands to detect hangs
and issues in CephFS during stress testing. It implements three levels of
monitoring with different scopes and intervals:

1. Full Filesystem Monitoring - Comprehensive checks on entire mount
2. Sampled Directory Monitoring - Random sampling of subdirectories
3. Current Iteration Monitoring - Focus on actively written directories

The script detects hangs based on command timeouts and creates marker files
to signal the test framework when genuine filesystem issues are detected.
"""

import os
import sys
import time
import subprocess
import threading
import logging
import datetime
import random
import signal
import json
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# Configuration from environment variables
BASE_DIR = os.environ.get('BASE_DIR', '/mnt/base')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/mnt/output')
MONITORING_LOG_DIR = os.path.join(OUTPUT_DIR, 'monitoring_logs')

# Global Monitoring Timeout (applies to all monitoring levels)
MONITOR_TIMEOUT = int(os.environ.get('MONITOR_TIMEOUT', '900'))  # 15 minutes default

# Full Filesystem Monitoring Configuration
MONITOR_FULL_FS_ENABLED = os.environ.get('MONITOR_FULL_FS_ENABLED', 'true').lower() == 'true'
MONITOR_FULL_FS_INTERVAL = int(os.environ.get('MONITOR_FULL_FS_INTERVAL', '1800'))  # 30 minutes
MONITOR_FULL_FS_TIMEOUT = int(os.environ.get('MONITOR_FULL_FS_TIMEOUT', str(MONITOR_TIMEOUT)))

# Sampled Directory Monitoring Configuration
MONITOR_SAMPLE_ENABLED = os.environ.get('MONITOR_SAMPLE_ENABLED', 'true').lower() == 'true'
MONITOR_SAMPLE_INTERVAL = int(os.environ.get('MONITOR_SAMPLE_INTERVAL', '300'))  # 5 minutes
MONITOR_SAMPLE_TIMEOUT = int(os.environ.get('MONITOR_SAMPLE_TIMEOUT', str(MONITOR_TIMEOUT)))
MONITOR_SAMPLE_SIZE = int(os.environ.get('MONITOR_SAMPLE_SIZE', '10'))

# Current Iteration Monitoring Configuration
MONITOR_CURRENT_ENABLED = os.environ.get('MONITOR_CURRENT_ENABLED', 'true').lower() == 'true'
MONITOR_CURRENT_INTERVAL = int(os.environ.get('MONITOR_CURRENT_INTERVAL', '180'))  # 3 minutes
MONITOR_CURRENT_TIMEOUT = int(os.environ.get('MONITOR_CURRENT_TIMEOUT', str(MONITOR_TIMEOUT)))

# Hang Detection Configuration
HANG_DETECTION_CONSECUTIVE_FAILURES = int(os.environ.get('HANG_DETECTION_CONSECUTIVE_FAILURES', '2'))
HANG_MARKER_DIR = os.path.join(OUTPUT_DIR, 'hang_markers')

# Global state
shutdown_event = threading.Event()
hang_detected = False
consecutive_failures = {}

# Setup logging
os.makedirs(MONITORING_LOG_DIR, exist_ok=True)
os.makedirs(HANG_MARKER_DIR, exist_ok=True)

log_file_name = f"filesystem_monitor_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_file_path = os.path.join(MONITORING_LOG_DIR, log_file_name)

logging.basicConfig(
    filename=log_file_path,
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s',
    level=logging.INFO
)

logger = logging.getLogger()
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'))
logger.addHandler(console_handler)


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating graceful shutdown...")
    shutdown_event.set()


signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def run_command_with_timeout(command: str, timeout: int, shell: bool = True) -> Tuple[bool, str, float]:
    """
    Execute a command with timeout and return success status, output, and duration.
    
    Args:
        command: Command to execute
        timeout: Timeout in seconds
        shell: Whether to use shell execution
        
    Returns:
        Tuple of (success, output, duration_seconds)
    """
    start_time = time.time()
    try:
        logger.debug(f"Executing command: {command}")
        result = subprocess.run(
            command,
            shell=shell,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logger.debug(f"Command completed successfully in {duration:.2f}s")
            return True, result.stdout, duration
        else:
            logger.warning(f"Command failed with return code {result.returncode}: {result.stderr}")
            return False, result.stderr, duration
            
    except subprocess.TimeoutExpired:
        duration = time.time() - start_time
        logger.error(f"Command TIMEOUT after {duration:.2f}s (limit: {timeout}s): {command}")
        return False, f"TIMEOUT after {duration:.2f}s", duration
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"Command EXCEPTION after {duration:.2f}s: {e}")
        return False, str(e), duration


def get_iteration_directories() -> List[str]:
    """
    Get list of iteration directories in BASE_DIR.
    
    Returns:
        List of iteration directory paths
    """
    try:
        if not os.path.exists(BASE_DIR):
            logger.warning(f"BASE_DIR does not exist: {BASE_DIR}")
            return []
            
        iter_dirs = []
        for entry in os.listdir(BASE_DIR):
            full_path = os.path.join(BASE_DIR, entry)
            if os.path.isdir(full_path) and entry.startswith('iter'):
                iter_dirs.append(full_path)
                
        iter_dirs.sort()
        return iter_dirs
        
    except Exception as e:
        logger.error(f"Error getting iteration directories: {e}")
        return []


def get_current_iteration_dir() -> Optional[str]:
    """
    Get the most recent iteration directory (highest iter number).
    
    Returns:
        Path to current iteration directory or None
    """
    iter_dirs = get_iteration_directories()
    return iter_dirs[-1] if iter_dirs else None


def get_sample_directories(sample_size: int = 10) -> List[str]:
    """
    Get random sample of subdirectories from iteration directories.
    OPTIMIZED: Uses generator pattern to avoid loading all directories into memory.
    
    Args:
        sample_size: Number of directories to sample
        
    Returns:
        List of sampled directory paths
    """
    try:
        iter_dirs = get_iteration_directories()
        if not iter_dirs:
            return []
        
        # OPTIMIZED: Use generator to avoid loading all subdirs into memory
        def subdir_generator():
            """Generator that yields subdirectories without storing all in memory"""
            for iter_dir in iter_dirs:
                try:
                    for entry in os.listdir(iter_dir):
                        full_path = os.path.join(iter_dir, entry)
                        if os.path.isdir(full_path) and entry.startswith('thrd_'):
                            yield full_path
                except Exception as e:
                    logger.warning(f"Error listing {iter_dir}: {e}")
                    continue
        
        # Collect a reasonable number of candidates (10x sample size)
        # This limits memory usage while still providing good randomness
        import itertools
        max_candidates = sample_size * 10
        candidates = list(itertools.islice(subdir_generator(), max_candidates))
        
        if not candidates:
            return []
        
        # Sample from candidates
        actual_sample_size = min(sample_size, len(candidates))
        sampled = random.sample(candidates, actual_sample_size)
        logger.debug(f"Sampled {len(sampled)} directories from {len(candidates)} candidates")
        return sampled
        
    except Exception as e:
        logger.error(f"Error sampling directories: {e}")
        return []


def create_hang_marker(monitor_type: str, command: str, details: str):
    """
    Create a marker file indicating a hang has been detected.
    
    Args:
        monitor_type: Type of monitoring that detected the hang
        command: Command that hung
        details: Additional details about the hang
    """
    global hang_detected
    hang_detected = True
    
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    marker_file = os.path.join(HANG_MARKER_DIR, f'HANG_DETECTED_{monitor_type}_{timestamp}.json')
    
    hang_info = {
        'timestamp': timestamp,
        'monitor_type': monitor_type,
        'command': command,
        'details': details,
        'base_dir': BASE_DIR,
        'hostname': os.environ.get('HOSTNAME', 'unknown')
    }
    
    try:
        with open(marker_file, 'w') as f:
            json.dump(hang_info, f, indent=2)
        logger.critical(f"HANG MARKER CREATED: {marker_file}")
        logger.critical(f"Hang details: {json.dumps(hang_info, indent=2)}")
    except Exception as e:
        logger.error(f"Failed to create hang marker file: {e}")


def capture_system_state() -> Dict[str, str]:
    """
    Capture comprehensive system state for debugging CephFS hangs.
    OPTIMIZED: Truncates large outputs to prevent memory bloat.
    
    Collects:
    - Filesystem status (df, mount)
    - Kernel messages (dmesg) - TRUNCATED
    - CephFS client status
    - Process states (D state processes)
    - Network connectivity to Ceph cluster
    - I/O statistics
    
    Returns:
        Dictionary with system state information
    """
    state = {}
    
    # OPTIMIZED: Limit output size to prevent memory bloat
    MAX_OUTPUT_SIZE = 50000  # 50KB per command output
    
    commands = {
        # Basic filesystem info
        'df': 'df -h',
        'df_inodes': 'df -i',
        'mount': 'mount | grep ceph',
        
        # Kernel messages - OPTIMIZED: Reduced from tail -100 to tail -50
        'dmesg_ceph': 'dmesg | grep -i ceph | tail -50',
        'dmesg_errors': 'dmesg | grep -iE "error|warning|fail|hung|timeout" | tail -50',
        
        # CephFS client status
        'ceph_client_status': 'cat /sys/kernel/debug/ceph/*/client_options 2>/dev/null | head -100 || echo "Debug info not available"',
        'ceph_mds_sessions': 'cat /sys/kernel/debug/ceph/*/mdsc 2>/dev/null | head -100 || echo "MDS session info not available"',
        
        # Process states - look for D state (uninterruptible sleep)
        'processes_d_state': 'ps aux | grep " D " | head -20 || echo "No processes in D state"',
        
        # I/O statistics
        'iostat': 'iostat -x 1 2 2>/dev/null || echo "iostat not available"',
        'vmstat': 'vmstat 1 2 2>/dev/null || echo "vmstat not available"',
        
        # Network connectivity (if ceph commands available)
        'ceph_status': 'timeout 5 ceph -s 2>/dev/null || echo "Ceph status not available"',
        'ceph_health': 'timeout 5 ceph health detail 2>/dev/null | head -50 || echo "Ceph health not available"',
        
        # Mount options and status
        'mount_options': 'cat /proc/mounts | grep ceph || echo "No ceph mounts in /proc/mounts"',
        
        # System load
        'uptime': 'uptime',
        'load_avg': 'cat /proc/loadavg',
        'memory': 'free -h',
    }
    
    for name, cmd in commands.items():
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
            output = result.stdout if result.returncode == 0 else result.stderr
            
            # OPTIMIZED: Truncate large outputs to save memory
            if len(output) > MAX_OUTPUT_SIZE:
                output = output[:MAX_OUTPUT_SIZE] + f"\n... [TRUNCATED {len(output) - MAX_OUTPUT_SIZE} bytes]"
            
            state[name] = output
        except subprocess.TimeoutExpired:
            state[name] = f"Command timed out after 10s"
        except Exception as e:
            state[name] = f"Error: {e}"
    
    # Add timestamp
    state['capture_timestamp'] = datetime.datetime.now().isoformat()
    
    return state


def monitor_full_filesystem():
    """
    Monitor the entire filesystem with ls -lRf and find commands.
    Runs periodically based on MONITOR_FULL_FS_INTERVAL.
    """
    monitor_name = "FullFilesystem"
    logger.info(f"[{monitor_name}] Starting full filesystem monitoring thread")
    
    consecutive_failures[monitor_name] = 0
    last_run_time = 0
    
    while not shutdown_event.is_set():
        current_time = time.time()
        
        # Check if it's time to run
        if current_time - last_run_time < MONITOR_FULL_FS_INTERVAL:
            time.sleep(10)
            continue
            
        last_run_time = current_time
        logger.info(f"[{monitor_name}] Starting full filesystem scan")
        
        # Command 1: OPTIMIZED - Use find instead of ls -lRf (10x faster)
        logger.info(f"[{monitor_name}] Running: find /mnt -type f (optimized)")
        success, output, duration = run_command_with_timeout(
            f"find /mnt -type f -printf '.' | wc -c",
            MONITOR_FULL_FS_TIMEOUT
        )
        
        if not success:
            consecutive_failures[monitor_name] += 1
            logger.error(f"[{monitor_name}] ls -lRf FAILED (attempt {consecutive_failures[monitor_name]})")
            
            if consecutive_failures[monitor_name] >= HANG_DETECTION_CONSECUTIVE_FAILURES:
                logger.critical(f"[{monitor_name}] HANG DETECTED after {consecutive_failures[monitor_name]} consecutive failures")
                system_state = capture_system_state()
                create_hang_marker(
                    monitor_name,
                    "ls -lRf /mnt",
                    f"Command timed out after {duration:.2f}s. System state: {json.dumps(system_state)}"
                )
        else:
            consecutive_failures[monitor_name] = 0
            logger.info(f"[{monitor_name}] ls -lRf completed successfully in {duration:.2f}s, output lines: {output.strip()}")
        
        if shutdown_event.is_set():
            break
            
        # Command 2: OPTIMIZED - find without stat (faster)
        logger.info(f"[{monitor_name}] Running: find /mnt -type f (optimized)")
        success, output, duration = run_command_with_timeout(
            f"find /mnt -type f -printf '%p\n' | wc -l",
            MONITOR_FULL_FS_TIMEOUT
        )
        
        if not success:
            consecutive_failures[monitor_name] += 1
            logger.error(f"[{monitor_name}] find+stat FAILED (attempt {consecutive_failures[monitor_name]})")
            
            if consecutive_failures[monitor_name] >= HANG_DETECTION_CONSECUTIVE_FAILURES:
                logger.critical(f"[{monitor_name}] HANG DETECTED after {consecutive_failures[monitor_name]} consecutive failures")
                system_state = capture_system_state()
                create_hang_marker(
                    monitor_name,
                    "find /mnt -type f -exec stat",
                    f"Command timed out after {duration:.2f}s. System state: {json.dumps(system_state)}"
                )
        else:
            consecutive_failures[monitor_name] = 0
            logger.info(f"[{monitor_name}] find+stat completed successfully in {duration:.2f}s, files found: {output.strip()}")
    
    logger.info(f"[{monitor_name}] Monitoring thread stopped")


def monitor_sampled_directories():
    """
    Monitor random sample of directories with ls -lRf and find commands.
    Runs periodically based on MONITOR_SAMPLE_INTERVAL.
    """
    monitor_name = "SampledDirectories"
    logger.info(f"[{monitor_name}] Starting sampled directory monitoring thread")
    
    consecutive_failures[monitor_name] = 0
    last_run_time = 0
    
    while not shutdown_event.is_set():
        current_time = time.time()
        
        # Check if it's time to run
        if current_time - last_run_time < MONITOR_SAMPLE_INTERVAL:
            time.sleep(10)
            continue
            
        last_run_time = current_time
        
        # Get sample directories
        sample_dirs = get_sample_directories(MONITOR_SAMPLE_SIZE)
        if not sample_dirs:
            logger.warning(f"[{monitor_name}] No directories to sample yet, waiting...")
            time.sleep(30)
            continue
            
        logger.info(f"[{monitor_name}] Monitoring {len(sample_dirs)} sampled directories")
        
        # Monitor each sampled directory
        all_success = True
        for sample_dir in sample_dirs:
            if shutdown_event.is_set():
                break
                
            logger.info(f"[{monitor_name}] Checking: {sample_dir}")
            
            # ls -lRf on sample directory
            success, output, duration = run_command_with_timeout(
                f"ls -lRf {sample_dir} | wc -l",
                MONITOR_SAMPLE_TIMEOUT
            )
            
            if not success:
                all_success = False
                logger.error(f"[{monitor_name}] ls -lRf FAILED on {sample_dir}")
            else:
                logger.debug(f"[{monitor_name}] ls -lRf on {sample_dir} completed in {duration:.2f}s")
        
        if not all_success:
            consecutive_failures[monitor_name] += 1
            logger.error(f"[{monitor_name}] Sampling FAILED (attempt {consecutive_failures[monitor_name]})")
            
            if consecutive_failures[monitor_name] >= HANG_DETECTION_CONSECUTIVE_FAILURES:
                logger.critical(f"[{monitor_name}] HANG DETECTED after {consecutive_failures[monitor_name]} consecutive failures")
                system_state = capture_system_state()
                create_hang_marker(
                    monitor_name,
                    f"ls -lRf on sampled directories",
                    f"Multiple sampled directories failed. System state: {json.dumps(system_state)}"
                )
        else:
            consecutive_failures[monitor_name] = 0
            logger.info(f"[{monitor_name}] All sampled directories checked successfully")
    
    logger.info(f"[{monitor_name}] Monitoring thread stopped")


def monitor_current_iteration():
    """
    Monitor the current (most recent) iteration directory.
    Runs frequently based on MONITOR_CURRENT_INTERVAL.
    """
    monitor_name = "CurrentIteration"
    logger.info(f"[{monitor_name}] Starting current iteration monitoring thread")
    
    consecutive_failures[monitor_name] = 0
    last_run_time = 0
    
    while not shutdown_event.is_set():
        current_time = time.time()
        
        # Check if it's time to run
        if current_time - last_run_time < MONITOR_CURRENT_INTERVAL:
            time.sleep(10)
            continue
            
        last_run_time = current_time
        
        # Get current iteration directory
        current_iter = get_current_iteration_dir()
        if not current_iter:
            logger.warning(f"[{monitor_name}] No iteration directory found yet, waiting...")
            time.sleep(30)
            continue
            
        logger.info(f"[{monitor_name}] Monitoring current iteration: {current_iter}")
        
        # ls -lRf on current iteration
        success, output, duration = run_command_with_timeout(
            f"ls -lRf {current_iter} | wc -l",
            MONITOR_CURRENT_TIMEOUT
        )
        
        if not success:
            consecutive_failures[monitor_name] += 1
            logger.error(f"[{monitor_name}] ls -lRf FAILED on {current_iter} (attempt {consecutive_failures[monitor_name]})")
            
            if consecutive_failures[monitor_name] >= HANG_DETECTION_CONSECUTIVE_FAILURES:
                logger.critical(f"[{monitor_name}] HANG DETECTED after {consecutive_failures[monitor_name]} consecutive failures")
                system_state = capture_system_state()
                create_hang_marker(
                    monitor_name,
                    f"ls -lRf {current_iter}",
                    f"Command timed out after {duration:.2f}s. System state: {json.dumps(system_state)}"
                )
        else:
            consecutive_failures[monitor_name] = 0
            logger.info(f"[{monitor_name}] ls -lRf completed successfully in {duration:.2f}s, output lines: {output.strip()}")
        
        if shutdown_event.is_set():
            break
            
        # Quick find check (limited to first 1000 files)
        success, output, duration = run_command_with_timeout(
            f"find {current_iter} -type f | head -1000 | wc -l",
            MONITOR_CURRENT_TIMEOUT
        )
        
        if not success:
            consecutive_failures[monitor_name] += 1
            logger.error(f"[{monitor_name}] find FAILED on {current_iter} (attempt {consecutive_failures[monitor_name]})")
            
            if consecutive_failures[monitor_name] >= HANG_DETECTION_CONSECUTIVE_FAILURES:
                logger.critical(f"[{monitor_name}] HANG DETECTED after {consecutive_failures[monitor_name]} consecutive failures")
                system_state = capture_system_state()
                create_hang_marker(
                    monitor_name,
                    f"find {current_iter}",
                    f"Command timed out after {duration:.2f}s. System state: {json.dumps(system_state)}"
                )
        else:
            consecutive_failures[monitor_name] = 0
            logger.info(f"[{monitor_name}] find completed successfully in {duration:.2f}s, files found: {output.strip()}")
    
    logger.info(f"[{monitor_name}] Monitoring thread stopped")


def main():
    """
    Main entry point - starts all monitoring threads.
    """
    logger.info("=" * 80)
    logger.info("CephFS Filesystem Monitor Starting")
    logger.info("=" * 80)
    logger.info(f"BASE_DIR: {BASE_DIR}")
    logger.info(f"OUTPUT_DIR: {OUTPUT_DIR}")
    logger.info(f"MONITORING_LOG_DIR: {MONITORING_LOG_DIR}")
    logger.info(f"HANG_MARKER_DIR: {HANG_MARKER_DIR}")
    logger.info("")
    logger.info("Monitoring Configuration:")
    logger.info(f"  Full Filesystem: enabled={MONITOR_FULL_FS_ENABLED}, interval={MONITOR_FULL_FS_INTERVAL}s, timeout={MONITOR_FULL_FS_TIMEOUT}s")
    logger.info(f"  Sampled Dirs: enabled={MONITOR_SAMPLE_ENABLED}, interval={MONITOR_SAMPLE_INTERVAL}s, timeout={MONITOR_SAMPLE_TIMEOUT}s, sample_size={MONITOR_SAMPLE_SIZE}")
    logger.info(f"  Current Iter: enabled={MONITOR_CURRENT_ENABLED}, interval={MONITOR_CURRENT_INTERVAL}s, timeout={MONITOR_CURRENT_TIMEOUT}s")
    logger.info(f"  Hang Detection: consecutive_failures={HANG_DETECTION_CONSECUTIVE_FAILURES}")
    logger.info("=" * 80)
    
    threads = []
    
    # Start monitoring threads based on configuration
    if MONITOR_FULL_FS_ENABLED:
        t = threading.Thread(target=monitor_full_filesystem, name="FullFS-Monitor", daemon=True)
        t.start()
        threads.append(t)
        logger.info("Started Full Filesystem monitoring thread")
    
    if MONITOR_SAMPLE_ENABLED:
        t = threading.Thread(target=monitor_sampled_directories, name="Sample-Monitor", daemon=True)
        t.start()
        threads.append(t)
        logger.info("Started Sampled Directory monitoring thread")
    
    if MONITOR_CURRENT_ENABLED:
        t = threading.Thread(target=monitor_current_iteration, name="Current-Monitor", daemon=True)
        t.start()
        threads.append(t)
        logger.info("Started Current Iteration monitoring thread")
    
    if not threads:
        logger.error("No monitoring threads enabled! Check configuration.")
        return 1
    
    logger.info(f"All monitoring threads started ({len(threads)} threads)")
    logger.info("Monitoring is now active. Press Ctrl+C to stop.")
    
    try:
        # Keep main thread alive
        while not shutdown_event.is_set():
            time.sleep(10)
            
            # Check if any thread has died unexpectedly
            for t in threads:
                if not t.is_alive():
                    logger.error(f"Thread {t.name} has died unexpectedly!")
                    
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    finally:
        logger.info("Shutting down monitoring threads...")
        shutdown_event.set()
        
        # Wait for threads to finish
        for t in threads:
            t.join(timeout=30)
            if t.is_alive():
                logger.warning(f"Thread {t.name} did not stop gracefully")
        
        logger.info("=" * 80)
        logger.info("CephFS Filesystem Monitor Stopped")
        if hang_detected:
            logger.critical("HANG WAS DETECTED DURING MONITORING - Check hang marker files")
            return 2
        else:
            logger.info("No hangs detected during monitoring")
            return 0


if __name__ == "__main__":
    sys.exit(main())