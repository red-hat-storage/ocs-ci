import socket
import logging
import random
from contextlib import closing

log = logging.getLogger(__name__)


class KrknPortManager:
    """
    Manages port allocation for Krkn server to avoid port conflicts.

    This class provides utilities to find available ports and handle
    port conflicts that can occur when multiple Krkn instances run
    simultaneously or when the default port is already in use.
    """

    DEFAULT_PORT = 8081
    PORT_RANGE_START = 8081
    PORT_RANGE_END = 8181
    MAX_RETRIES = 10

    @classmethod
    def is_port_available(cls, port, host="0.0.0.0"):
        """
        Check if a port is available for binding.

        Args:
            port (int): Port number to check
            host (str): Host address to bind to (default: '0.0.0.0')

        Returns:
            bool: True if port is available, False otherwise
        """
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((host, port))
                return True
        except OSError as e:
            if e.errno == 98:  # Address already in use
                log.debug(f"Port {port} is already in use")
                return False
            else:
                log.warning(f"Error checking port {port}: {e}")
                return False
        except Exception as e:
            log.warning(f"Unexpected error checking port {port}: {e}")
            return False

    @classmethod
    def find_available_port(cls, preferred_port=None, host="0.0.0.0"):
        """
        Find an available port for Krkn server.

        Args:
            preferred_port (int, optional): Preferred port to try first
            host (str): Host address to bind to (default: '0.0.0.0')

        Returns:
            int: Available port number

        Raises:
            RuntimeError: If no available port is found after max retries
        """
        # Try preferred port first (usually the default 8081)
        if preferred_port and cls.is_port_available(preferred_port, host):
            log.info(f"Using preferred port {preferred_port} for Krkn server")
            return preferred_port

        # Try default port if not specified as preferred
        if preferred_port != cls.DEFAULT_PORT and cls.is_port_available(
            cls.DEFAULT_PORT, host
        ):
            log.info(f"Using default port {cls.DEFAULT_PORT} for Krkn server")
            return cls.DEFAULT_PORT

        # Try sequential ports in range
        log.info(
            f"Default port {cls.DEFAULT_PORT} is in use, searching for alternative..."
        )
        for port in range(cls.PORT_RANGE_START, cls.PORT_RANGE_END + 1):
            if port == preferred_port or port == cls.DEFAULT_PORT:
                continue  # Already tried these

            if cls.is_port_available(port, host):
                log.info(f"Found available port {port} for Krkn server")
                return port

        # Try random ports as fallback
        log.warning("No sequential ports available, trying random ports...")
        for attempt in range(cls.MAX_RETRIES):
            port = random.randint(cls.PORT_RANGE_START, cls.PORT_RANGE_END)
            if cls.is_port_available(port, host):
                log.info(
                    f"Found random available port {port} for Krkn server (attempt {attempt + 1})"
                )
                return port

        # Last resort: let the system assign a port
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.bind((host, 0))  # 0 means let the system choose
                port = sock.getsockname()[1]
                log.info(f"Using system-assigned port {port} for Krkn server")
                return port
        except Exception as e:
            log.error(f"Failed to get system-assigned port: {e}")

        raise RuntimeError(
            f"Unable to find available port after {cls.MAX_RETRIES} attempts. "
            f"Tried range {cls.PORT_RANGE_START}-{cls.PORT_RANGE_END} and random selection."
        )

    @classmethod
    def get_port_for_krkn(cls, host="0.0.0.0"):
        """
        Get an available port specifically for Krkn server usage.

        This method tries the default Krkn port first, then falls back
        to finding any available port in the acceptable range.

        Args:
            host (str): Host address to bind to (default: '0.0.0.0')

        Returns:
            int: Available port number for Krkn server
        """
        try:
            port = cls.find_available_port(cls.DEFAULT_PORT, host)

            if port != cls.DEFAULT_PORT:
                log.warning(
                    f"Krkn default port {cls.DEFAULT_PORT} was not available. "
                    f"Using alternative port {port}. "
                    f"This may indicate multiple Krkn instances or port conflicts."
                )

            return port

        except RuntimeError as e:
            log.error(f"Critical port allocation failure: {e}")
            raise

    @classmethod
    def validate_port_range(cls, start_port, end_port):
        """
        Validate that a port range is reasonable for Krkn usage.

        Args:
            start_port (int): Start of port range
            end_port (int): End of port range

        Returns:
            bool: True if range is valid, False otherwise
        """
        if not isinstance(start_port, int) or not isinstance(end_port, int):
            return False

        if start_port < 1024:  # Avoid privileged ports
            log.warning(f"Start port {start_port} is in privileged range (< 1024)")
            return False

        if end_port > 65535:  # Maximum port number
            log.warning(f"End port {end_port} exceeds maximum port number (65535)")
            return False

        if start_port >= end_port:
            log.warning(f"Invalid port range: {start_port} >= {end_port}")
            return False

        if (end_port - start_port) < 10:  # Ensure reasonable range size
            log.warning(f"Port range too small: {end_port - start_port} ports")
            return False

        return True

    @classmethod
    def check_port_conflicts(cls, ports, host="0.0.0.0"):
        """
        Check multiple ports for availability.

        Args:
            ports (list): List of port numbers to check
            host (str): Host address to bind to (default: '0.0.0.0')

        Returns:
            dict: Dictionary with port numbers as keys and availability as values
        """
        results = {}
        for port in ports:
            results[port] = cls.is_port_available(port, host)

        available_count = sum(1 for available in results.values() if available)
        total_count = len(ports)

        log.info(
            f"Port availability check: {available_count}/{total_count} ports available"
        )

        return results
