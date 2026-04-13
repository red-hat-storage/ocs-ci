#!/usr/bin/env python3
"""
ODF Must-Gather Health Analyzer - Module Entry Point
Allows running as: python -m must_gather_report_generator
"""

import sys
from .main import main

if __name__ == "__main__":
    sys.exit(main())
