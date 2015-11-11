#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
dummy.py: First and dummy WiSHFUL controller

Usage:
   dummy.py [options] [-q | -v]
   dummy.py --config

Options:
   -f                  foo

Other options:
   -h, --help          show this help message and exit
   -q, --quiet         print less text
   -v, --verbose       print more text
   --version           show version and exit
"""

__author__ = "Mikolaj Chwalisz"
__copyright__ = "Copyright (c) 2015, Technische Universität Berlin"
__version__ = "0.1.0"
__email__ = "chwalisz@tkn.tu-berlin.de"

import logging

def main(args):
    """Run the code for dummy"""
    log = logging.getLogger('dummy.main')
    log.debug(args)
# def main



if __name__ == "__main__":
    try:
        from docopt import docopt
    except:
        print("""
        Please install docopt using:
            pip install docopt==0.6.1
        For more refer to:
        https://github.com/docopt/docopt
        """)
        raise

    args = docopt(__doc__, version=__version__)

    log_level = logging.INFO  # default
    if args['--verbose']:
        log_level = logging.DEBUG
    elif args['--quiet']:
        log_level = logging.ERROR
    logging.basicConfig(level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main(args)
