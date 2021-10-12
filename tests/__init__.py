# -*- coding: utf-8 -*-
from __future__ import unicode_literals, division, print_function, absolute_import

from caches.compat import *
import caches
import caches.interface

import testdata
from testdata import TestCase as BaseTestCase


testdata.basic_logging()


class TestCase(BaseTestCase):
    @classmethod
    def setUpClass(cls):
        """
        http://docs.python.org/2/library/unittest.html#setupmodule-and-teardownmodule
        """
        for interface_name, i in caches.get_interfaces().items():
            i.unsafe_flush()
            pass

        caches.interface.interfaces = {}
        caches.configure_environ()

