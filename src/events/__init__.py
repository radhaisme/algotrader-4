#!/usr/bin/python
# -*- coding: utf-8 -*-
# events.py
"""
@since: 2014-11-12
@author: Javier Garcia
@contact: javier.garcia@bskapital.com
@summary: abstract class for events.
"""


class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass
