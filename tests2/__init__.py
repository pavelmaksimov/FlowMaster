"""
Tests with real interaction with services.
"""

from flowmaster.models import FlowItem, database

database.create_tables([FlowItem])
