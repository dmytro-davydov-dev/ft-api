"""Supabase PostgreSQL client for ft-api drone module.

Thin wrapper around the supabase-py library. Use get_supabase_client()
to obtain a client instance — this function is mockable in tests.
"""
import os

from supabase import Client, create_client


def get_supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    return create_client(url, key)
