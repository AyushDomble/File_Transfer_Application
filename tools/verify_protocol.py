"""Run the targeted protocol contract suite; --memory-transport is explicit.

This is a subset of pytest, not a replacement for native app integration.
"""
from pathlib import Path
import os
import sys
import pytest

SUITE = [f'tests/{name}.py' for name in (
    'test_transfer_manager', 'test_session_protocol', 'test_scheduler',
    'test_chunk_ack_stress', 'test_pause_resume', 'test_pause_flow_race',
    'test_flow_control', 'test_retry_and_persistence', 'test_reconnection',
    'test_receiver_restart', 'test_bulk_actions', 'test_transfer_contracts',
    'test_integrity_retry_scheduler', 'test_flow_persistence_recovery', 'test_api_event_contracts',
    'test_recovery_event_consistency')]
SUITE += [f'tests/test_transfer_events.py::{name}' for name in (
    'test_event_name_for_creation_vs_requeue_vs_start_vs_resume',
    'test_manager_fires_created_event_exactly_once_at_creation',
    'test_listener_registered_at_creation_time_still_sees_it',
    'test_listener_does_not_deadlock_the_transition_it_observes')]

if __name__ == '__main__':
    project = Path(__file__).resolve().parents[1]
    os.chdir(project)
    sys.path.insert(0, str(project))
    print('Contract suite mode:', 'IN-MEMORY (not native TCP)' if '--memory-transport' in sys.argv else 'NATIVE SOCKETPAIR', flush=True)
    raise SystemExit(pytest.main(['-q', *SUITE, *sys.argv[1:]]))
