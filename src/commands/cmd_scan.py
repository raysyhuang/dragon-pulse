"""
CLI Command: Dragon Pulse Scan
"""
from pipelines.dragon_pulse import run_dragon_pulse

def cmd_scan(args):
    """Execute daily Dragon Pulse scan."""
    run_dragon_pulse(
        config_path=args.config,
        asof_date=args.date
    )
    return 0
