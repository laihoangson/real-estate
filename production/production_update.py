"""
Weekly orchestrator: runs clean -> train -> predict in sequence.

This is the entry point called by GitHub Actions every week after the scraper
updates data/melbourne_price_data_enriched.csv.

Usage:
    python production/production_update.py
"""

import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
import config as cfg


# ============================================================
# LOGGING
# ============================================================

def setup_logger():
    cfg.ensure_dirs()
    log_path = cfg.LOG_DIR / "weekly_update.log"
    logger = logging.getLogger("weekly_update")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh); logger.addHandler(sh)
    return logger


log = setup_logger()


# ============================================================
# STAGE RUNNER
# ============================================================

def run_stage(name, script_path):
    """Run a stage script as a subprocess. Returns elapsed seconds. Raises on failure."""
    log.info(f">>> Starting stage: {name}")
    t0 = time.time()

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
    )

    elapsed = time.time() - t0

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            log.info(f"  [{name}] {line}")
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            log.warning(f"  [{name}] {line}")

    if result.returncode != 0:
        log.error(f"<<< {name} FAILED after {elapsed:.1f}s (exit code {result.returncode})")
        raise RuntimeError(f"Stage {name} failed")

    log.info(f"<<< {name} completed in {elapsed:.1f}s")
    return elapsed


# ============================================================
# SANITY CHECKS
# ============================================================

def check_input():
    if not cfg.INPUT_CSV.exists():
        raise FileNotFoundError(f"Input CSV not found: {cfg.INPUT_CSV}")
    size_mb = cfg.INPUT_CSV.stat().st_size / 1e6
    log.info(f"Input CSV: {cfg.INPUT_CSV} ({size_mb:.1f} MB)")


def check_outputs():
    expected = [
        cfg.CLEANED_PARQUET,
        cfg.EDA_DECISIONS_JSON,
        cfg.MODEL_POINT_PKL,
        cfg.MODEL_Q10_PKL,
        cfg.MODEL_Q90_PKL,
        cfg.PREPROCESSOR_PKL,
        cfg.METRICS_JSON,
        cfg.PREDICTIONS_PARQUET,
    ]
    missing = [p for p in expected if not p.exists()]
    if missing:
        for p in missing:
            log.error(f"Missing expected output: {p}")
        raise RuntimeError(f"{len(missing)} expected outputs missing")
    log.info(f"All {len(expected)} expected outputs present")


# ============================================================
# MAIN
# ============================================================

def main():
    start = datetime.now()
    log.info("#" * 60)
    log.info(f"WEEKLY UPDATE START at {start:%Y-%m-%d %H:%M:%S}")
    log.info("#" * 60)

    here = Path(__file__).resolve().parent
    stages = [
        ("CLEAN",   here / "clean.py"),
        ("TRAIN",   here / "train_pipeline.py"),
        ("PREDICT", here / "predict.py"),
    ]

    timings = {}
    try:
        check_input()
        for name, script in stages:
            timings[name] = run_stage(name, script)
        check_outputs()
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        log.info("#" * 60)
        log.info(f"WEEKLY UPDATE FAILED at {datetime.now():%Y-%m-%d %H:%M:%S}")
        log.info("#" * 60)
        sys.exit(1)

    end = datetime.now()
    total = (end - start).total_seconds()
    log.info("-" * 60)
    log.info("Stage timings:")
    for name, sec in timings.items():
        log.info(f"  {name:8s} {sec:7.1f}s ({sec/60:.1f} min)")
    log.info(f"  {'TOTAL':8s} {total:7.1f}s ({total/60:.1f} min)")
    log.info("#" * 60)
    log.info(f"WEEKLY UPDATE DONE at {end:%Y-%m-%d %H:%M:%S}")
    log.info("#" * 60)


if __name__ == "__main__":
    main()