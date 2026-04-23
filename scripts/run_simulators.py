"""Run all platform simulators in one go."""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SIMULATORS_DIR = PROJECT_ROOT / "simulators"

SIMULATORS = [
    ("shopify", "gen_shopify.py", 200),
    ("mercadolibre", "gen_mercadolibre.py", 150),
    ("amazon", "gen_amazon.py", 180),
    ("tiendanube", "gen_tiendanube.py", 120),
    ("pos", "gen_pos.py", 250),
]


def main():
    for name, script, n in SIMULATORS:
        print(f"\n--- Running {name} simulator ---")
        result = subprocess.run(
            [sys.executable, str(SIMULATORS_DIR / script), str(n)],
            cwd=PROJECT_ROOT,
        )
        if result.returncode != 0:
            print(f"[!] {name} failed")
            sys.exit(result.returncode)
    print("\n[OK] All simulators completed.")


if __name__ == "__main__":
    main()
