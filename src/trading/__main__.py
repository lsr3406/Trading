"""Allow `python -m trading` as an alternative to the console script."""

from trading.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
