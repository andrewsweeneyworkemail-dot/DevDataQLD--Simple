"""Utility package for Development.i automation scripts."""

# Expose the top-level modules to make ``from scripts import dev_i_pipeline`` work
# consistently when the package is used in GitHub Actions or imported locally.
__all__ = [
    "dev_i_csv_last30",
    "dev_i_pipeline",
]

