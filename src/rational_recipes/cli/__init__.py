"""Packaged CLI entry points.

Each module here exposes a public ``run`` (or ``main``) function that the
thin shim under ``scripts/`` invokes. Tests import directly from this
package so they don't need ``sys.path`` manipulation.
"""
