"""
Shortcut entry point: python master.py
Menjalankan Master Server dengan parameter default praktis (Auto-Discovery enabled).
"""
import sys
import os

if __name__ == "__main__":
    _root = os.path.dirname(os.path.abspath(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from master.master import main
    main()
