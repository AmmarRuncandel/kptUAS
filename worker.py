"""
Shortcut entry point: python worker.py
Menjalankan Worker Node. Otomatis menemukan IP Master di jaringan LAN via UDP Broadcast.
"""
import sys
import os

if __name__ == "__main__":
    _root = os.path.dirname(os.path.abspath(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)
    from worker.worker import main
    main()
