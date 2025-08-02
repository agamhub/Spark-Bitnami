import sys
import os

rerun_mode = True

if rerun_mode:

    print(f"rerun mode = {rerun_mode}")
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'jobs')))
    print(sys.path)