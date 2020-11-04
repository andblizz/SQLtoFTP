import subprocess
import sys

while True:
    process = subprocess.Popen([sys.executable, "main.py"])
    process.wait()
