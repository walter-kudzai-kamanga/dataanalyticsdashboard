import os
import subprocess
import sys
import socket
from datetime import datetime

DEFAULT_HOST = os.environ.get("DASHBOARD_HOST", "0.0.0.0")
DEFAULT_PORT = int(os.environ.get("DASHBOARD_PORT", "9000"))

def _get_lan_ip():
    """
    Best-effort LAN IP detection for sharing a URL.
    Does not require external connectivity; uses a UDP socket trick.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("1.1.1.1", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        try:
            return socket.gethostbyname(socket.gethostname())
        except Exception:
            return None

def _print_web_access_logs(host: str, port: int):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lan_ip = _get_lan_ip()

    print(f"[{ts}] Web access URLs")
    print(f"  - Local (this machine): http://127.0.0.1:{port}")
    if host in ("0.0.0.0", "::"):
        if lan_ip and lan_ip not in ("127.0.0.1", "0.0.0.0"):
            print(f"  - LAN (other devices): http://{lan_ip}:{port}")
        print("  - Binding: 0.0.0.0 (accessible on your network if firewall allows)")
    else:
        print(f"  - Binding: {host} (LAN access may be blocked by bind address)")
    print()

def start_app():
    """Automates environment activation and starts the dashboard."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    venv_path = os.path.join(base_dir, 'venv')
    
    # Identify the correct python path
    if os.name == 'nt': # Windows
        python_exe = os.path.join(venv_path, 'Scripts', 'python.exe')
    else: # MacOS/Linux
        python_exe = os.path.join(venv_path, 'bin', 'python3')
    
    # Fallback to system python if venv is missing
    if not os.path.exists(python_exe):
        print(f"Warning: Virtual environment not found at {venv_path}")
        print("Starting with system Python...")
        python_exe = sys.executable
    
    app_path = os.path.join(base_dir, 'app.py')
    
    print("\n" + "="*40)
    print("🚀 Starting Data Analytics Dashboard...")
    print("="*40 + "\n")

    # Ensure the app binds consistently with what we print.
    # app.py reads these env vars (defaults match current app.py values).
    os.environ.setdefault("DASHBOARD_HOST", DEFAULT_HOST)
    os.environ.setdefault("DASHBOARD_PORT", str(DEFAULT_PORT))
    # Enable request/access logs when running via start.py
    os.environ.setdefault("DASHBOARD_ACCESS_LOGS", "1")

    _print_web_access_logs(os.environ["DASHBOARD_HOST"], int(os.environ["DASHBOARD_PORT"]))
    
    try:
        # Run the app
        subprocess.run([python_exe, app_path], env=os.environ.copy())
    except KeyboardInterrupt:
        print("\n\n👋 Application stopped by user.")
    except Exception as e:
        print(f"\n❌ Error starting application: {e}")

if __name__ == "__main__":
    start_app()
