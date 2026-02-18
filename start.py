import os
import subprocess
import sys

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
    
    try:
        # Run the app
        subprocess.run([python_exe, app_path])
    except KeyboardInterrupt:
        print("\n\n👋 Application stopped by user.")
    except Exception as e:
        print(f"\n❌ Error starting application: {e}")

if __name__ == "__main__":
    start_app()
