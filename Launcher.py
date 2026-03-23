import subprocess
import time
import json
import sys
import os

# --- CONFIG ---
CONFIG_FILE = "system_config.json"
MANAGER_SCRIPT = "Trade_Manager.py"
DASHBOARD_SCRIPT = "Dashboard.py"
BRAIN_SCRIPT = os.path.join("ML_Pipeline", "ML_Brain.py") # <--- THE NEW BRAIN

def load_config():
    if not os.path.exists(CONFIG_FILE):
        print(f"Error: {CONFIG_FILE} not found.")
        sys.exit(1)
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

def launch_dashboard():
    print("Launcher: 🚀 Starting Dashboard UI...")
    CREATE_NEW_CONSOLE = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
    cmd = [
        "streamlit", "run", DASHBOARD_SCRIPT,
        "--server.address=0.0.0.0",
        "--server.port=8501",
        "--theme.base=dark",
        "--server.headless=true",
        "--global.developmentMode=false"
    ]
    return subprocess.Popen(cmd, creationflags=CREATE_NEW_CONSOLE)

def main():
    print("--- ALGOTRADING SYSTEM LAUNCHER ---")
    
    try:
        config = load_config()
    except Exception as e:
        print(f"CRITICAL: system_config.json error!\n{e}")
        input("Press Enter to exit...")
        return

    processes = []

    # 1. Start Trade Manager (MT5 Execution)
    if os.path.exists(MANAGER_SCRIPT):
        print("Launcher: Starting Trade Manager...")
        CREATE_NEW_CONSOLE = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
        manager_proc = subprocess.Popen(["cmd", "/k", "python", MANAGER_SCRIPT], creationflags=CREATE_NEW_CONSOLE)
        processes.append(manager_proc)
    else:
        print(f"CRITICAL: {MANAGER_SCRIPT} not found.")
        return

    # 2. Start ML Brain (Quantower Listener & Router)
    if os.path.exists(BRAIN_SCRIPT):
        print("Launcher: Starting ML Brain...")
        CREATE_NEW_CONSOLE = subprocess.CREATE_NEW_CONSOLE if os.name == 'nt' else 0
        # cmd /k keeps the window open so you can see the Quantower signals arriving!
        brain_proc = subprocess.Popen(["cmd", "/k", "python", BRAIN_SCRIPT], creationflags=CREATE_NEW_CONSOLE)
        processes.append(brain_proc)
    else:
        print(f"CRITICAL: {BRAIN_SCRIPT} not found.")
        return

    # 3. Start Dashboard (UI)
    dash_proc = None
    if os.path.exists(DASHBOARD_SCRIPT):
        dash_proc = launch_dashboard()
        if dash_proc:
            print("Launcher: Dashboard running on http://localhost:8501")

    print(f"\n--- SYSTEM RUNNING: {len(processes)} Processes Active ---")
    print("Keep this window open. Press Ctrl+C to kill all bots.")

    # 4. Monitor Loop
    try:
        while True:
            time.sleep(2)
            if manager_proc.poll() is not None:
                print("CRITICAL: Trade Manager died! Shutting down system.")
                break
            if brain_proc.poll() is not None:
                print("CRITICAL: ML Brain died! Shutting down system.")
                break
            if dash_proc is not None and dash_proc.poll() is not None:
                print("⚠️ WARNING: Dashboard crashed. Restarting...")
                dash_proc = launch_dashboard()

    except KeyboardInterrupt:
        print("\nLauncher: Stopping all processes...")

    # Cleanup
    if dash_proc and dash_proc not in processes: 
        processes.append(dash_proc)
        
    for p in processes:
        if p.poll() is None:
            try: 
                if os.name == 'nt':
                    # /F = Force Kill, /T = Kill child processes (Tree Kill)
                    subprocess.call(['taskkill', '/F', '/T', '/PID', str(p.pid)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                else:
                    p.terminate()
            except Exception as e: 
                print(f"Cleanup error: {e}")
            
    print("Launcher: System Shutdown Complete.")

if __name__ == "__main__":
    main()