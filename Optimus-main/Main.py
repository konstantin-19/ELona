import subprocess
import time

if __name__ == "__main__":
    print("="*80)
    print("🚀 STARTING MAIN ORCHESTRATOR")
    print("="*80)

    print("\n📌 Starting Filter.py in background...")
    print("📌 Filter.py will scan ALL symbols and monitor via WebSocket")
    print("📌 It will auto-update oversold_symbols.json when symbols become oversold\n")

    filter_process = subprocess.Popen(['python', 'Filter.py'])

    print("⏳ Waiting 30 seconds for Filter to:")
    print("   - Scan all symbols for oversold conditions")
    print("   - Create oversold_symbols.json")
    print("   - Start WebSocket monitoring\n")

    time.sleep(10)

    print("="*80)
    print("📌 Starting Indicators.py...")
    print("📌 Indicators.py will monitor oversold symbols with EMA 9/21 calculations")
    print("="*80 + "\n")

    # Start Indicators.py (this blocks and runs forever)
    try:
        subprocess.run(['python', 'Indicators.py'])
    except KeyboardInterrupt:
        print("\n\n⛔ Shutting down gracefully...")
        print("Terminating Filter.py...")
        filter_process.terminate()
        filter_process.wait()
        print("="*80)
        print("✅ All processes stopped")
        print("="*80)
