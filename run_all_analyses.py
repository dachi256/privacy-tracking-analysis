import subprocess
import time
import os
import sys

def run_script(script_name, description):
    """Run a Python script and display its output."""
    print("=" * 80)
    print(f"Running {description} ({script_name})...")
    print("=" * 80)
    
    # Set environment variable to ignore deprecation warnings
    env = os.environ.copy()
    env["PYTHONWARNINGS"] = "ignore::DeprecationWarning,ignore::FutureWarning"
    
    # Run the script with warnings suppressed
    process = subprocess.Popen(['python', '-W', 'ignore', script_name], 
                              stdout=subprocess.PIPE, 
                              stderr=subprocess.PIPE,
                              text=True,
                              env=env)
    
    # Stream stdout in real-time
    while True:
        output = process.stdout.readline()
        if output == '' and process.poll() is not None:
            break
        if output:
            print(output.strip())
    
    # Only show stderr if it contains actual errors (not INFO logs)
    stderr = process.stderr.read()
    if stderr and not stderr.strip().startswith(('INFO', '2025')):
        print("\nERROR OUTPUT:")
        print(stderr)
    
    # Make sure the process exited cleanly
    return_code = process.poll()
    if return_code != 0:
        print(f"\nWARNING: Script exited with non-zero status: {return_code}")
    
    print("\n")
    time.sleep(1)  # Brief pause between scripts

def main():
    """Run all analysis scripts from A to F."""
    print("Starting Web Privacy Analysis Pipeline")
    print("This script will run all analysis tasks (A through F)")
    print("\n")
    
    # List of scripts to run with descriptions
    scripts = [
        ("question_a.py", "Question A: Crawl Status Analysis"),
        ("question_b.py", "Question B: Third-Party Analysis"),
        ("question_c.py", "Question C: JavaScript Cookie Analysis"),
        ("question_d.py", "Question D: HTTP Cookie Analysis"),
        ("question_e.py", "Question E: Cookie Sync Analysis"),
        ("question_f.py", "Question F: Fingerprinting API Analysis")
    ]
    
    # Verify all scripts exist
    missing_scripts = []
    for script, _ in scripts:
        if not os.path.exists(script):
            missing_scripts.append(script)
    
    if missing_scripts:
        print("ERROR: The following scripts are missing:")
        for script in missing_scripts:
            print(f"  - {script}")
        print("\nPlease ensure all script files are in the current directory.")
        return
    
    # Run each script in sequence
    all_succeeded = True
    for script, description in scripts:
        try:
            run_script(script, description)
        except Exception as e:
            print(f"ERROR running {script}: {e}")
            all_succeeded = False
    
    if all_succeeded:
        print("All analyses completed successfully!")
        print("Generated plots:")
        print("  - third_party_distribution.png (Question B)")
        print("  - cookie_sync_distribution.png (Question E)")
    else:
        print("Some analyses encountered errors. Please check the output above.")

if __name__ == "__main__":
    main()