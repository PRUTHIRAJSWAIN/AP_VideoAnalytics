import os
from pathlib import Path

# --- CONFIGURATION ---
# Folders to completely ignore (os.walk will not descend into these)
IGNORE_DIRS = {'.git', 'node_modules', '__pycache__', 'postgres_data', 'ThingsToRunForDB', '.idea', '.vscode'}

# Specific paths where all file content should be skipped (e.g., image folders)
# Use relative path strings (e.g., 'shared/frames', 'data/logs')
SKIP_DETAILS = {'shared/frames', 'assets/images'}

# Individual filenames to ignore globally
IGNORE_FILES = {'extractor.py', 'project_report.txt', '.DS_Store'}

OUTPUT_FILE = 'project_report.txt'

def is_binary(file_path: Path) -> bool:
    """Checks for null bytes (b'\x00') to determine if a file is binary."""
    try:
        # Read the start of the file in binary mode
        with open(file_path, 'rb') as f:
            chunk = f.read(1024)
            return b'\x00' in chunk
    except Exception:
        # Assume true if we cannot even read the file
        return True

def generate_report(root_path: Path) -> str:
    """Generates a report with folder structure and file content."""
    report = []
    
    for dirpath, dirnames, filenames in os.walk(root_path):
        rel_dir = Path(dirpath).relative_to(root_path)
        rel_dir_str = str(rel_dir).replace('\\', '/')

        # 1. Check if the current directory is inside a path we need to skip (e.g., 'shared/frames/plantA')
        is_skipped_path = any(
            # Checks if the skip path is a substring of the current path
            skip_path in rel_dir_str 
            for skip_path in SKIP_DETAILS
        )

        if is_skipped_path:
            # Prune traversal: stop walking into any subdirectories of the current path
            dirnames[:] = [] 
            # Prune files: stop processing any files in the current directory
            filenames[:] = [] 
            
            # Log the skip only for the top-level skipped folder to keep the report concise
            if rel_dir_str in SKIP_DETAILS:
                 report.append(f"\n[Content skipped: {rel_dir_str}]")
            continue 
        
        # 2. Prune globally ignored directories (if they are not already pruned by SKIP_DETAILS)
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]

        for filename in filenames:
            if filename in IGNORE_FILES:
                continue

            file_path = Path(dirpath) / filename
            rel_file = file_path.relative_to(root_path)

            # Check if file is binary/unreadable and skip printing the entry entirely
            if is_binary(file_path):
                continue 
            
            # --- START PRINTING FILE DETAILS ---
            report.append(f"\n{'='*50}")
            report.append(f"FILE: {rel_file}")
            report.append(f"{'='*50}\n")

            try:
                # Use errors='ignore' to handle minor encoding issues in text files gracefully
                content = file_path.read_text(encoding='utf-8', errors='ignore')
                report.append(content)
            except Exception as e:
                # Should be rare after the is_binary check, but good for safety
                report.append(f"[Error reading text file: {e}]")

    return '\n'.join(report)

def main():
    root_folder = Path.cwd()
    output_path = root_folder / OUTPUT_FILE

    if not root_folder.exists():
        print(f"Error: The folder '{root_folder}' does not exist.")
        return

    print('Starting report generation...')
    report = generate_report(root_folder)
    
    # Save report
    try:
        output_path.write_text(report, encoding='utf-8')
        print(f"Report saved to {output_path}")
    except Exception as e:
        print(f"Error saving report: {e}")

if __name__ == "__main__":
    main()