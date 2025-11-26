from pathlib import Path

def read_file(file_path):
    """Reads content of a file, returns content if it's a readable text file."""
    try:
        # Open file with 'utf-8' encoding
        return file_path.read_text(encoding='utf-8')
    except UnicodeDecodeError:
        # If the file is not a text file, return None
        return None
    except Exception as e:
        # Log other errors (like permission issues) and return None
        print(f"Error reading file {file_path}: {e}")
        return None

def generate_report(root_folder):
    """Generates a report with the folder structure and file content."""
    report = []

    # Walk through all folders and files in the root folder
    for path in root_folder.rglob('*'):  # rglob('*') recursively gets all files and directories
        # Get relative path from root_folder
        rel_path = path.relative_to(root_folder)
        
        # Add folder structure (if it's a directory, skip reading its content)
        if path.is_dir():
            report.append(f"\n{rel_path}")
        elif path.is_file():
            file_content = read_file(path)
            
            if file_content:  # If content was successfully read
                report.append(f"\nFile name: {rel_path}")
                report.append(f"Content: {file_content}")
            else:  # If it's a binary or unreadable file, just append its name
                report.append(f"\nFile name: {rel_path}")
                report.append("Content: [Binary/Unreadable file]")

    return '\n'.join(report)

def save_report(report, output_file):
    """Save the generated report to a text file."""
    try:
        output_file.write_text(report, encoding='utf-8')
        print(f"Report saved to {output_file}")
    except Exception as e:
        print(f"Error saving report: {e}")

def main():
    # Get the current working directory (the folder where the script is run from)
    root_folder = Path.cwd()  # Path to the current working directory

    # Output text file to save the report
    output_file = root_folder / 'project_report.txt'

    # Check if the root folder exists
    if not root_folder.exists():
        print(f"Error: The folder '{root_folder}' does not exist.")
        return

    # Generate the report
    report = generate_report(root_folder)

    # Save the report
    save_report(report, output_file)

if __name__ == "__main__":
    print('Starting report generation...')
    main()
