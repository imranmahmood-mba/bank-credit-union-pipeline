import os
import glob

# Helper functions
def get_latest_file(directory, pattern):
    """
    Get the latest file in a directory that matches the given wildcard pattern.

    Args:
        directory (str): Path to the directory.
        pattern (str): Pattern to match files (e.g., "*.txt" or "report_*").

    Returns:
        str: Path to the latest matching file, or None if no file matches.
    """
    # Construct the full pattern for glob
    search_pattern = os.path.join(directory, pattern)

    # Use glob to find matching files
    matching_files = glob.glob(search_pattern)

    if not matching_files:
        return None  # No files match the pattern

    # Find the latest file based on modification time
    latest_file = max(matching_files, key=os.path.getmtime)
    return latest_file