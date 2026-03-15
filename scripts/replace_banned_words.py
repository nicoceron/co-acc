import os
import re

directories = ['api/src', 'etl/src', 'frontend/src']

replacements = {
    'suspicious': 'prioritized',
    'Suspicious': 'Prioritized',
    'SUSPICIOUS': 'PRIORITIZED',
    'corruption': 'integrity',
    'Corruption': 'Integrity',
    'CORRUPTION': 'INTEGRITY'
}

# Sort keys by length in descending order to avoid partial replacement issues
sorted_keys = sorted(replacements.keys(), key=len, reverse=True)

for directory in directories:
    if not os.path.exists(directory):
        print(f"Skipping {directory} as it does not exist.")
        continue
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                new_content = content
                for key in sorted_keys:
                    new_content = new_content.replace(key, replacements[key])
                
                if new_content != content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    print(f"Updated {file_path}")
            except (UnicodeDecodeError, PermissionError):
                # Skip files that can't be decoded (e.g., binaries) or read
                continue
