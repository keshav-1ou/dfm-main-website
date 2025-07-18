import os
from collections import defaultdict

def count_file_extensions(base_dir):
    extension_counts = defaultdict(int)

    for root, _, files in os.walk(base_dir):
        for file in files:
            _, ext = os.path.splitext(file)
            ext = ext.lower().strip()
            if ext:
                extension_counts[ext] += 1

    if not extension_counts:
        print("❌ No files with extensions found.")
        return

    print(f"\n📄 File extension counts in: {base_dir}\n")
    for ext, count in sorted(extension_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"{ext:10} : {count} file(s)")
    
    print(f"\n🔢 Total different file types: {len(extension_counts)}")
    print(f"📁 Total files counted: {sum(extension_counts.values())}")

# Example usage
if __name__ == "__main__":
    base_folder = r"C:\Temp\downloaded_files"  # Change this to your folder path
    count_file_extensions(base_folder)
