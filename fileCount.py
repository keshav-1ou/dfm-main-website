import os
import hashlib
from collections import defaultdict

DOWNLOAD_DIR = r"C:\Temp\downloaded_files"

def compute_file_hash(file_path):
    """Compute SHA256 hash of a file"""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def analyze_files(base_dir):
    all_files = []
    file_hash_map = defaultdict(list)

    for root, _, files in os.walk(base_dir):
        for file in files:
            full_path = os.path.join(root, file)
            all_files.append(full_path)
            file_hash = compute_file_hash(full_path)
            file_hash_map[file_hash].append(full_path)

    total_files = len(all_files)
    unique_files = len(file_hash_map)
    duplicate_files = sum(len(paths) - 1 for paths in file_hash_map.values() if len(paths) > 1)

    print(f"📊 Total files: {total_files}")
    print(f"✅ Unique files: {unique_files}")
    print(f"⚠️ Duplicate files: {duplicate_files}\n")

    print("🔁 Duplicate groups:")
    group_num = 1
    for hash_val, paths in file_hash_map.items():
        if len(paths) > 1:
            print(f"\nGroup {group_num} (hash: {hash_val}):")
            for p in paths:
                print(f"  - {os.path.relpath(p, base_dir)}")
            group_num += 1


if __name__ == "__main__":
    analyze_files(DOWNLOAD_DIR)
