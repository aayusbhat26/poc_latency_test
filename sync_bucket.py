import os
import sys
from huggingface_hub import list_bucket_tree, download_bucket_files

def main():
    print("=== Hugging Face Bucket Downloader ===")
    token = os.environ.get("HF_TOKEN")
    if not token:
        token = input("Enter your Hugging Face Access Token (or paste it here): ").strip()
    
    if not token:
        print("Error: A token is required to download from this bucket.")
        sys.exit(1)
        
    bucket_id = "aayushbhat26/poc_testing_latency"
    local_dir = "./data_lake"
    
    print(f"\nScanning bucket: {bucket_id}...")
    try:
        files = list_bucket_tree(bucket_id, token=token)
        download_list = []
        for file in files:
            # We only want to download files, not directories (though list_bucket_tree returns files usually)
            if not file.path.endswith("/"): 
                local_path = os.path.join(local_dir, file.path)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                download_list.append((file.path, local_path))
                
        print(f"Found {len(download_list)} files. Starting download...")
        download_bucket_files(bucket_id, files=download_list, token=token)
        print("\nDownload complete! The data_lake folder is now populated.")
        print("You can now view your Data Explorer at http://localhost:3000")
        
    except Exception as e:
        print(f"\nAn error occurred: {e}")

if __name__ == "__main__":
    main()
