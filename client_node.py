import os

def partition_file(input_file, output_dir, chunk_size=64 * 1024 * 1024):
    """
    Splits a large file into smaller chunks.

    Parameters:
        input_file (str): Path to the input file.
        output_dir (str): Directory where the chunks will be saved.
        chunk_size (int): Size of each chunk in bytes (default: 64 MB).
    """
    if not os.path.isfile(input_file):
        raise FileNotFoundError(f"File not found: {input_file}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Extract file name for chunk naming
    base_name = os.path.basename(input_file)
    chunk_index = 0

    print(f"Partitioning file: {input_file}")
    with open(input_file, 'rb') as file:
        while chunk := file.read(chunk_size):
            chunk_filename = os.path.join(output_dir, f"{base_name}_chunk_{chunk_index}")
            with open(chunk_filename, 'wb') as chunk_file:
                chunk_file.write(chunk)
            
            print(f"Created chunk: {chunk_filename} ({len(chunk)} bytes)")
            chunk_index += 1
    
    print(f"File successfully partitioned into {chunk_index} chunks.")

# Example usage
if __name__ == "__main__":
    # Path to the file to partition
    input_file = "example_large_file.txt"  # Replace with your file path
    # Directory to save file chunks
    output_dir = "./file_chunks"
    # Chunk size in bytes (default: 64 MB)
    chunk_size = 64 * 1024 * 1024  # Modify as needed

    try:
        partition_file(input_file, output_dir, chunk_size)
    except Exception as e:
        print(f"Error: {e}")