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
    chunk_list = dict()

    print(f"Partitioning file: {input_file}")
    with open(input_file, 'rb') as file:
        while chunk := file.read(chunk_size):
            chunk_filename = f"{base_name}_chunk_{chunk_index}"
            chunk_list[chunk_filename] = chunk
            # chunk_path = os.path.join(output_dir, chunk_filename)
            # with open(chunk_path, 'wb') as chunk_file:
            #     chunk_file.write(chunk)
            
            # print(f"Created chunk: {chunk_path} ({len(chunk)} bytes)")
            # chunk_list.append(chunk_filename)
            chunk_index += 1
    
    print(f"File successfully partitioned into {chunk_index} chunks.")
    return chunk_list

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