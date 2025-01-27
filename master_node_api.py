from flask import Flask, request, jsonify, send_file
import requests
import os
from utils.metadata_manager import MetadataManager
from utils.file_partitioning_node import partition_file  # Your file partitioning logic

app = Flask(__name__)
metadata_manager = MetadataManager()

# Worker nodes (dummy configuration for now)
worker_nodes = [
    {"id": "worker1", "url": "http://127.0.0.1:5003"},
    {"id": "worker2", "url": "http://127.0.0.1:5004"}
]

def get_least_loaded_worker():
    """Select the least loaded worker node."""
    # For simplicity, select the first worker. Enhance with dynamic load checks later.
    return worker_nodes[0]["url"]

@app.get("/")
def hello_world():
    return jsonify({"status": "success", "message": "Hello World!"}), 200

@app.route("/upload", methods=["POST"])
def upload_file():
    """Handle file uploads."""
    try:
        # Get file from request
        file = request.files["file"]
        file_name = file.filename
        file_size = len(file.read())
        file.seek(0)  # Reset file pointer
        
        output_dir = request.args.get('output_dir')
        chunk_size = request.args.get('chunk_size', default=40 * 1024 * 1024, type=int)

        # Partition file
        chunks = partition_file(file_name, output_dir, chunk_size)
        chunk_id_list = []
        # Distribute chunks to workers
        for chunk_id, chunk in chunks.items():
            worker_url = get_least_loaded_worker()
            response = requests.post(
                url=f"{worker_url}/store_chunk",
                data=chunk,
                params={"chunk_id": chunk_id}
            )
            if response.status_code != 200:
                raise Exception(f"Failed to upload chunk {chunk_id} to {worker_url}.")
            chunk_id_list.append(chunk_id)

        # Update metadata
        metadata_manager.add_file(file_name, chunk_id_list, file_size)

        return jsonify({"status": "success", "message": f"File {file_name} uploaded."}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/download/<file_name>", methods=["GET"])
def download_file(file_name):
    """Provide chunk locations for downloading a file."""
    try:
        metadata = metadata_manager.get_file_metadata(file_name)
        if not metadata:
            return jsonify({"status": "error", "message": "File not found."}), 404
        
        if os.path.isfile(f"./downloadedFile/{file_name}"):
          os.remove(f"./downloadedFile/{file_name}")

        os.makedirs("./downloadedFile", exist_ok=True)
        worker_url = get_least_loaded_worker()
        for chunk_id in metadata["chunk_ids"]:
            response = requests.get(f"{worker_url}/get_chunk/{chunk_id}", stream=True)
            with open(f"./downloadedFile/{file_name}", "ab") as fullFile:
                for chunk in response.iter_content(chunk_size=40 * 1024 * 1024):
                    fullFile.write(chunk)

        return send_file(f"./downloadedFile/{file_name}", as_attachment=True)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/delete/<file_name>", methods=["DELETE"])
def delete_file(file_name):
    """Delete a file from metadata and workers."""
    try:
        metadata = metadata_manager.get_file_metadata(file_name)
        if not metadata:
            return jsonify({"status": "error", "message": "File not found."}), 404

        for chunk_id in metadata["chunk_ids"]:
            worker_url = get_least_loaded_worker()
            requests.delete(f"{worker_url}/delete_chunk/{chunk_id}")

        metadata_manager.connection.execute("DELETE FROM files WHERE file_name = ?", (file_name,))
        metadata_manager.connection.commit()

        return jsonify({"status": "success", "message": f"File {file_name} deleted."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/health_check", methods=["GET"])
def health_check():
    """Perform health checks for worker nodes."""
    results = {}
    for worker in worker_nodes:
        try:
            response = requests.get(f"{worker['url']}/health_check")
            results[worker["id"]] = response.json()
        except Exception as e:
            results[worker["id"]] = {"status": "unreachable", "message": str(e)}

    return jsonify(results), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5002)
