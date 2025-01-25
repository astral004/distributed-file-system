from flask import Flask, request, jsonify, send_file
import os
import time

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = None
app.config["MAX_FORM_MEMORY_SIZE"] = None
# Directory to store file chunks
CHUNK_STORAGE_PATH = "./chunks"
os.makedirs(CHUNK_STORAGE_PATH, exist_ok=True)

START_TIME = time.time()

@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """Store a file chunk received from the Master Node."""
    try:
        chunk = request.files["file"]
        chunk_id = request.args.get("chunk_id")
        print('reached')
        chunk_path = os.path.join(CHUNK_STORAGE_PATH, chunk_id)
        data = chunk.read()
        with open(chunk_path, "wb") as f:
            print('reached')
            f.write(data)

        return jsonify({"status": "success", "message": f"Chunk {chunk_id} stored successfully."}), 200
    except Exception as e:
        print(e)
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/get_chunk/<chunk_id>", methods=["GET"])
def get_chunk(chunk_id):
    """Retrieve a file chunk."""
    try:
        chunk_path = os.path.join(CHUNK_STORAGE_PATH, chunk_id)
        if os.path.exists(chunk_path):
            return send_file(chunk_path, as_attachment=True)
        else:
            return jsonify({"status": "error", "message": "Chunk not found."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/delete_chunk/<chunk_id>", methods=["DELETE"])
def delete_chunk(chunk_id):
    """Delete a file chunk."""
    try:
        chunk_path = os.path.join(CHUNK_STORAGE_PATH, chunk_id)
        if os.path.exists(chunk_path):
            os.remove(chunk_path)
            return jsonify({"status": "success", "message": f"Chunk {chunk_id} deleted successfully."}), 200
        else:
            return jsonify({"status": "error", "message": "Chunk not found."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/health_check", methods=["GET"])
def health_check():
    """Perform a health check for the worker node."""
    try:
        uptime = time.time() - START_TIME
        free_space = get_free_space()

        return jsonify({
            "status": "healthy",
            "uptime_seconds": round(uptime, 2),
            "free_space_mb": free_space
        }), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

def get_free_space():
    """Get available disk space in MB."""
    stat = os.statvfs(CHUNK_STORAGE_PATH)
    free_space = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)  # Convert bytes to MB
    return round(free_space, 2)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5003)
