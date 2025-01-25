from flask import Flask, request, jsonify
from utils.metadata_manager import MetadataManager

# Initialize Flask app and MetadataManager
app = Flask(__name__)
metadata_manager = MetadataManager()

# API Routes

@app.route("/add_file_metadata", methods=["POST"])
def add_file_metadata():
    """API to add file metadata."""
    try:
        data = request.json
        metadata_manager.add_file(data["file_name"], data["chunk_ids"], data["size"])
        return jsonify({"status": "success", "message": "File metadata added."}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/add_chunk_metadata", methods=["POST"])
def add_chunk_metadata():
    """API to add chunk metadata."""
    try:
        data = request.json
        metadata_manager.add_chunk(data["chunk_id"], data["replicas"], data["size"])
        return jsonify({"status": "success", "message": "Chunk metadata added."}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/add_server_metadata", methods=["POST"])
def add_server_metadata():
    """API to add server metadata."""
    try:
        data = request.json
        metadata_manager.add_server(data["server_id"], data["status"], data["load"])
        return jsonify({"status": "success", "message": "Server metadata added."}), 201
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get_file_metadata/<file_name>", methods=["GET"])
def get_file_metadata(file_name):
    """API to retrieve file metadata."""
    try:
        metadata = metadata_manager.get_file_metadata(file_name)
        if metadata:
            return jsonify(metadata), 200
        else:
            return jsonify({"status": "error", "message": "File not found."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get_chunk_metadata/<chunk_id>", methods=["GET"])
def get_chunk_metadata(chunk_id):
    """API to retrieve chunk metadata."""
    try:
        metadata = metadata_manager.get_chunk_metadata(chunk_id)
        if metadata:
            return jsonify(metadata), 200
        else:
            return jsonify({"status": "error", "message": "Chunk not found."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/get_server_metadata/<server_id>", methods=["GET"])
def get_server_metadata(server_id):
    """API to retrieve server metadata."""
    try:
        metadata = metadata_manager.get_server_metadata(server_id)
        if metadata:
            return jsonify(metadata), 200
        else:
            return jsonify({"status": "error", "message": "Server not found."}), 404
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/perform_health_check", methods=["GET"])
def perform_health_check():
    """API to perform health checks on servers."""
    try:
        metadata_manager.perform_health_check()
        return jsonify({"status": "success", "message": "Health checks completed."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/delete_file_metadata/<file_name>", methods=["DELETE"])
def delete_file_metadata(file_name):
    """API to delete file metadata."""
    try:
        metadata_manager.connection.execute("DELETE FROM files WHERE file_name = ?", (file_name,))
        metadata_manager.connection.commit()
        return jsonify({"status": "success", "message": f"File {file_name} deleted."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/delete_chunk_metadata/<chunk_id>", methods=["DELETE"])
def delete_chunk_metadata(chunk_id):
    """API to delete chunk metadata."""
    try:
        metadata_manager.connection.execute("DELETE FROM chunks WHERE chunk_id = ?", (chunk_id,))
        metadata_manager.connection.commit()
        return jsonify({"status": "success", "message": f"Chunk {chunk_id} deleted."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/delete_server_metadata/<server_id>", methods=["DELETE"])
def delete_server_metadata(server_id):
    """API to delete server metadata."""
    try:
        metadata_manager.connection.execute("DELETE FROM servers WHERE server_id = ?", (server_id,))
        metadata_manager.connection.commit()
        return jsonify({"status": "success", "message": f"Server {server_id} deleted."}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# Run the Flask app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
