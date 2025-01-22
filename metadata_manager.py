import sqlite3
import time
from typing import List, Dict, Any


class MetadataManager:
    def __init__(self, db_path: str = "data/metadata.db"):
        self.db_path = db_path
        self.connection = self._connect_to_db()
        self._initialize_tables()

    def _connect_to_db(self):
        """Establish a connection to the SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            return conn
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def _initialize_tables(self):
        """Create necessary tables if they don't already exist."""
        try:
            with self.connection:
                self.connection.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    file_name TEXT PRIMARY KEY,
                    chunk_ids TEXT,
                    size INTEGER,
                    upload_date REAL
                )
                """)
                self.connection.execute("""
                CREATE TABLE IF NOT EXISTS chunks (
                    chunk_id TEXT PRIMARY KEY,
                    replicas TEXT,
                    size INTEGER,
                    last_verified REAL
                )
                """)
                self.connection.execute("""
                CREATE TABLE IF NOT EXISTS servers (
                    server_id TEXT PRIMARY KEY,
                    status TEXT,
                    load INTEGER
                )
                """)
        except sqlite3.Error as e:
            print(f"Error initializing tables: {e}")
            raise

    def add_file(self, file_name: str, chunk_ids: List[str], size: int):
        """Add a file entry to the metadata."""
        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO files (file_name, chunk_ids, size, upload_date)
                    VALUES (?, ?, ?, ?)
                    """,
                    (file_name, ",".join(chunk_ids), size, time.time()),
                )
        except sqlite3.IntegrityError:
            print(f"File {file_name} already exists in metadata.")

    def add_chunk(self, chunk_id: str, replicas: List[str], size: int):
        """Add a chunk entry to the metadata."""
        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO chunks (chunk_id, replicas, size, last_verified)
                    VALUES (?, ?, ?, ?)
                    """,
                    (chunk_id, ",".join(replicas), size, time.time()),
                )
        except sqlite3.IntegrityError:
            print(f"Chunk {chunk_id} already exists in metadata.")

    def add_server(self, server_id: str, status: str, load: int):
        """Add a server entry to the metadata."""
        try:
            with self.connection:
                self.connection.execute(
                    """
                    INSERT INTO servers (server_id, status, load)
                    VALUES (?, ?, ?)
                    """,
                    (server_id, status, load),
                )
        except sqlite3.IntegrityError:
            print(f"Server {server_id} already exists in metadata.")

    def get_file_metadata(self, file_name: str) -> Dict[str, Any]:
        """Retrieve metadata for a given file."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM files WHERE file_name = ?", (file_name,))
        row = cursor.fetchone()
        if row:
            return {
                "file_name": row[0],
                "chunk_ids": row[1].split(","),
                "size": row[2],
                "upload_date": row[3],
            }
        return {}

    def get_chunk_metadata(self, chunk_id: str) -> Dict[str, Any]:
        """Retrieve metadata for a given chunk."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM chunks WHERE chunk_id = ?", (chunk_id,))
        row = cursor.fetchone()
        if row:
            return {
                "chunk_id": row[0],
                "replicas": row[1].split(","),
                "size": row[2],
                "last_verified": row[3],
            }
        return {}

    def get_server_metadata(self, server_id: str) -> Dict[str, Any]:
        """Retrieve metadata for a given server."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM servers WHERE server_id = ?", (server_id,))
        row = cursor.fetchone()
        if row:
            return {
                "server_id": row[0],
                "status": row[1],
                "load": row[2],
            }
        return {}

    def perform_health_check(self):
        """Perform a health check for all servers."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM servers")
        servers = cursor.fetchall()
        for server in servers:
            print(f"Checking server {server[0]}: Status - {server[1]}")

    def close_connection(self):
        """Close the database connection."""
        self.connection.close()


# Example Usage
if __name__ == "__main__":
    metadata_manager = MetadataManager()

#     # Add some sample data
#     metadata_manager.add_file("example.txt", ["chunk1", "chunk2"], 200)
#     metadata_manager.add_chunk("chunk1", ["server1", "server2"], 64)
#     metadata_manager.add_server("server1", "healthy", 10)

#     # Query and print metadata
#     print(metadata_manager.get_file_metadata("example.txt"))
#     print(metadata_manager.get_chunk_metadata("chunk1"))
#     print(metadata_manager.get_server_metadata("server1"))

    # Perform health checks
    metadata_manager.perform_health_check()

    # Close connection
    metadata_manager.close_connection()
