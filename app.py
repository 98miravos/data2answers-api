from flask import Flask, request, jsonify
import duckdb
import os

app = Flask(__name__)

DB_PATH = os.environ.get("DB_PATH", "restaurant.db")

@app.route("/query", methods=["POST"])
def query():
    data = request.get_json()
    sql = data.get("sql", "")
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        result = con.execute(sql).fetchdf()
        con.close()
        return jsonify(result.to_dict(orient="records"))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
