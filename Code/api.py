from flask import Flask, jsonify
from pymongo import MongoClient
from bson import ObjectId
import json

app = Flask(__name__)

client = MongoClient("mongodb://localhost:27017")
db = client["traffic"]

# Endpoints
@app.route("/api/stats", methods=["GET"])
def get_stats():
    pipeline = [
        {"$sort": {"time": -1}},
        {"$group": {
            "_id": "$link",
            "last_time": {"$first": "$time"},
            "vcount":    {"$first": "$vcount"},
            "vspeed":    {"$first": "$vspeed"},
            "vmax":      {"$first": "$vmax"},
            "vmin":      {"$first": "$vmin"}
        }},
        {"$sort": {"_id": 1}}
    ]
    results = list(db["stats"].aggregate(pipeline))
    for r in results:
        r["link"] = r.pop("_id")
    return jsonify(results)


@app.route("/api/stats/<link>", methods=["GET"])
def get_stats_for_link(link):
    #Επιστρέφει όλα τα στατιστικά για μια συγκεκριμένη ακμή.
    docs = list(
        db["stats"]
        .find({"link": link}, {"_id": 0})
        .sort("time", 1)
    )
    if not docs:
        return jsonify({"error": f"Link '{link}' not found"}), 404
    return jsonify(docs)


@app.route("/api/windowed", methods=["GET"])
def get_windowed():
    #Επιστρέφει windowed aggregations (30s buckets) για όλες τις ακμές.
    docs = list(
        db["windowed_stats"]
        .find({}, {"_id": 0})
        .sort([("window_start", 1), ("link", 1)])
        .limit(100)
    )
    return jsonify(docs)


@app.route("/api/raw/count", methods=["GET"])
def get_raw_count():
    #Επιστρέφει το πλήθος των raw εγγραφών στη βάση.
    count = db["raw_data"].count_documents({})
    return jsonify({"raw_data_count": count})


@app.route("/api/links", methods=["GET"])
def get_links():
    #Επιστρέφει λίστα με όλες τις μοναδικές ακμές.
    links = db["stats"].distinct("link")
    return jsonify(sorted(links))


@app.route("/", methods=["GET"])
def index():
    return jsonify({
        "message": "UXSIM Traffic Statistics API",
        "endpoints": [
            "GET /api/stats",
            "GET /api/stats/<link>",
            "GET /api/windowed",
            "GET /api/raw/count",
            "GET /api/links"
        ]
    })


if __name__ == "__main__":
    print("Starting REST API on http://localhost:5001")
    app.run(debug=True, host="0.0.0.0", port=5001)
