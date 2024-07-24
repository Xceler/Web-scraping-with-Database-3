from flask import Flask, jsonify
import main 

app = Flask(__name__)

@app.route('/update-jobs', methods = ['GET'])
def update_jobs():
    try:
        main.update_mongodb()
        return jsonify({"message" : "Data Scraping And Updated Completed"}), 200
    
    except Exception as e:
        return jsonify({"Error" : str(e)}), 500 
    
if __name__ == '__main__':
    app.run(debug = True)
