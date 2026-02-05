import subprocess
from flask import Flask, request

app = Flask(__name__)

@app.route('/', methods=['POST'])
@app.route('/<dbname>', methods=['POST'])
def run_query(dbname=""):
    # Use -u in Docker to make sure this prints immediately!
    query = request.data.decode('utf-8').strip()
    print(f"--- EXECUTING ---", flush=True)
    print(f"Query: {query}", flush=True)
    
    try:
        # We call the actual Mongo Shell installed in the container
        process = subprocess.run(
            ['mongosh', f"mongodb://mongodb:27017/{dbname}", '--quiet', '--eval', query],
            capture_output=True, 
            text=True
        )
        
        if process.returncode == 0:
            return process.stdout, 200, {'Content-Type': 'application/json'}
        else:
            return f"Mongo Shell Error: {process.stderr}\n", 400
            
    except Exception as e:
        return f"System Error: {str(e)}\n", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
