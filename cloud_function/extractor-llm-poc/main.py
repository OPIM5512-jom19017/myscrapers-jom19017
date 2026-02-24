from flask import jsonify, Request

def llm_extract_http(request: Request):
    """
    Minimal LLM Extractor Cloud Function.
    This is an HTTP-triggered function compatible with Gen2 Cloud Functions.
    """
    # Example: read JSON body
    try:
        request_json = request.get_json(silent=True)
    except Exception:
        request_json = None

    # Example output
    response = {
        "message": "LLM Extractor deployed successfully!",
        "input_received": request_json
    }

    return jsonify(response)