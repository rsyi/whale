from urllib import parse
import requests
from flask import Flask, request, redirect, flash

app = Flask(__name__)

SLACK_API_URL = "https://slack.com/api/oauth.v2.access"
REDIRECT_URL = "https://docs.whale.cx/features/metrics#setup"

CLIENT_ID = "123"
CLIENT_SECRET = "456"

@app.route('/', methods=['GET'])
def code_to_token():
    code_param = request.args.get("code")

    r = requests.get(
        SLACK_API_URL,
        params={
            "code": code_param,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
    )

    if not r.json()['ok']:
        return redirect(f"{REDIRECT_URL}?error={r.json().get('error')}")

    return redirect(f"{REDIRECT_URL}?token={r.json().get('access_token')}")

