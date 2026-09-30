import json

def on_message(server, event):
    message = json.loads(event.data)
    if message.get("type") == "prompt":
        data = message["voicePrompt"]
        server.send(json.dumps({
            "type": "text",
            "token": f"You said {data}.",
            "last": True,
    }))