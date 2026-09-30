"""Portable health-check behavior used to prove the unit-test setup works."""
from workers import WorkerEntrypoint, Response
from urllib.parse import urlparse, parse_qs
from to_xml import voice_xml, webhook_xml
from js import WebSocketPair
from pyodide.ffi import create_proxy
import json


class Default(WorkerEntrypoint):
    async def fetch(self, request):

        url = urlparse(request.url)
        params = parse_qs(url.query)

        if url.path == "/voice":
            if request.method != "POST":
                return Response(
                    "Method not allowed", 
                    status=405,
                    headers={"Allowed": "POST"}
                )

            return Response(
                webhook_xml(), 
                status=200,
                headers={
                    "Content-Type": "application/xml; charset=utf-8",
                    "Cache-Control": "no-store",
            })

        if url.path == "/message_hook":
            upgrade = request.headers.get("Upgrade")
            if not upgrade or upgrade.lower() != "websocket":
                return Response("Expected a valid webhook!", status=426)

            client, server = WebSocketPair.new().object_values()
            server.accept()

            def on_message(event):
                message = json.loads(event.data)
                if message.get("type") == "prompt":
                    data = message["voicePrompt"]
                    server.send(json.dumps({
                        "type": "text",
                        "token": f"You said {data}.",
                        "last": True,
                    }))

            handler = create_proxy(on_message)
            server.addEventListener("message", handler)

            return Response(None, status=101, web_socket=client)


        return Response("Not found", status=404)
        
            