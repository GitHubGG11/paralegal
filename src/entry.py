"""Minimal Cloudflare Python Worker used to verify the learning environment."""

from workers import Response, WorkerEntrypoint

from health import health_message


class Default(WorkerEntrypoint):
    async def fetch(self, request):
        return Response(health_message())
