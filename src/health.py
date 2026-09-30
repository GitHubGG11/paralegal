"""Portable health-check behavior used to prove the unit-test setup works."""
from workers import WorkerEntrypoint, Response
from urllib.parse import urlparse, parse_qs

class Default(WorkerEntrypoint):
    async def fetch(self, request):
        url = urlparse(request.url)
        params = parse_qs(url.query)
        

        if 'message' in params:
            message = params['message'][0]
            return Response.json({'message': message})

        return Response('none')
        
            