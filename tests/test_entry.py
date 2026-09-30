from test_baseclass import WranglerTest
import httpx
import unittest
import xml.etree.ElementTree as ET

class TestLiveWorker(WranglerTest):
    # Inherits to turn on pywrangler instance. 

    def test_validMethods(self):
        with httpx.Client(base_url="http://localhost:8787") as client:
            response = client.post("/voice")
            assert(response.status_code == 200)

            response_get = client.get("/voice")
            assert(response_get.status_code == 405)
            assert(response_get.headers.get("Allowed") == "POST")


    def test_validWebhook(self):
        with httpx.Client(base_url="http://localhost:8787") as client:
            response = client.post("/voice")

            # Checks for valid XML & webhook properly formatted
            try: 
                root = ET.fromstring(response.content)
            except ET.ParseError:
                self.fail("Response text is not a valid XML.")

            connect = root.find("Connect")
            if connect is None:
                self.fail("<Connect> is not included in XML.")

            relay = connect.find("ConversationRelay")
            if relay is None:
                self.fail("<ConversationRelay> is not included in XML.")


    #TODO: Implement a unit test for returning responses. 

if __name__ == '__main__':
    unittest.main()
