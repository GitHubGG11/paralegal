from xml.etree.ElementTree import Element, SubElement, tostring

HOOK = "message_hook"

def voice_xml():
    response = Element("Response")

    say = SubElement(response, "Say")
    say.text = "Hello. This call is being handled by my Cloudflare Worker."

    SubElement(response, "Hangup")

    xml = tostring(response, encoding="unicode")
    return '<?xml version="1.0" encoding="UTF-8"?>' + xml

def webhook_xml():
    '''Returns the webhook for the '''
    response = Element("Response")

    connect = SubElement(response, "Connect")
    SubElement(connect, "ConversationRelay", url=f"wss://twilio-paralegal-learning.gene-huntley2.workers.dev/{HOOK}")

    xml = tostring(response, encoding="unicode")

    return '<?xml version="1.0" encoding="UTF-8"?>' + xml
