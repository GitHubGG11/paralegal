from xml.etree.ElementTree import Element, SubElement, tostring

def voice_xml():
    response = Element("Response")

    say = SubElement(response, "Say")
    say.text = "Hello. This call is being handled by my Cloudflare Worker."

    SubElement(response, "Hangup")

    xml = tostring(response, encoding="unicode")
    return '<?xml version="1.0" encoding="UTF-8"?>' + xml

def webhook_xml():
    response = Element("Response")

    connect = SubElement(response, "Connect")
    SubElement(connect, "ConversationRelay", url="wss://twilio-paralegal-learning.gene-huntley2.workers.dev/message_hook")

    xml = tostring(response, encoding="unicode")

    return '<?xml version="1.0" encoding="UTF-8"?>' + xml
