import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from xml.etree import ElementTree

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from core import parse_form, parse_transcription, session_cookie, session_expiry, voice_xml, valid_twilio_signature


class CoreTests(unittest.TestCase):
    """Exercise portable logic without starting the Cloudflare runtime."""

    def test_twilio_sdk_signature_vector(self):
        """Accept an SDK-generated signature and reject a changed body."""
        # Vector independently calculated with Twilio's Node SDK (not real credentials).
        params = parse_form('CallSid=CA1234567890ABCDE&Caller=%2B14158675310&Digits=1234&From=%2B14158675310&To=%2B18005551212')
        self.assertTrue(valid_twilio_signature('12345', 'https://mycompany.com/myapp.php?foo=1&bar=2', params, 'GvWf1cFY/Q7PnoempGyD5oXAezc='))
        params['Digits'] = ['9999']
        self.assertFalse(valid_twilio_signature('12345', 'https://mycompany.com/myapp.php?foo=1&bar=2', params, 'GvWf1cFY/Q7PnoempGyD5oXAezc='))

    def test_voice_contract(self):
        """Keep the consent announcement before a ten-minute transcription."""
        xml = ElementTree.fromstring(voice_xml('https://twilio.genehuntley.com'))
        self.assertEqual(xml[0].tag, 'Say')
        transcription = xml.find('Start/Transcription')
        self.assertEqual(transcription.get('partialResults'), 'true')
        self.assertEqual(transcription.get('track'), 'inbound_track')
        self.assertEqual(transcription.get('statusCallbackUrl'), 'https://twilio.genehuntley.com/transcription')
        self.assertEqual(sum(int(p.get('length')) for p in xml.findall('Pause')), 600)
        self.assertEqual(xml[-1].tag, 'Hangup')

    def test_session_tampering_rotation_and_expiry(self):
        """Reject modified, rotated-secret, and expired console cookies."""
        with patch('core.now_ms', return_value=1000):
            cookie = session_cookie('test-password')
            self.assertEqual(session_expiry(cookie, 'test-password'), 28801000)
            self.assertEqual(session_expiry(cookie, 'other-password'), 0)
            self.assertEqual(session_expiry(cookie.replace('28801000', '38801000'), 'test-password'), 0)
        with patch('core.now_ms', return_value=28801001):
            self.assertEqual(session_expiry(cookie, 'test-password'), 0)

    def test_form_repeated_and_empty_values(self):
        """Preserve every value needed by Twilio signature validation."""
        self.assertEqual(parse_form('a=1&a=2&empty=&unicode=%E2%9C%93'), {'a':['1','2'], 'empty':[''], 'unicode':['✓']})

    def test_events(self):
        """Normalize valid content callbacks and reject malformed fields."""
        form = {key:[value] for key,value in dict(CallSid='CA'+'a'*32,TranscriptionSid='GT'+'b'*32,SequenceId='2',TranscriptionEvent='transcription-content',Final='false',TranscriptionData=json.dumps({'transcript':'hello'})).items()}
        self.assertFalse(parse_transcription(form)['final'])
        form['Final'] = ['true']
        self.assertTrue(parse_transcription(form)['final'])
        form['TranscriptionData'] = ['[]']
        with self.assertRaises(ValueError): parse_transcription(form)
        form['SequenceId'] = ['-1']
        with self.assertRaises(ValueError): parse_transcription(form)

if __name__ == '__main__':
    unittest.main()
