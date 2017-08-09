import boto3
import json

def build_speechlet_response(title, output, reprompt_text, should_end_session):
    return {
        'outputSpeech': {
            'type': 'PlainText',
            'text': output
        },
        'card': {
            'type': 'Simple',
            'title': "SessionSpeechlet - " + title,
            'content': "SessionSpeechlet - " + output
        },
        'reprompt': {
            'outputSpeech': {
                'type': 'PlainText',
                'text': reprompt_text
            }
        },
        'shouldEndSession': should_end_session
    }


def build_response(speechlet_response):
    return {
        'version': '1.0',
        'sessionAttributes': {},
        'response': speechlet_response
    }


# --------------- Functions that control the skill's behavior ------------------

def get_welcome_response():
    card_title = "Welcome"
    speech_output = "Willkommen zum Isar-Pegel Alexa Skill. " \
                    "Du kannst bei mir erfahren welche Temperatur, welchen Wasserstand " \
                    "und welche Fließgeschwindigkeit die Isar in München hat."
    reprompt_text = "Frage mich, was Du über die Isar wissen magst."
    should_end_session = False
    return build_response(build_speechlet_response(
        card_title, speech_output, reprompt_text, should_end_session))

def handle_session_end_request():
    card_title = "Session Ended"
    speech_output = "Ciao und viel Spaß an der Isar!"
    should_end_session = True
    return build_response(build_speechlet_response(
        card_title, speech_output, None, should_end_session))

def get_isar_data():
    lambda_client = boto3.client('lambda')
    response = lambda_client.invoke(FunctionName="IsarPegel", InvocationType='RequestResponse')
    data = response['Payload'].read()
    data = str(data, 'utf-8')
    data = json.loads(data)
    data = {
        'level': data['level']['value'],
        'flow': data['flow']['value'],
        'temperature': data['temperature']['value']
    }
    return data

def get_isar_info(query):
    wording = {
        'level': 'Der Wasserstand beträgt zur Zeit {level:.0f} Zentimeter',
        'temperature': 'Die Isar hat zur Zeit {temperature:.1f} Grad',
        'flow': 'Im Moment fließt die Isar mit {flow:.0f} Kubikmetern pro Sekunde',
        'all': 'Die Isar hat einen Pegel von {level:.0f} Zentimetern, ist {temperature:.1f} Grad kalt und fließt mit {flow:.0f} Kubikmetern pro Sekunde'
    }

    data = get_isar_data()

    card_title = query
    speech_output = wording[query].format(**data).replace('.', ',')
    reprompt_text = ''
    should_end_session = True

    return build_response(
                build_speechlet_response(
                    card_title,
                    speech_output,
                    reprompt_text,
                    should_end_session
                )
            )


# --------------- Events ------------------

def on_launch(launch_request, session):
    print("on_launch requestId=" + launch_request['requestId'] +
          ", sessionId=" + session['sessionId'])
    return get_welcome_response()


def on_intent(intent_request, session):
    print("on_intent requestId=" + intent_request['requestId'] +
          ", sessionId=" + session['sessionId'])

    intent = intent_request['intent']
    intent_name = intent_request['intent']['name']

    if intent_name == "GetIsarLevel":
        return get_isar_info('level')
    elif intent_name == "GetIsarTemperature":
        return get_isar_info('temperature')
    elif intent_name == "GetIsarFlow":
        return get_isar_info('flow')
    elif intent_name == "GetIsarInfo":
        return get_isar_info('all')
    elif intent_name == "AMAZON.HelpIntent":
        return get_welcome_response()
    elif intent_name == "AMAZON.CancelIntent" or intent_name == "AMAZON.StopIntent":
        return handle_session_end_request()
    else:
        raise ValueError("Invalid intent")


# --------------- Main handler ------------------

def lambda_handler(event, context):
    print("event.session.application.applicationId=" +
          event['session']['application']['applicationId'])

    if event['request']['type'] == "LaunchRequest":
        return on_launch(event['request'], event['session'])
    elif event['request']['type'] == "IntentRequest":
        return on_intent(event['request'], event['session'])

