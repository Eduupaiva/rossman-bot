import pandas as pd 
import json 
import requests
import os 
from flask import Flask, request, Response 


#script_directory = os.path.dirname(os.path.abspath(__file__))

# Constants 
TOKEN = '6516231254:AAG4eNxk6IAv65uYfuE0B807EpXosDUkGGA'

# Bot Infos
#https://api.telegram.org/bot6516231254:AAG4eNxk6IAv65uYfuE0B807EpXosDUkGGA/getMe

# Get Updates
#https://api.telegram.org/bot6516231254:AAG4eNxk6IAv65uYfuE0B807EpXosDUkGGA/getUpdates

# Webhook
#https://api.telegram.org/bot6516231254:AAG4eNxk6IAv65uYfuE0B807EpXosDUkGGA/setWebhook?url=https://075cadaf9ab578.lhr.life/

# Send Message
#https://api.telegram.org/bot6516231254:AAG4eNxk6IAv65uYfuE0B807EpXosDUkGGA/sendMessage?chat_id=6231017320&text=Dai meu calabreso

#6231017320


def send_message( chat_id, text ):
    url = 'https://api.telegram.org/bot{}/'.format( TOKEN )
    url = url + 'sendMessage?chat_id={}'.format( chat_id )

    r = requests.post( url, json={'text': text } )
    print( 'Status Code {}'. format( r.status_code ) )

    return None 



def load_dataset( store_id ):
    
    # Loading Test DataSet
    df10 = pd.read_csv( 'test.csv' )
    df_store_raw = pd.read_csv( 'store.csv' ) 

    # Merge Test Dataset + Store
    df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )

    # Choose Store for prediction
    df_test = df_test[df_test['Store'] == store_id ]

    if not df_test.empty:
        # Remove Closed Days
        df_test = df_test[df_test['Open'] != 0 ]
        df_test = df_test[~df_test['Open'].isnull() ]
        df_test = df_test.drop( 'Id', axis=1 )

        # Convert DataFrame to Json
        data = json.dumps( df_test.to_dict( orient='records' ) )
    else:
        data = 'error'

    return data

def predict( data ):
    # API Call 
    url = 'https://teste-rossman-api-nzpe.onrender.com/rossman/predict'
    header = { 'Content-type': 'application/json' }
    data = data

    r = requests.post( url, data=data, headers=header )

    # Verifica se a resposta foi bem-sucedida (código 200)
    if r.status_code == 200:
        # Tenta decodificar o JSON
        try:
            d1 = pd.DataFrame(r.json(), columns=r.json()[0].keys())
            print('JSON Decoding Successful')

            # Continue com o restante do seu código aqui...

        except json.JSONDecodeError as e:
            print(f'Error decoding JSON: {e}')

    else:
        print(f'Request failed with status code: {r.status_code}')
        
    print( 'Status Code {}'.format( r.status_code ) )
    
    d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )

    return d1

def parse_message( message ):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']

    store_id = store_id.replace( '/', '' )

    try:
        store_id = int( store_id )
    
    except ValueError:
        #send_message( chat_id, 'Thats not a Store ID' )
        store_id = 'Error'
    
    return chat_id, store_id 


# API Initialize
app = Flask( __name__ )

@app.route( '/', methods=['GET', 'POST'] )
def index():
    if request.method == 'POST':
        message = request.get_json()

        chat_id, store_id = parse_message( message )

        if store_id != 'error':
            # Loading Data
            data = load_dataset( store_id )

            if data != 'error':
                # Prediction
                d1 = predict( data )

                # Calculation
                d2 = d1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()

                # Send Message
                msg = 'Store Number {} will sell R$ {:,.2f} in the next 6 weeks'.format( d2['store'].values[0], d2['prediction'].values[0] )
                send_message ( chat_id, msg )
                return Response( 'Ok', status=200 )

            else: 
                send_message( chat_id, 'Store is Not Available' )
                return Response( 'Ok', status=200 )
        else:
             send_message( chat_id, 'Thats not a Store ID' )
             return Response( 'Ok', status=200 ) 
    else:
        return '<h1> Rossman Telegram BOT </h1>' 

if __name__ == '__main__':
    port = os.environ.get( 'PORT', 5000 )
    app.run( host='0.0.0.0', port=port )
