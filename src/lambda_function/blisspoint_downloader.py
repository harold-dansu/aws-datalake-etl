import base64
import binascii
import boto3
import csv
import datetime
import hashlib
import hmac
import json
import os
import pandas as pd
import re
import requests
import six
import time

from warrant import aws_srp
from warrant.aws_srp import AWSSRP
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

print('Loading function')

def get_secrets():
  """Retrieve credentials from AWS Secrets Manager"""
  secret_name = "blisspoint-misfits"
  region_name = "us-east-1"

  # create a Secrets Manager client
  session = boto3.session.Session()
  client = session.client(
      service_name='secretsmanager',
      region_name=region_name
  )
  try:
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
  except ClientError as e:
    raise e
  else:
    secret = get_secret_value_response['SecretString']
  return secret

# use credentials from Secrets Manager to create user & pwd variables
credentials_string = get_secrets()
credentials = json.loads(credentials_string) #create dict so credentials not accessed with int indices

FILE_NAME = '/tmp/' + 'blisspoint_file.csv'
#BUCKET_NAME = 'insightsdp-dev-datalake'
BUCKET_NAME = '371584219818-rubix-prd-datalake-raw'
USERNAME = credentials["username"]
PASSWORD = credentials["password"]

client_id = '65hq4tpaon1b81kr7q9upoh1mn'    #cognito app id 
pool_id = 'us-west-2_QRVXCFwDq'             #cognito user pool id


# define helper functions
def hash_sha256(buf):
    """AuthenticationHelper.hash"""
    a = hashlib.sha256(buf).hexdigest()
    return (64 - len(a)) * '0' + a
    
def hex_hash(hex_string):
    return hash_sha256(bytearray.fromhex(hex_string))
	
def pad_hex(long_int):
    """
    Converts a Long integer (or hex string) to hex format padded with zeroes for hashing
    :param {Long integer|String} long_int Number or string to pad.
    :return {String} Padded hex string.
    """
    if not isinstance(long_int, six.string_types):
        hash_str = long_to_hex(long_int)
    else:
        hash_str = long_int
    if len(hash_str) % 2 == 1:
        hash_str = '0%s' % hash_str
    elif hash_str[0] in '89ABCDEFabcdef':
        hash_str = '00%s' % hash_str
    return hash_str

def hex_to_long(hex_string):
    return int(hex_string, 16)


def long_to_hex(long_num):
    return '%x' % long_num

# Step 1: Initiate session
client = boto3.client('cognito-idp', region_name="us-west-2")

aws = AWSSRP(username=USERNAME, password=PASSWORD, pool_id=pool_id,
            client_id=client_id, client=client)

srp_a = aws_srp.long_to_hex(aws.large_a_value)  #get A value

# Step 2:
# Submit USERNAME & SRP_A to Cognito, get challenge.

auth_init = client.initiate_auth(
    AuthFlow='USER_SRP_AUTH',
    AuthParameters={
        'USERNAME': USERNAME,
    	'SRP_A': srp_a,
    },
    ClientId=client_id,
    ClientMetadata={ 'UserPoolId': pool_id }
)

def get_password_authentication_key(username, password, server_b_value, salt):
	"""
	Calculates the final hkdf based on computed S value, and computed U value and the key
	:param {String} username Username.
	:param {String} password Password.
	:param {Long integer} server_b_value Server B value.
	:param {Long integer} salt Generated salt.
	:return {Buffer} Computed HKDF value.
	"""
	#u_value = calculate_u(self.large_a_value, server_b_value)
	u_value = aws_srp.calculate_u(srp_a, server_b_value)
	
	if u_value == 0:
		raise ValueError('U cannot be zero.')
	username_password = '%s%s:%s' % (pool_id.split('_')[1], username, password)
	username_password_hash = aws_srp.hash_sha256(username_password.encode('utf-8'))

	x_value = hex_to_long(hex_hash(pad_hex(salt) + username_password_hash))
	g_mod_pow_xn = pow(aws.g, x_value, aws.big_n)
	int_value2 = server_b_value - aws.k * g_mod_pow_xn
	s_value = pow(int_value2, aws.small_a_value + u_value * x_value, aws.big_n)
	hkdf = aws_srp.compute_hkdf(bytearray.fromhex(pad_hex(s_value)),
						bytearray.fromhex(pad_hex(long_to_hex(u_value))))
	return hkdf

# construct response to challenge
def process_challenge(auth_init):
	user_name = auth_init['ChallengeParameters']['USERNAME']
	salt_hex = auth_init['ChallengeParameters']['SALT']
	srp_b_hex = auth_init['ChallengeParameters']['SRP_B']
	secret_block_b64 = auth_init['ChallengeParameters']['SECRET_BLOCK']

	# re strips leading zero from a day number (required by AWS Cognito)
	timestamp = re.sub(r" 0(\d) ", r" \1 ",
					   datetime.utcnow().strftime("%a %b %d %H:%M:%S UTC %Y"))
					   
	hkdf = get_password_authentication_key(user_name, PASSWORD, 
										   hex_to_long(srp_b_hex),
										   salt_hex)
											  
	secret_block_bytes = base64.standard_b64decode(secret_block_b64)

	msg = bytearray(pool_id.split('_')[1], 'utf-8') + bytearray(user_name, 'utf-8') + bytearray(secret_block_bytes) + bytearray(timestamp, 'utf-8')

	hmac_obj = hmac.new(hkdf, msg, digestmod=hashlib.sha256)
	signature_string = base64.standard_b64encode(hmac_obj.digest())

	processed_responses = {'TIMESTAMP': timestamp,
		'USERNAME': user_name,
		'PASSWORD_CLAIM_SECRET_BLOCK': secret_block_b64,
		'PASSWORD_CLAIM_SIGNATURE': signature_string.decode('utf-8') 
	}
	
	return processed_responses

# Step 3:
# Use challenge parameters from Cognito to construct challenge response.

cr = process_challenge(auth_init)

response = client.respond_to_auth_challenge(
    ClientId=client_id,
    ChallengeName=auth_init['ChallengeName'],
    ChallengeResponses=cr
)

access_token = response.get('AuthenticationResult')['AccessToken']

# helper functions
def get_start_date_month():
	''' get monday from 4 calendar weeks before current week '''
	now = datetime.now()
	monday = now - timedelta(days = now.weekday(), weeks=4)
	return monday.strftime("%Y-%m-%d")
 
def get_end_date_month():
	''' get sunday from 4 calendar weeks before current week '''
	now = datetime.now()
	sunday = now + timedelta(days=6, weeks=4)
	return sunday.strftime("%Y-%m-%d")

def get_start_date_week():
	''' get monday of current week '''
	now = datetime.now()
	monday = now - timedelta(days = now.weekday(), weeks=0)
	return monday.strftime("%Y-%m-%d")

def get_streaming_data(token):
	"""
	Uses access token to query Blisspoint api and store result as a csv
	:param {String} token Access Token.
	:return {Json} json file with server response.
	"""
	start_date = get_start_date_month()
	end_date = get_end_date_month()
	
	REQUEST_URL = 'https://api.blisspointmedia.com/admin/pacing?company=misfitsmarket&start=' +\
				start_date + '&end=' + end_date + '&groupbyday=1'

	authorization = 'bearer ' + token

	header={
	  'Authority': 'api.blisspointmedia.com',
	  'Referer': 'https://app.blisspointmedia.com/',
	  'Origin': 'https://app.blisspointmedia.com',
	  'Authorization' : authorization,
	  'Sec-Fetch-Dest': 'empty',
	  'Sec-Fetch-Mode': 'cors',
	  'Sec-Fetch-Site': 'same-site',
	  'Sec-Gpc' :'1'
	  }

	try:
		a = requests.get( REQUEST_URL, headers= header )

	except requests.exceptions.RequestException as e:  #catch all exceptions 
		print(e)
		raise SystemExit(e)

	return a.json()

def get_linear_data(token):
	"""
	Uses access token to query Blisspoint api and returns a json string
	:param {String} token Access Token.
	:return {Json} json file with server response.
	"""
	start_date = get_start_date_week()
	
	REQUEST_URL = 'https://api.blisspointmedia.com/linear/estimates?company=misfitsmarket&week=' + start_date 
  #start date is monday of current week

	authorization = 'bearer ' + token

	header={
	  'Authority': 'api.blisspointmedia.com',
	  'Referer': 'https://app.blisspointmedia.com/',
	  'Origin': 'https://app.blisspointmedia.com',
	  'Authorization' : authorization,
	  'Sec-Fetch-Dest': 'empty',
	  'Sec-Fetch-Mode': 'cors',
	  'Sec-Fetch-Site': 'same-site',
	  'Sec-Gpc' :'1'
	  }
    
	try:
		a = requests.get( REQUEST_URL, headers= header )

	except requests.exceptions.RequestException as e:  #catch all exceptions 
		print(e)
		raise SystemExit(e)

	return a.json()

streaming_data = get_streaming_data(access_token)
linear_data = get_linear_data(access_token)

def streaming_json_to_df(payload):
	"""
	Converts nested json to csv using pandas
	:param {Dict} dict of streaming video and audio metrics.
	:return {Dataframe} panda dataframe
	"""

	text = json.dumps(payload)
	report = json.loads(text)
	audio = report['audio']
	video = report['streaming']

	#create list of lists
	list_of_report_rows = []
	for key, value in video.items():
		spend = value['spend']
		row = [key, spend]
		list_of_report_rows.append(row)

	staging_df = pd.DataFrame(list_of_report_rows,)
	headers = ['Date', 'Streaming Video']
	df1  = pd.DataFrame(staging_df.values, columns=headers)

	list_of_report_rows = []
	for key, value in audio.items():
		spend = value['spend']
		row = [key, spend]
		list_of_report_rows.append(row)

	staging_df = pd.DataFrame(list_of_report_rows,)
	headers = ['Date', 'Streaming Audio']
	df2  = pd.DataFrame(staging_df.values, columns=headers)

	df= pd.merge(df1, df2, how='outer', on='Date')  # streaming and audio dicts can be of different lengths

	return df

def linear_json_to_df(payload):
	"""
	Converts nested json to csv using pandas
	:param {Dict} dict of streaming video and audio metrics.
	:return {Dataframe} panda dataframe
	"""

	text = json.dumps(payload)
	report = json.loads(text)
	tv_data = report['weekSummary']

	# loop through list 
	list_of_report_rows = []

	for datum in tv_data:
		day = datum['day']
		spend = datum['electronic']
		row = [day, spend]
		list_of_report_rows.append(row)
	  
	staging_df = pd.DataFrame(list_of_report_rows,)
	headers = ['Date', 'Linear TV']
	df = pd.DataFrame(staging_df.values, columns=headers)
	
	return df

def pd_to_csv(df):
	"""
	Convert panda dataframe to csv
	:param {Dataframe} panda dataframe
	:return {File} csv file.
	"""
	#sort by date before converting to csv for human readability
	df = df.sort_values(by=['Date'], ascending=True)
	return df.to_csv(FILE_NAME, index=False)

if len(streaming_data)==2 and len(linear_data)==1:
	df1 = streaming_json_to_df(streaming_data)
	df2 = linear_json_to_df(linear_data)
	df = pd.merge(df1, df2, how='outer', on='Date')    
	pd_to_csv(df)
else:
	#TODO: Trigger alert if response is not 2 nested dicts
	raise UserWarning('incomplete result from query')

def save_to_s3(file_name, bucket, object_name=None):
  '''
  Upload report file from local file system
  to s3 bucket. Also name file per conventions
  
  :param file_name: File to upload
  :param bucket: Bucket to upload to
  :param object_name: S3 object name. If not specified then file_name is used
  :return: True if file was uploaded, else False
  '''
      
  # If S3 object_name not specified, assume it's same as file_name
  if object_name is None:
    object_name = os.path.basename(file_name)
        
  # upload file
  s3 = boto3.client('s3')
  try:
    s3.upload_file(file_name, bucket, object_name)
    print(object_name + ' successfully uploaded to s3 bucket')
  except Exception as e:
    print(e)
    return False
  return True


def name_target_file():
  today = datetime.now()
  source = 'blisspoint'
  report = 'spend_summary'
  #account_num = '101010'

  year = str(today.year)
  month = str(today.month).rjust(2, '0')  #pad string if not double digit
  day =  str(today.day).rjust(2, '0')     #pad string if not double digit

  date = year + '-' + month + '-' + day
  target_file = source+'/' + report + '/year='+year + \
                '/month=' + month + '/day=' + day + '/' + date + '.csv'
  return target_file


def lambda_handler(event, context):
    target_name = name_target_file()
    # if report download is successful, then proceed to S3 upload
    save_to_s3(FILE_NAME, BUCKET_NAME, target_name)