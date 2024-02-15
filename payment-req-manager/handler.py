import boto3
from boto3.dynamodb.conditions import Key
import logging
from datetime import datetime, timedelta
if len(logging.getLogger().handlers) > 0:
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO)
from io import BytesIO
from openpyxl import Workbook
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import decimal
import json
import csv
import os
import calendar
from io import StringIO
from dateutil.relativedelta import relativedelta

dynamodb = boto3.resource('dynamodb')
pending_req_table = dynamodb.Table('pending-request')
batch_table = dynamodb.Table('batches')
academy_table = dynamodb.Table('academys')
academyPayments_table = dynamodb.Table('academyPayments')
standard_user_table = dynamodb.Table('standard-user')

//admin_email = 'sales@capriolesportstech.com'

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return str(obj)  # Convert Decimal to a string
        elif isinstance(obj, list):
            return [self.default(item) for item in obj]  # Recursively handle list elements
        return super(DecimalEncoder, self).default(obj)
    
    
    #find required months
    def get_date(duration):
        now = datetime.now()
        target_date = now - relativedelta(months=duration)
        target_date = datetime(target_date.year, target_date.month, 1)
        return target_date.strftime("%Y-%m-%d %H:%M:%S")
    
    def sendMail(body, duration, email, filename):
        
        # Your HTML content goes here
        html_content = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>CAPRIOLE PAYMENTS {#var1#}</title>
        </head>
        <body>
            <img src="https://capriole-photos.s3.ap-south-1.amazonaws.com/header.png" style="width:100%">
            <div style="white-space:pre-wrap;text-align:center">
                <h1 style="color: #0C8DCD;">PAYMENT COLLECTION DETAILS</h1>
            </div>
		    <div style="margin-left:5%;white-space:pre-wrap;">
                <p>Dear User,

As per your request, I have attached the payment details. This information is intended to assist you in reviewing and managing the financial records efficiently.
                
Thanks & Regards,
CAPRIOLE TEAM.</p>
            </div>
            <img src="https://capriole-photos.s3.ap-south-1.amazonaws.com/footer.png" style="width:100%">
        </body>
        </html>
        """

        # Replace {#var1#} in the HTML content with the date
        # html_content = html_content.replace("{#var1#}", date)
        # html_content = html_content.replace("{#var2#}", str(duration))

        # Create a MIME message with HTML content
        msg = MIMEMultipart()

        html_part = MIMEText(html_content, 'html')
        msg.attach(html_part)
        ses = boto3.client('ses', region_name='ap-south-1')
        if duration != 0:
            subject = "Requested Payment Details for the Last {#var4#} Months"
            subject = subject.replace("{#var4#}", str(duration))
        else:
            subject = "Requested Payment Details for the Current Month"
                

        msg['Subject'] = subject
        msg['From'] = os.environ['FROM_EMAIL']
        
        # Attach the Excel sheet as a file
        attachment = MIMEApplication(body, _subtype="csv")
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        msg.attach(attachment)

        # Send the email
        ses.send_raw_email(
            Source=msg['From'],
            Destinations=[email],
            RawMessage={'Data': msg.as_string()}
        )

        
def execute(event, context):
    logging.info(event)

    response = pending_req_table.scan()
    logging.info(response)
    all_payment_req = response.get('Items')
    
    for payment_req in all_payment_req:
        batchID = payment_req['batchID']
        duration = int(payment_req['duration'])
        email = payment_req['email']
        
        #fetching academy name and batch name from respective tables
        resposne = batch_table.get_item(Key={'batchID': batchID})
        batch_details = resposne.get('Item')
        batch_name = batch_details.get('name')
        academyID = batch_details.get('academyID')
        
        response = academy_table.get_item(Key={'academyID': academyID})
        if not response:
            continue
        else:
            academy_details = response.get('Item')
            academy_name = academy_details.get('name')
            # email = academy_details.get('emailID')
        file_name=f"{academy_name}_{batch_name}.csv"    
        csv_content = StringIO()
        writer = csv.writer(csv_content)
        # Write the header row
        writer.writerow(["Name", "PaymentID", "TransactionID","Time", "Mode","Amount"])
        date = DecimalEncoder.get_date(duration)
        logging.info(date)
        #fetch payment data from db
        response = academyPayments_table.query(
        IndexName= "searchBybatchIDandPaymentDate",
        KeyConditionExpression= Key('batchID').eq(batchID) & Key('paymentDate').gt(date)
        )
        logging.info(response)
        payment_data = response.get('Items')
        if len(payment_data)>0 and payment_data:
            # Write data for each enrollment 
            for data in payment_data:
                if data.get('transaction_status') == 'SUCCESS':
                    userID = data['enrollmentID'].split('_')[1]
                    response = standard_user_table.get_item(Key={'user_id': userID})
                    user_details = response.get('Item')
                    if user_details:
                        mode = 'Online' if data.get('isOnline') == True else 'Cash'
                        transactionID = data.get('transactionID','-')
                        writer.writerow([user_details['name'],data['paymentID'], transactionID,data['paymentDate'], mode,data['amount']])

        #delete the entry from pending-request which has proceed
        pending_req_table.delete_item(Key={'batchID': batchID})

        DecimalEncoder.sendMail(csv_content.getvalue(), duration, email,file_name)

    return {
         'statusCode': 200,
         'body': 'Success'
     }
