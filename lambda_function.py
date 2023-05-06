import boto3
import os
import base64
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get S3 destination from environment
out_s3_bucket = os.environ['OUT_S3_BUCKET']
out_s3_prefix = "streaming/enriched"
in_s3_prefix = "streaming/data"
s3 = boto3.resource('s3')

# Create Comprehend client
comprehend = boto3.client('comprehend')
batch_size = 25

# Call Comprehend to find sentiment and key phrases in batch
def bulk_comprehend(recs):
    texts = []
    for rec in recs:
        texts.append(rec["text"])
    sentiment = comprehend.batch_detect_sentiment(TextList=texts, LanguageCode="en")
    for result in sentiment["ResultList"]:
        idx = result["Index"]
        recs[idx]["Sentiment"] = result["Sentiment"]
        recs[idx]["SentimentPositive"] = result["SentimentScore"]["Positive"]
        recs[idx]["SentimentNegative"] = result["SentimentScore"]["Negative"]
        recs[idx]["SentimentNeutral"] = result["SentimentScore"]["Neutral"]
        recs[idx]["SentimentMixed"] = result["SentimentScore"]["Mixed"]
    phrases = comprehend.batch_detect_key_phrases(TextList=texts, LanguageCode="en")
    for result in phrases["ResultList"]:
        idx = result["Index"]
        recs[idx]["KeyPhrases"] = []
        for phrase in result["KeyPhrases"]:
            recs[idx]["KeyPhrases"].append(phrase["Text"])

# Handler
def lambda_handler(event, context):
    logger.info(f"Event: {event}")

    for record in event["Records"]:
        in_s3_bucket = record["s3"]["bucket"]["name"]
        in_s3_key=record["s3"]["object"]["key"]
        logger.info(f"Processing s3://{in_s3_bucket}/{in_s3_key}")
        out_s3_key = out_s3_prefix + in_s3_key[len(in_s3_prefix):]
        in_lines = s3.Object(in_s3_bucket, in_s3_key).get()['Body'].read().decode('utf-8').splitlines()
        outstr = ""
        batch = []
        ct = 0
        for line in in_lines:
            rec = json.loads(line)
            ct = ct + 1
            batch.append(rec)
            if (len(batch) >= batch_size):
                bulk_comprehend(batch)
                for b in batch:
                    outstr = outstr + json.dumps(b) + "\n"
                batch = []
                ct = 0
        if (len(batch) > 0):
            bulk_comprehend(batch)
            for b in batch:
                outstr = outstr + json.dumps(b) + "\n"
            batch = []
            ct = 0
        res = s3.Object(out_s3_bucket, out_s3_key).put(Body=outstr.encode('utf-8'))
        logger.info(f"Inserted: {res}")

