"""Real end-to-end test. Requires the running Docker application; never synthesizes success."""
import argparse,json,time,urllib.request
from urllib.error import URLError
def request(path,data=None):
    body=None if data is None else json.dumps(data).encode()
    req=urllib.request.Request('http://127.0.0.1:8095/api/'+path,data=body,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req,timeout=30) as r:return json.load(r)

def verify_database(row,region='us-east-1',endpoint='http://127.0.0.1:4570'):
    import boto3
    from decimal import Decimal
    options={'region_name':region}
    if endpoint:options.update(endpoint_url=endpoint,aws_access_key_id='test',aws_secret_access_key='test')
    db=boto3.client('dynamodb',**options)
    bundles=[]
    for page in db.get_paginator('query').paginate(TableName='eventflow-data',ConsistentRead=True,
        KeyConditionExpression='pk = :p AND begins_with(sk, :s)',
        ExpressionAttributeValues={':p':{'S':'RUN#'+row['runId']},':s':{'S':'RESULT#'}}):
        bundles.extend(json.loads(item['payload']['S']) for item in page['Items'])
    assert len(bundles)==row['totalLoans']*len(row['config']['applications']),len(bundles)
    identities={(b['loanId'],b['application']) for b in bundles}
    assert len(identities)==len(bundles),'Duplicate loan/application result'
    count=0
    for bundle in bundles:
        assert bundle['status']=='COMMITTED',bundle
        for record in bundle['records']:
            assert sum(Decimal(line['debit'])-Decimal(line['credit']) for line in record['AR']['lines'])==0,record
            count+=1
    assert count==row['beCount']==row['arCount'],count
    return {'loanApplicationItems':len(bundles),'balancedAR':count}

def main():
    p=argparse.ArgumentParser();p.add_argument('--level',choices=['POOL','LOAN'],default='POOL');p.add_argument('--timeout',type=int,default=240)
    p.add_argument('--run-id',help='Watch an existing run instead of submitting a new event')
    p.add_argument('--verify-db',action='store_true',help='Also reconcile actual DynamoDB items and debit/credit balance')
    p.add_argument('--aws-native',action='store_true',help='Verify DynamoDB in AWS with the normal credential chain instead of LocalStack')
    p.add_argument('--region',default='us-east-1')
    a=p.parse_args()
    from datetime import date
    run={'runId':a.run_id} if a.run_id else request('runs',{'totalLoans':100,'poolSize':100,'level':a.level,'eventType':'settled','accountingDate':date.today().isoformat()})
    print('Watching' if a.run_id else 'Started',run['runId']);deadline=time.monotonic()+a.timeout
    row={'status':'WAITING_FOR_BACKEND'}
    while time.monotonic()<deadline:
        try:row=request('run?id='+run['runId'])
        except (URLError,TimeoutError):
            # Java-service rebuilds briefly stop HTTP; keep watching the same run.
            time.sleep(3)
            continue
        if row['status']=='COMMITTED':
            apps=row['config']['applications'];expected=sum(len(app['businessEvents']) for app in apps)*row['totalLoans']
            assert row['published']==row['totalLoans'],row
            assert row['beCount']==expected and row['arCount']==expected,row
            detail=verify_database(row,region=a.region,endpoint=None if a.aws_native else 'http://127.0.0.1:4570') if a.verify_db else {}
            print(json.dumps({'status':'PASS','runId':run['runId'],'published':row['published'],'BE':row['beCount'],'AR':row['arCount'],**detail}))
            return
        if row.get('generationError'):raise RuntimeError(row['generationError'])
        time.sleep(3)
    raise TimeoutError('Run did not commit within test deadline: '+json.dumps(row))
if __name__=='__main__':main()
