from email.mime import application
import boto3
import subprocess

# Amazon S3 Buckets & Shell CLI application
# Written By: Lorent Aliu 
# This project was created for CIS4010 - Cloud Computing at the University of Guelph

currBucket = None
currPath = None

def ch_folder(list, client, s3):
    if(len(list)<2):
        print('Cannot change folder')
        return 1
    global currBucket
    global currPath
    inputLine = list[1]
    if inputLine == '/':
        currBucket = None
        currPath = None
        return 0
    inputSplit = inputLine.split(":")
    #First Case if its a full path!
    if(len(inputSplit) > 1):
        #First check if bucket exists
        if(s3.Bucket(inputSplit[0]) in s3.buckets.all()):
            #check if path exists
            bucket = s3.Bucket(inputSplit[0])
            flag = False
            for object_summary in bucket.objects.filter(Prefix=inputSplit[1] + '/'):
                flag = True
            if flag == True:
                #Both bucket and path were found!
                currBucket = inputSplit[0]
                currPath = inputSplit[1] + '/'
                return 0
            else:
                print('Cannot change folder')
                return 1
        else:
            print('Cannot change folder')
            return 1
        
    elif(len(inputSplit) == 1):
        #Second case if only a 1 word input
        #Check if were in a current bucket first
        if(currBucket==None):
            #Must have given us a bucket name
            if(s3.Bucket(inputSplit[0]) in s3.buckets.all()):
                currBucket = inputSplit[0]
                return
            else:
                print('Cannot change folder')
                return 1
        else:
            #Current Bucket does exist
            #therefore the 1 word must be a path/relative-path
            #First lets check relative
            bucket = s3.Bucket(currBucket)
            if(currPath!=None):
                tempPath = currPath + inputSplit[0]
                flag = False
                for object_summary in bucket.objects.filter(Prefix=tempPath + '/'):
                    flag = True
                if flag == True:
                    currPath = tempPath + '/'
                    return 0
                else:
                    pass
            #Now lets check if it was a full path
            flag = False
            for object_summary in bucket.objects.filter(Prefix=inputSplit[0] + '/'):
                flag = True
            if flag == True:
                currPath = inputSplit[0] + '/'
                return 0
            print('Cannot change folder')
            return 1

def create_bucket(list, client):
    if len(list)<2:
        print('Cannot create bucket')
        return 1

    bucket = list[1]
    try:
        response = client.create_bucket(Bucket = bucket)
    except:
        print("Cannot create Bucket")
        return 1

def create_folder(list, client):
    if len(list) < 2:
        print('Cannot create folder')
        return 1
    bucketPath = list[1].split(":")
    if len(bucketPath) == 2:
        bucket = bucketPath[0]
    elif len(bucketPath) == 1 and currBucket != None:
        bucket = currBucket
    elif len(bucketPath) == 1 and currBucket == None:
        print('Cannot create folder')
        return 1
    if(currPath != None):
        folderName = currPath + bucketPath[-1]
    else:
        folderName = bucketPath[-1]
    try:
        response = client.put_object(Bucket = bucket, Key = (folderName + '/'))
    except:
        print("Cannot create folder")
        return 1

def cl_copy(list, client):
    if len(list) < 3:
        print('Unsuccessful copy')
        return 1

    bucketPath = list[1].split(":")
    if(len(bucketPath) == 1):
        if(currBucket!=None):
            #Use relative or full path (bucket already known)
            objName = bucketPath[-1]
            fileName = list[-1]
            #try relative first
            if(currPath!=None):
                tempPath = currPath + objName
                try:
                    response = client.download_file(currBucket, tempPath, fileName)
                except:
                    #Try full
                    try:
                        response = client.download_file(currBucket, objName, fileName)
                    except:
                        print('Unsuccessful copy')
                        return 1
            elif(currPath==None):
                try:
                    response = client.download_file(currBucket, objName, fileName)
                except:
                    print('Unsuccessful copy')
                    return 1
            return
        else:
            print('Unsuccessful copy')
            return 1
    elif(len(bucketPath) > 1):
        bucket = bucketPath[0]
        objName = bucketPath[-1]
        fileName = list[-1]
        try:
            response = client.download_file(bucket, objName, fileName)
            return 0
        except:
            print("Download Failed")
            return 1

def lc_copy(list, client):
    if len(list) < 3:
        print('Unsuccessful copy')
        return 1
    fileName = list[1]
    bucketPath = list[2].split(":")
    bucket = bucketPath[0]
    objName = bucketPath[-1]
    try:
        response = client.upload_file(fileName, bucket, objName)
        return 0
    except:
        print("Unsuccessful copy")
        return 1

def listF(list, client, s3):
    if len(list)== 1:
        #Must just be list
        for bucket in s3.buckets.all():
            print(bucket.name)
        return 0
    elif len(list) > 1:
        #Got arguments
        bucketPath = list[1].split(":")
        if len(bucketPath) == 1:
            #Must be just bucket name
            bucketName = bucketPath[0]
            try:
                bucket = s3.Bucket(bucketName)
                for obj in bucket.objects.all():
                    print(obj.key)
                return 0
            except:
                print('Cannot list contents of this S3 location')
                return 1
        if(len(bucketPath) == 2):
            bucketName = bucketPath[0]
            path = bucketPath[1]
            try:
                bucket = s3.Bucket(bucketName)
                for obj in bucket.objects.filter(Prefix=path):
                    print(obj.key)
                return 0
            except:
                print('Cannot list contents of this S3 location')
                return 1
    else:
        print('Cannot list contents of this S3 location')
        return 1


def cwf(list, client):
    if(currPath== None and currBucket == None):
        print('/')
        return 0
    elif(currBucket!=None):
        if(currPath==None):
            print(currBucket + ':')
            return 0
        elif(currPath!=None):
            print(currBucket + ':' + currPath)
            return 0
    else:
        print('Cannot access location in S3 space')
        return 1

def delete_bucket(list, client, s3):
    if(currBucket == list[1]):
        print('Cannot delete bucket')
        return
    bucketName = list[1]
    try:
        bucket = s3.Bucket(bucketName)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
        return 0
    except:
        print('Cannot delete bucket')
        return 1

def cdelete(list, client, s3):
    
    fullPath = list[1]
    bucketPath = fullPath.split(":")
    if len(bucketPath) > 1:
        #Full path
        bucketName = bucketPath[0]
        path = bucketPath[1]
        try:
            obj = s3.Object(bucketName, path)
            obj.delete()
            return 0
        except:
            print('Cannot perform delete')
    elif len(bucketPath) == 1:
        #No bucket
        path = bucketPath[0]
        if(currBucket==None):
            print('Cannot perform delete')
            return 1
        else:
            bucketName = currBucket
            #try relative
            if(currPath!=None):
                tempPath = currPath + path
                try:
                    obj = s3.Object(bucketName, tempPath)
                    obj.delete()
                except:
                    #try full
                    try:
                        obj = s3.Object(bucketName, path)
                        obj.delete()
                        return 0
                    except:
                        print('Cannot perform delete')
            elif currPath == None:
                try:
                    obj = s3.Object(bucketName, path)
                    obj.delete()
                    return 0
                except:
                    print('Cannot perform delete')
    return 1

def ccopy(list, client, s3):
    fromString = list[1].split(":")
    toString = list[2].split(":")
    if len(fromString) > 1:
        fromBucket = fromString[0]
        fromPath = fromString[1]
    if len(fromString) == 1:
        fromBucket = currBucket
        fromPath = fromString[0]
    if len(toString) > 1:
        toBucket = toString[0]
        toPath = toString[1]
    if len(toString) == 1:
        toBucket = currBucket
        toPath = toString[0]
    flag = False
    #Try relative From
    try:
        client.copy_object(Bucket=toBucket, CopySource= fromBucket + '/' + currPath + fromPath, Key= toPath)
        return 0
    except:
        flag = True
    #Try both full
    try:
        client.copy_object(Bucket=toBucket, CopySource= fromBucket + '/' + fromPath, Key= toPath)
        return 0
    except:
        flag = True

    if flag == True:
        print('Cannot perform copy')

    return 1

f = open("S5-S3conf", "r")
text = f.read()

print("Welcome to the AWS S3 Storage Shell (S5)")


try:
    client = boto3.client(
        's3',
        aws_access_key_id=text.split()[3],
        aws_secret_access_key=text.split()[6]
    )
    tempresp = client.list_buckets()
except:
    print('You could not be connected to your S3 storage')
    print('Please review procedures for authenticating your account on AWS S3')
    exit()
accessId = text.split()[3]
accessKey = text.split()[6]

s3 = boto3.resource(
    's3',
    aws_access_key_id=accessId,
    aws_secret_access_key=accessKey
)

print('You are now connected to your S3 storage')

while 1:
    userInput = input("S5> ")
    if userInput == 'exit':
        break
    if userInput == 'quit':
        break

    list = userInput.split()
    if list:
        if list[0] == 'ch_folder':
            errorResult = ch_folder(list, client, s3)
        elif list[0] == 'lc_copy':
            errorResult = lc_copy(list, client)
        elif list[0] == 'cl_copy':
            errorResult = cl_copy(list, client)
        elif list[0] == 'create_bucket':
            errorResult = create_bucket(list, client)
        elif list[0] == 'create_folder':
            errorResult = create_folder(list, client)
        elif list[0] == 'list':
            errorResult = listF(list, client, s3)
        elif list[0] == 'cwf':
            errorResult = cwf(list, client)
        elif list[0] == 'delete_bucket':
            errorResult = delete_bucket(list, client, s3)
        elif list[0] == 'cdelete':
            errorResult = cdelete(list, client, s3)
        elif list[0] == 'ccopy':
            errorResult = ccopy(list, client, s3)
        else:
            try:
                subprocess.run(list)
            except:
                print('Could not complete task')

