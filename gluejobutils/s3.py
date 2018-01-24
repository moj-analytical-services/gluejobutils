def s3_path_to_bucket_key(path):
    path = path.replace("s3://", "")
    bucket, key = path.split('/', 1)
    return bucket, key
