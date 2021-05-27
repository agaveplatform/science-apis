# Agave Systems API Core library


## Testing

Unit tests are run without any third-party dependencies as part of the `test` lifecycle phase using the standard Surefire plugin. 

Run the unit tests for a submodule using the following command from the submodule's root folder. 

```bash
mvn -pl test 
```

### Integration Tests

Integration tests are run against a suite of Docker containers representing the data and execution protocols supported by the system definitions. During the `pre-integration-test` the containers are started up [Docker Compose](https://docs.docker.com/compose/) via the [Docker Compose Maven Plugin](https://github.com/dkanejs/docker-compose-maven-plugin). The tests then run during the `integration-test` lifecycle phase. Finally, the containers are torn down by the Docker Compose Maven Plugin during the `post-integration-test` phase.


Run the integration tests for a submodule using the following command from the submodule's root folder. 
  
```bash
mvn -pl verify 
```

#### S3
In order to run the AWS S3 integration tests, create an IAM user and grant them the following policy replacing `bucket_name` with the actual name of your test bucket as defined by the `s3.bucket` property in your Maven settings file.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": "s3:ListBucket",
            "Resource": "arn:aws:s3:::bucket_name"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": [
                "arn:aws:s3:::bucket_name/*",
                "arn:aws:s3:::bucket_name"
            ]
        },
        {
            "Sid": "VisualEditor2",
            "Effect": "Allow",
            "Action": "s3:ListAllMyBuckets",
            "Resource": "*"
        }
    ]
}
```



