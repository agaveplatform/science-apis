#!/bin/bash
start=$SECONDS
#parallel --joblog /tmp/parLog --delay 5.0 release/sftp-client-darwin-amd64 get -H sftp -g "[::1]:50051" -d "/tmp/1MB{1}.txt" -s "/tmp/1MB.txt" ::: {1..2}
parallel --joblog /tmp/parLog --delay 5.0 release/sftp-client-darwin-amd64 put -H sftp -g "[::1]:50051" -d "/tmp/1MB{1}.txt" -s "/tmp/1MB.txt" ::: {1..2}

end=$SECONDS
echo "duration: $((end-start)) seconds."


#go run main.go put -H sftp -g "[::1]:50052" -d "/tmp/10MB.txt"  -s "/tmp/10MB.txt"
