#python3 dstest -r --iter $1 \
#--workers 10 --timeout 60 --output out.log TestBackup2
#TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B \
#TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B \
#TestCount2B

python3 dstest -r --iter $1 --workers 30 --timeout 150 --output out.log TestPersist12C \
TestPersist22C TestPersist32C TestFigure82C \
TestUnreliableAgree2C TestFigure8Unreliable2C \
TestReliableChurn2C TestUnreliableChurn2C