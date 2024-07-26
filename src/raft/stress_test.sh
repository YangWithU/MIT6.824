python3 dstest -r --iter $1 \
--workers 10 --timeout 40 --output out.log \
TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B \
TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B TestBackup2B TestCount2B