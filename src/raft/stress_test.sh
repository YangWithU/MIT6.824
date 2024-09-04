#python3 dstest -r --iter $1 \
#--workers 10 --timeout 60 --output out.log TestBackup2
#TestBasicAgree2B TestRPCBytes2B TestFollowerFailure2B TestLeaderFailure2B \
#TestFailAgree2B TestFailNoAgree2B TestConcurrentStarts2B TestRejoin2B \
#TestCount2B

#python3 dstest -r --iter $1 --workers 30 --timeout 150 --output out.log TestPersist12C \
#TestPersist22C TestPersist32C TestFigure82C \
#TestUnreliableAgree2C TestFigure8Unreliable2C \
#TestReliableChurn2C TestUnreliableChurn2C


#python3 dstest -r --iter 100 --workers 20 --timeout 60 --output out.log TestBasic3A TestSpeed3A TestConcurrent3A \
#TestUnreliable3A TestUnreliableOneKey3A TestOnePartition3A TestManyPartitionsOneClient3A \
#TestManyPartitionsManyClients3A TestPersistOneClient3A TestPersistConcurrent3A TestPersistConcurrentUnreliable3A \
#TestPersistPartition3A TestPersistPartitionUnreliable3A TestPersistPartitionUnreliableLinearizable3A

python3 dstest -r --iter 1000 --workers 60 --timeout 60 --output out.log TestSnapshotRPC3B TestSnapshotSize3B \
TestSpeed3B TestSnapshotRecover3B TestSnapshotRecoverManyClients3B TestSnapshotUnreliable3B \
TestSnapshotUnreliableRecover3B \
TestSnapshotUnreliableRecoverConcurrentPartition3B \
TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B
