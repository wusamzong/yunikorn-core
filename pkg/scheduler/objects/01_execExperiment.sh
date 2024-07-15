go test -timeout 3000000s -run ^TestP5$ github.com/apache/yunikorn-core/pkg/scheduler/objects &
sleep 1
go test -timeout 3000000s -run ^TestP4$ github.com/apache/yunikorn-core/pkg/scheduler/objects &
sleep 1
go test -timeout 3000000s -run ^TestP3$ github.com/apache/yunikorn-core/pkg/scheduler/objects &
sleep 1
go test -timeout 3000000s -run ^TestP2$ github.com/apache/yunikorn-core/pkg/scheduler/objects &
sleep 1
go test -timeout 3000000s -run ^TestP1$ github.com/apache/yunikorn-core/pkg/scheduler/objects &

wait