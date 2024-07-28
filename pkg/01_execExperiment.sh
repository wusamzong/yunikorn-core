go test -timeout 3000000s -run ^TestP5$ thesis/pkg &
sleep 1
go test -timeout 3000000s -run ^TestP4$ thesis/pkg &
sleep 1
go test -timeout 3000000s -run ^TestP3$ thesis/pkg &
sleep 1
go test -timeout 3000000s -run ^TestP2$ thesis/pkg &
sleep 1
go test -timeout 3000000s -run ^TestP1$ thesis/pkg &

wait