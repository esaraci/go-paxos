build: 
	go build main.go
	go build node_controller.go

release:
	zip release.zip node_controller main config.yaml
	tar -czf release.tar.gz node_controller main config.yaml
	rm node_controller main

doc:
	rm -rf Docs
	godoc -http=:6060 &
	sleep 1
	wget -r -np -N -E -p -k http://localhost:6060/pkg/go-paxos/
	pkill -f "godoc -http=:6060"
	mv localhost:6060 Docs
	echo "Documentation can be found in pkg/go-paxos/index.html" > Docs/readme.txt