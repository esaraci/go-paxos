build: 
	go build main.go
	go build node_controller.go

release: build
	zip release.zip node_controller main config.yaml
	tar cvzf release.tar.gz node_controller main config.yaml
	rm node_controller main 
