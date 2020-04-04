test:
	CGO_ENABLED=0 APP_ENV=test GO111MODULE=on go test -count=1 -v ./...

deps:
	go mod download
