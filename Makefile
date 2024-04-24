
run: 
	go run main.go

compile:
	protoc api/v1/*.proto \
        --go_out=. \
        --go_opt=paths=source_relative \
        --proto_path=.
test:
	go test -race ./...

clean:
	# Remove generated files from protoc
	rm *.pb.go
