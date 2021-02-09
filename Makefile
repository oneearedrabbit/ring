build:
	go build -v && go test -v && golint

profile:
	./ring -cpuprofile=prof.cpu -memprofile=prof.mem

cleanup:
	rm prof.mem prof.cpu profile*.pdf
