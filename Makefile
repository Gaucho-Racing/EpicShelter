.PHONY: clean run

clean:
	go clean
	go mod tidy
	rm *.out
	rm coverage.html

run:
	chmod +x scripts/run.sh
	./scripts/run.sh

keygen:
	chmod +x scripts/keygen.sh
	./scripts/keygen.sh