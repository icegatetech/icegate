.PHONY: examples-infra-up examples-infra-down tests

examples-infra-up:
	cd ./examples && docker compose up --detach

examples-infra-down:
	cd ./examples && docker compose down

test:
	cargo test

clean:
	cargo clean