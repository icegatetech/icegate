.PHONY: dev

dev:
	PROFILE=debug docker compose -f config/docker/docker-compose.yml up --watch --build
