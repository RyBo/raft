# syntax=docker/dockerfile:1

# 1) Build the React/Vite UI -> webui/dist. The Go binary embeds this directory
#    via //go:embed all:dist in webui/embed.go, so it must exist before `go build`.
FROM node:20-alpine AS ui
WORKDIR /app/webui
COPY webui/package.json webui/package-lock.json ./
RUN npm ci
COPY webui/ ./
RUN npm run build

# 2) Build the static Go binary with the UI embedded.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=ui /app/webui/dist ./webui/dist
RUN CGO_ENABLED=0 go build -ldflags "-s -w" -o /out/raftdemo ./cmd/raftdemo

# 3) Minimal, non-root runtime. No shell, no package manager.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/raftdemo /raftdemo
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/raftdemo"]
CMD ["-addr", ":8080", "-nodes", "3", "-seed", "1"]
